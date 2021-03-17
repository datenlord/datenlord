//! Global proactor

use super::small_box::SmallBox;

use crate::util::{u32_to_usize, u64_to_ptr, usize_to_u64};

use std::future::Future;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::{io, ptr, thread};

use ring_io::cq::CompletionQueue;
use ring_io::cqe::CQE;
use ring_io::ring::RingBuilder;
use ring_io::sq::SubmissionQueue;
use ring_io::sqe::SQE;

use futures::channel::mpsc;
use futures::StreamExt;

use crossbeam_queue::ArrayQueue;
use event_listener::{Event, EventListener};
use log::trace;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

/// The global proactor
struct Proactor {
    /// Inner state shared with submitter task and completer thread.
    inner: Arc<ProactorInner>,
}

/// Proactor state
struct ProactorInner {
    /// A mpsc channel from workers to submitter.
    chan: mpsc::Sender<SharedState>,
    /// An object pool of [`SharedState`].
    pool: ArrayQueue<SharedState>,
    /// The limit of concurrent IO operations.
    available_count: AtomicU32,
    /// An event emitted once a batch of [`CQE`]s is reaped.
    complete_event: Event,
}

/// Shared state of an [`IoRequest`].
type SharedState = Arc<Mutex<State>>;

/// Owned state of an [`IoRequest`].
#[derive(Debug)]
struct State {
    /// The current step of an [`IoRequest`].
    step: Step,
    /// The current waker of an [`IoRequest`].
    waker: Option<Waker>,
    /// A stable storage for an IO operation to store some values.
    storage: SmallBox,
    /// A stable storage for an IO operation's data to escape into.
    escape: SmallBox,
    /// The sqe of an IO operation.
    /// It is initialized only when the step is [`Step::InQueue`].
    sqe: MaybeUninit<SQE>,
    /// The result of an IO operation.
    /// It is valid only when the step is [`Step::Completed`].
    res: i32,
}

/// The current step of an [`IoRequest`].
#[derive(Debug)]
enum Step {
    /// The `State` structure contains nothing now.
    Empty,
    /// [`IoRequest`] is preparing to send its state to submitter task.
    Preparing,
    /// The shared state is in the channel between workers and submitter.
    InQueue,
    /// Submitter has submitted the corresponding SQE.
    Submitted,
    /// Completer has reaped the corresponding CQE.
    Completed,
    /// The [`IoRequest`] future has been dropped.
    Dropped,
    /// Panicked during switching the step.
    Poisoned,
}

/// An IO request which can be executed by the global proactor.
pub struct IoRequest<T: Operation + Send + Unpin> {
    /// Inner state shared with the global proactor
    state: SharedState,
    /// The listener of complete event.
    listener: Option<EventListener>,
    /// The IO operation which contains file descriptors and buffers.
    operation: ManuallyDrop<T>,
}

/// An IO operation
pub trait Operation {
    /// # Safety
    /// + `self` must be movable during the lifetime of IO.
    /// + Moving self must not invalidate the pointers passed into the proactor.
    /// + The `storage` has stable address. It will not be moved until the IO ends.
    /// + All resources (fd, buffers) used by the proactor must be valid until the IO ends.
    /// + When the IO ends, `storage` will be cleared and `self` will be returned or dropped.
    unsafe fn prepare(&mut self, storage: &mut SmallBox, sqe: &mut MaybeUninit<SQE>);
}

/// Do something with a shared state
fn with<R>(shared_state: &SharedState, f: impl FnOnce(&mut State) -> R) -> R {
    let mut state = shared_state.lock();
    f(&mut *state)
}

impl State {
    /// Creates an empty [`State`]
    fn empty() -> Self {
        Self {
            step: Step::Empty,
            storage: SmallBox::empty(),
            escape: SmallBox::empty(),
            waker: None,
            sqe: MaybeUninit::uninit(),
            res: i32::MIN,
        }
    }

    /// Resets the [`State`]
    fn reset(&mut self) {
        self.step = Step::Empty;
        self.storage.clear();
        self.escape.clear();
        self.waker = None;
        self.res = i32::MIN;
    }
}

impl<T: Operation + Send + Unpin> IoRequest<T> {
    /// Creates a new [`IoRequest`]
    pub fn new(op: T) -> Self {
        let proactor = Proactor::global();
        let shared_state = proactor.create_shared_state();
        with(&shared_state, |s| s.step = Step::Preparing);
        Self {
            state: shared_state,
            listener: None,
            operation: ManuallyDrop::new(op),
        }
    }
}

impl<T: Operation + Send + Unpin> Future for IoRequest<T> {
    type Output = (io::Result<u32>, T);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this: &mut Self = &mut *self;

        let ProactorInner {
            ref chan,
            ref complete_event,
            ref available_count,
            ..
        } = *Proactor::global().inner;

        let mut state = this.state.lock();
        let step = mem::replace(&mut state.step, Step::Poisoned);

        match step {
            Step::Preparing => 'prepare: loop {
                if let Some(ref mut listener) = this.listener {
                    if let Poll::Pending = Pin::new(listener).poll(cx) {
                        state.step = Step::Preparing;
                        return Poll::Pending;
                    }
                }
                this.listener = None;

                let mut count = available_count.load(Ordering::Acquire);
                loop {
                    if count == 0 {
                        this.listener = Some(complete_event.listen());
                        continue 'prepare;
                    }
                    match available_count.compare_exchange_weak(
                        count,
                        count.wrapping_sub(1),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(c) => count = c,
                    }
                }

                unsafe {
                    let state = &mut *state;
                    T::prepare(&mut this.operation, &mut state.storage, &mut state.sqe)
                };

                state.waker = Some(cx.waker().clone());
                state.step = Step::InQueue;

                chan.clone()
                    .try_send(Arc::clone(&this.state))
                    .unwrap_or_else(|e| panic!("send error: {}", e));
                return Poll::Pending;
            },
            Step::InQueue | Step::Submitted => {
                state.waker = Some(cx.waker().clone());
                state.step = step;
                Poll::Pending
            }
            Step::Completed => {
                let result = resultify(state.res);
                let data = unsafe { ManuallyDrop::take(&mut this.operation) };
                state.reset();
                Poll::Ready((result, data))
            }
            Step::Empty | Step::Dropped | Step::Poisoned => panic!("invalid step: {:?}", step),
        }
    }
}

impl<T: Operation + Send + Unpin> Drop for IoRequest<T> {
    fn drop(&mut self) {
        let ProactorInner { ref pool, .. } = *Proactor::global().inner;
        let mut state = self.state.lock();
        let step = mem::replace(&mut state.step, Step::Poisoned);

        match step {
            Step::Empty => {
                drop(pool.push(Arc::clone(&self.state)));
            }
            Step::InQueue => {
                state.step = Step::Dropped;
            }
            Step::Submitted => unsafe {
                state.escape.put(ManuallyDrop::take(&mut self.operation));
                state.step = Step::Dropped;
            },
            Step::Preparing | Step::Completed => {
                unsafe { ManuallyDrop::drop(&mut self.operation) }
                state.reset();
                drop(pool.push(Arc::clone(&self.state)));
            }
            Step::Dropped | Step::Poisoned => panic!("invalid step: {:?}", step),
        }
    }
}

/// Converts `cqe.raw_result()` to [`io::Result<u32>`]
fn resultify(res: i32) -> io::Result<u32> {
    if res >= 0 {
        Ok(better_as::number::wrapping_cast(res))
    } else {
        Err(io::Error::from_raw_os_error(res.wrapping_neg()))
    }
}

impl Proactor {
    /// Returns the global proactor
    fn global() -> &'static Self {
        /// Global proactor
        #[allow(clippy::expect_used)]
        static GLOBAL_PROACTOR: Lazy<Proactor> =
            Lazy::new(|| Proactor::start_driver().expect("failed to start global proactor driver"));
        &*GLOBAL_PROACTOR
    }

    /// Creates a shared state from the inner object pool
    fn create_shared_state(&self) -> SharedState {
        match self.inner.pool.pop() {
            Some(state) => state,
            None => Arc::new(Mutex::new(State::empty())),
        }
    }

    /// Starts the submitter task and completer thread.
    fn start_driver() -> io::Result<Self> {
        let ring_entries = 32;
        let available = 64;

        let ring = RingBuilder::new(ring_entries).build()?;

        #[allow(box_pointers)]
        let ring = Box::leak(Box::new(ring));

        let (mut sq, mut cq, _) = ring.split();

        let (tx, mut rx): _ = mpsc::channel(u32_to_usize(available));

        let inner = Arc::new(ProactorInner {
            chan: tx,
            pool: ArrayQueue::new(u32_to_usize(available)),
            available_count: AtomicU32::new(available),
            complete_event: Event::new(),
        });

        {
            let inner = Arc::clone(&inner);
            smol::spawn(async move { Self::submitter(&mut sq, &mut rx, &*inner).await }).detach();
        }
        {
            let inner = Arc::clone(&inner);
            thread::spawn(move || Self::completer(&mut cq, &*inner));
        }

        Ok(Self { inner })
    }

    /// Submitter task
    async fn submitter(
        sq: &mut SubmissionQueue<'static>,
        chan: &mut mpsc::Receiver<SharedState>,
        inner: &ProactorInner,
    ) {
        loop {
            let mut ops_cnt = 0;
            loop {
                let available_sqes = sq.space_left();
                while ops_cnt < available_sqes {
                    match chan.try_next() {
                        Ok(Some(handle)) => Self::prepare(sq, handle, &inner.pool),
                        Ok(None) => panic!("proactor failed"),
                        Err(_) => break,
                    }
                    ops_cnt = ops_cnt.wrapping_add(1);
                }
                if ops_cnt > 0 {
                    break;
                }
                trace!("submitter is waiting an operation");
                if let Some(handle) = chan.next().await {
                    Self::prepare(sq, handle, &inner.pool);
                    ops_cnt = 1;
                }
            }

            trace!("submitter is submitting");
            let on_err = |err| panic!("proactor failed: {}", err);
            let n_submitted = sq.submit().unwrap_or_else(on_err);
            trace!("submitter submitted {} sqes", n_submitted);
        }
    }

    /// Completer thread
    fn completer(cq: &mut CompletionQueue<'static>, inner: &ProactorInner) {
        let ProactorInner {
            ref available_count,
            ref complete_event,
            ref pool,
            ..
        } = *inner;

        loop {
            trace!("completer enters loop");
            if cq.ready() == 0 {
                trace!("completer is waiting a cqe");
                cq.wait_cqes(1)
                    .unwrap_or_else(|err| panic!("proactor failed: {}", err));
            }
            let mut cqes_cnt: u32 = 0;
            while let Some(cqe) = cq.peek_cqe() {
                Self::complete(cqe, pool);
                cqes_cnt = cqes_cnt.wrapping_add(1);
                unsafe { cq.advance_unchecked(1) };
            }
            trace!("completer completed {} cqes", cqes_cnt);

            available_count.fetch_add(cqes_cnt, Ordering::AcqRel);
            complete_event.notify(u32_to_usize(cqes_cnt));
        }
    }

    /// Prepares an IO request
    fn prepare(
        sq: &mut SubmissionQueue<'static>,
        shared_state: SharedState,
        pool: &ArrayQueue<SharedState>,
    ) {
        let mut state = shared_state.lock();
        let step = mem::replace(&mut state.step, Step::Poisoned);

        match step {
            Step::InQueue => {
                trace!("submitter is fetching a sqe");

                let sqe = loop {
                    unsafe {
                        if let Some(sqe) = sq.get_sqe_uninit() {
                            break sqe;
                        }
                    }
                };

                let addr = better_as::pointer::to_address(Arc::into_raw(Arc::clone(&shared_state)));

                unsafe {
                    SQE::overwrite_uninit(sqe, ptr::read(state.sqe.as_mut_ptr().cast()))
                        .set_user_data(usize_to_u64(addr))
                };

                state.step = Step::Submitted;

                trace!("submitter prepared a sqe");
            }
            Step::Dropped => {
                state.reset();
                drop(state);
                drop(pool.push(shared_state));
            }
            Step::Empty | Step::Preparing | Step::Submitted | Step::Completed | Step::Poisoned => {
                panic!("invalid step: {:?}", step)
            }
        };
    }

    /// Completes an IO request
    fn complete(cqe: &CQE, pool: &ArrayQueue<SharedState>) {
        unsafe {
            let ptr = u64_to_ptr(cqe.user_data());
            let shared_state: SharedState = Arc::from_raw(ptr.cast());
            let mut state = shared_state.lock();
            let step = mem::replace(&mut state.step, Step::Poisoned);
            match step {
                Step::Submitted => {
                    state.res = cqe.raw_result();
                    state.step = Step::Completed;
                    if let Some(waker) = state.waker.take() {
                        waker.wake()
                    }
                }
                Step::Dropped => {
                    state.reset();
                    drop(state);
                    drop(pool.push(shared_state));
                }
                Step::Empty
                | Step::Preparing
                | Step::InQueue
                | Step::Completed
                | Step::Poisoned => {
                    panic!("invalid step: {:?}", step);
                }
            }
        }
    }
}
