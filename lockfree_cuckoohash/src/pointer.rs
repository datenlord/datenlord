use crossbeam_epoch::Guard;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

/// `AtomicPtr` is a pointer which can only be manipulated by
/// atomic operations.
#[derive(Debug)]
pub struct AtomicPtr<T: ?Sized> {
    data: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T: ?Sized + Send + Sync> Send for AtomicPtr<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for AtomicPtr<T> {}

impl<T> AtomicPtr<T> {
    const fn from_usize(data: usize) -> Self {
        Self {
            data: AtomicUsize::new(data),
            _marker: PhantomData,
        }
    }

    pub const fn null() -> Self {
        Self::from_usize(0)
    }

    pub fn load<'g>(&self, ord: Ordering, _: &'g Guard) -> SharedPtr<'g, T> {
        SharedPtr::from_usize(self.data.load(ord))
    }

    pub fn compare_and_set<'g>(
        &self,
        current: SharedPtr<'_, T>,
        new: SharedPtr<'_, T>,
        ord: Ordering,
        _: &'g Guard,
    ) -> Result<SharedPtr<'g, T>, (SharedPtr<'g, T>, SharedPtr<'g, T>)> {
        let new = new.as_usize();
        // TODO: allow different ordering.
        self.data
            .compare_exchange(current.as_usize(), new, ord, ord)
            .map(|_| SharedPtr::from_usize(new))
            .map_err(|current| (SharedPtr::from_usize(current), SharedPtr::from_usize(new)))
    }
}

/// `SharedPtr` is a pointer which can be shared by multi-threads.
/// `SharedPtr` can only be used with 64bit-wide pointer, and the
/// pointer address must be 8-byte aligned.
pub struct SharedPtr<'g, T: 'g> {
    data: usize,
    _marker: PhantomData<(&'g (), *const T)>,
}

impl<T> Clone for SharedPtr<'_, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<T> Copy for SharedPtr<'_, T> {}

#[allow(clippy::trivially_copy_pass_by_ref, clippy::missing_const_for_fn)]
impl<T> SharedPtr<'_, T> {
    pub fn from_usize(data: usize) -> Self {
        SharedPtr {
            data,
            _marker: PhantomData,
        }
    }

    pub fn from_box(b: Box<T>) -> Self {
        Self::from_raw(Box::into_raw(b))
    }

    pub fn from_raw(raw: *const T) -> Self {
        Self::from_usize(raw as usize)
    }

    pub fn null() -> Self {
        Self::from_usize(0)
    }

    pub const fn as_usize(&self) -> usize {
        self.data
    }

    fn decompose_lower_bit(data: usize) -> (usize, bool) {
        (data & !1, data & 1 == 1)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn decompose_higher_u8(data: usize) -> (u8, usize) {
        let mask: usize = (1 << 56) - 1;
        (((data & !mask) >> 56) as u8, data & mask)
    }

    pub fn decompose(&self) -> (u8, *const T, bool) {
        let data = self.data;
        let (data, lower_bit) = Self::decompose_lower_bit(data);
        let (higher_u8, data) = Self::decompose_higher_u8(data);
        (higher_u8, data as *const T, lower_bit)
    }

    pub fn as_raw(&self) -> *const T {
        let (_, raw, _) = self.decompose();
        raw
    }

    pub fn with_tag(&self) -> Self {
        Self::from_usize(self.data | 1)
    }

    pub fn without_tag(&self) -> Self {
        Self::from_usize(self.data & !1)
    }

    pub fn with_higher_u8(&self, higher_u8: u8) -> Self {
        let data = self.data;
        let mask: usize = (1 << 56) - 1;
        Self::from_usize((data & mask) | ((higher_u8 as usize) << 56))
    }

    pub fn tag(&self) -> bool {
        (self.data & 1) == 1
    }
}
