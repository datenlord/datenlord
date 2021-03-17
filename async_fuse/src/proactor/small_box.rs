//! A type-erased container which stores small item on stack and fallbacks to heap for large item.

use std::{fmt, mem, ptr};

/// The number of inline pointers in a [`SmallBox`].
///
/// The size of a [`SmallBox`] is `size_of::<usize>() * (NR_INLINE_PTR + 1)`
const NR_INLINE_PTR: usize = 7;

/// A type-erased container which stores small item on stack and fallbacks to heap for large item.
pub struct SmallBox {
    /// inline storage
    bytes: [usize; NR_INLINE_PTR],
    /// inline vtable
    drop: unsafe fn(*mut ()),
}

unsafe impl Send for SmallBox {}
unsafe impl Sync for SmallBox {}

/// Type-erased drop function
unsafe fn drop_value<T>(p: *mut ()) {
    ptr::drop_in_place(p.cast::<T>())
}

impl SmallBox {
    /// The size of inline storage in a [`SmallBox`]
    const BYTES: usize = mem::size_of::<usize>() * NR_INLINE_PTR;

    /// Creates an empty [`SmallBox`]
    pub fn empty() -> Self {
        Self {
            bytes: [0; NR_INLINE_PTR],
            drop: drop_value::<()>,
        }
    }

    /// Drops the value in the [`SmallBox`].
    ///
    /// After calling [`SmallBox::clear`], the [`SmallBox`] is empty.
    pub fn clear(&mut self) {
        let drop_fn = mem::replace(&mut self.drop, drop_value::<()>);
        unsafe { drop_fn(self.bytes.as_mut_ptr().cast()) }
    }

    /// Puts a value into the [`SmallBox`]. The previous value in it will be dropped.
    pub fn put<T: Send>(&mut self, value: T) -> *mut T {
        assert!(mem::align_of::<usize>().wrapping_rem(mem::align_of::<T>()) == 0);
        unsafe { self.put_unchecked(value) }
    }

    /// Puts a value into the [`SmallBox`]. The previous value in it will be dropped.
    ///
    /// # Safety
    /// + `T` must be [`Send`].
    /// + `align_of::<T>()` must not be larger than `align_of::<usize>()`.
    #[allow(box_pointers)]
    pub unsafe fn put_unchecked<T>(&mut self, value: T) -> *mut T {
        self.clear();

        if mem::size_of::<T>() <= Self::BYTES {
            let value_ptr = self.bytes.as_mut_ptr().cast();
            ptr::write(value_ptr, value); // <- memcpy here: stack to stack or heap
            self.drop = drop_value::<T>;
            value_ptr
        } else {
            let data_ptr = Box::into_raw(Box::new(value)); // <- memcpy here: stack to heap
            self.bytes[0] = better_as::pointer::to_address_mut(data_ptr);
            self.drop = drop_value::<Box<T>>;
            data_ptr
        }
    }
}

impl Drop for SmallBox {
    fn drop(&mut self) {
        self.clear();
    }
}

impl fmt::Debug for SmallBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SmallBox {{ .. }}")
    }
}
