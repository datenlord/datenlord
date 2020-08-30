use std::convert::TryFrom;

/// A type cast trait used to replace as conversion.
pub trait Cast {
    /// Performs the conversion.
    fn cast<T>(self) -> T
    where
        T: TryFrom<Self>,
        Self: Sized + std::fmt::Display + Copy,
    {
        T::try_from(self).unwrap_or_else(|_| {
            panic!(
                "Failed to convert from {}: {} to {}",
                std::any::type_name::<Self>(),
                self,
                std::any::type_name::<T>(),
            )
        })
    }
}

impl<U> Cast for U {}

/// Cast to pointer
pub const fn cast_to_ptr<T: ?Sized, U>(val: &T) -> *const U {
    let ptr: *const _ = val;
    ptr.cast()
}

#[allow(dead_code)]
/// Cast to mut pointer
pub fn cast_to_mut_ptr<T: ?Sized, U>(val: &mut T) -> *mut U {
    let ptr: *mut _ = val;
    ptr.cast()
}