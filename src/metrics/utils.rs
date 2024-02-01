//! Utils for metrics.

/// To convert a value into another type, which may lose precision.
pub trait LossyCast<T: Copy>: Copy {
    /// Lossy cast `self` to the target, wrapping on overflow.
    fn lossy_cast(self) -> T;
}

/// Implement `LossyCast` to f64.
macro_rules! impl_lossy_cast_to_f64 {
    ($($ty:ident),* $(,)?) => {
        $(
            #[allow(clippy::as_conversions)]
            impl LossyCast<f64> for $ty {
                #[inline]
                fn lossy_cast(self) -> f64 {
                    self as f64
                }
            }
        )*
    }
}

impl_lossy_cast_to_f64!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);
