use macro_utils::*;

fn main() {
    const SIZE: usize = calculate_size! {
        BE;         // This field makes no difference but cannot be ignored.
        u64: 65,
        u64: 20,
        u64: 507,
        u32: 0x20,
        u32: 0x20,
        str: b"foo.bar\0",
    };

    assert_eq!(SIZE, 40);
}