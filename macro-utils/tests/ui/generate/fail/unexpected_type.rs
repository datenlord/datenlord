use macro_utils::*;

fn main() {
    let _ = generate_bytes! {
        BE;
        usize: 1024,
        u64: 65,
        u64: 20,
        u64: 507,
        u32: 0x20,
        str: b"foo.bar\0",
    };
}