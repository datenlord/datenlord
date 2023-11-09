use macro_utils::*;

fn main() {
    let _ = generate_bytes! {
        BE;
        u64: 65,
        u64: 20,
        u64: 507,
        u32: 0x20,
        str: 20,
    };
}