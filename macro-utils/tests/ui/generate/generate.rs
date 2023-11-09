use macro_utils::*;

fn main() {
    const BYTES: [u8; 40] = generate_bytes! {
        BE;
        u64: 65,
        u64: 20,
        u64: 507,
        u32: 0x20,
        u32: 0x20,
        str: b"foo.bar\0",
    };

    const EXPECTED: [u8; 40] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfb,
    0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x20,
    0x66, 0x6f, 0x6f, 0x2e, 0x62, 0x61, 0x72, 0x00
    ];

    assert_eq!(BYTES, EXPECTED);
}