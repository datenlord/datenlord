//! A utility crate of macros for generating test data for `DatenLord`.
//!
//! The main purpose of this crate is to generate FUSE request packets.
//!
//! # Examples
//!
//! ```
//! use macro_utils::{calculate_size, generate_bytes};
//! const DATA: [u8; 40] = generate_bytes! {
//!     BE;         // Endian
//!     u64: 65,    // field 1
//!     u64: 20,    // ...
//!     u64: 507,
//!     u32: 0x20,
//!     u32: 0x20,
//!     str: b"foo.bar\0",  // bytes
//! };
//!
//! const EXPECTED: [u8; 40] = [
//!     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
//!     0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfb, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00,
//!     0x00, 0x20, 0x66, 0x6f, 0x6f, 0x2e, 0x62, 0x61, 0x72, 0x00,
//! ];
//!
//! assert_eq!(DATA, EXPECTED);
//!
//! const SIZE: usize = calculate_size! {
//!     BE;         // This field makes no difference but cannot be ignored.
//!     u64: 65,
//!     u64: 20,
//!     u64: 507,
//!     u32: 0x20,
//!     u32: 0x20,
//!     str: b"foo.bar\0",
//! };
//!
//! assert_eq!(SIZE, 40);
//! ```

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    // missing_copy_implementations, // Copy may cause unnecessary memory copy
    missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    // unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results, // TODO: fix unused results
    variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow clippy::restriction
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::unreachable,  // Use `unreachable!` instead of `panic!` when impossible cases occurs
    clippy::separated_literal_suffix, // conflict with unseparated_literal_suffix
    clippy::shadow_unrelated, //it’s a common pattern in Rust code
    clippy::shadow_reuse, //it’s a common pattern in Rust code
    clippy::shadow_same, //it’s a common pattern in Rust code
    clippy::arithmetic_side_effects,  // An error is raised when calling `quote::quote!`
    clippy::arithmetic_side_effects  // As same as above
)]

use proc_macro::TokenStream;

mod generator;

/// Generate an array with defined data in the input.
///
/// See the crate documentation for examples.
#[proc_macro]
pub fn generate_bytes(tokens: TokenStream) -> TokenStream {
    generator::generate_bytes_impl(tokens)
}

/// Calculate the size of the array returned by `generate_bytes!` of the same
/// input.
#[proc_macro]
pub fn calculate_size(tokens: TokenStream) -> TokenStream {
    generator::calculate_size_impl(tokens)
}

#[cfg(test)]
mod tests {
    #[test]
    fn ui() {
        let t = trybuild::TestCases::new();
        t.pass("tests/ui/calculate/calculate.rs");
        t.pass("tests/ui/generate/generate.rs");
        t.compile_fail("tests/ui/generate/fail/*.rs");
    }
}
