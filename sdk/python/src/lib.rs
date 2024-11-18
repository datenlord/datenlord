//! DatenLord Python SDK

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
    clippy::module_name_repetitions, // repetition of module name in a struct name is not a big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::unreachable, // Use `unreachable!` instead of `panic!` when impossible cases occur
    clippy::missing_errors_doc, // TODO: add error docs
    clippy::exhaustive_structs,
    clippy::exhaustive_enums,
    clippy::missing_panics_doc, // TODO: add panic docs
    clippy::panic_in_result_fn,
    clippy::single_char_lifetime_names,
    clippy::separated_literal_suffix, // conflict with unseparated_literal_suffix
    clippy::undocumented_unsafe_blocks, // TODO: add safety comment
    clippy::missing_safety_doc, // TODO: add safety comment
    clippy::shadow_unrelated, // it’s a common pattern in Rust code
    clippy::shadow_reuse, // it’s a common pattern in Rust code
    clippy::shadow_same, // it’s a common pattern in Rust code
    clippy::same_name_method, // Skip for protobuf generated code
    clippy::mod_module_files, // TODO: fix code structure to pass this lint
    clippy::std_instead_of_core, // Cause false positive in src/common/error.rs
    clippy::std_instead_of_alloc,
    clippy::pub_use, // pub use mod::item as new_name is common in Rust
    clippy::missing_trait_methods, // TODO: fix this
    clippy::arithmetic_side_effects, // TODO: fix this
    clippy::use_debug, // Allow debug print
    clippy::print_stdout, // Allow println!
    clippy::question_mark_used, // Allow ? operator, it’s a common pattern in Rust code
    clippy::absolute_paths, // Allow use through absolute paths, like `std::env::current_dir`
    clippy::multiple_unsafe_ops_per_block, // Mainly caused by `etcd_delegate`, will remove later
    clippy::ref_patterns, // Allow Some(ref x)
    clippy::single_call_fn, // Allow function is called only once
    clippy::pub_with_shorthand, // Allow pub(super)
    clippy::min_ident_chars, // Allow Err(e)
    clippy::missing_assert_message, // Allow assert! without message, mainly in test code
    clippy::impl_trait_in_params, // Allow impl AsRef<Path>, it's common in Rust
    clippy::module_inception, // We consider mod.rs as a declaration file
    clippy::semicolon_outside_block, // We need to choose between this and `semicolon_inside_block`, we choose outside
    clippy::similar_names // Allow similar names, due to the existence of uid and gid
)]

use file::File;
use pyo3::{pymodule, types::PyModule, Bound, PyResult, Python};
use sdk::DatenLordSDK;
use utils::{Buffer, Entry};

/// DatenLord Python SDK file
pub mod file;
/// DatenLord Python SDK
pub mod sdk;
/// DatenLord Python SDK utils
pub mod utils;

#[pymodule]
fn datenlordsdk(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatenLordSDK>()?;
    m.add_class::<File>()?;
    m.add_class::<Buffer>()?;
    m.add_class::<Entry>()?;

    Ok(())
}
