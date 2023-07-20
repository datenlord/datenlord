# Code Style

## Default Rust style and check

Please *DO* the following steps before submitting a PR:
* Run `cargo fmt` to format the code, you can also run `cargo fmt -- --check` to review the format changes;
* Run `cargo clippy` to make sure the code has no warnings from clippy;
* Run `cargo test` in project root directory to make sure no tests break.

## `impl`

Use `impl` whenever possible, since `impl` means static dispatch, not dynamic, which has less overhead.
Also `impl` makes a lot succinct.
* Try to avoid trait object, or don't use `dyn`. Use `impl` instead. For example:
```Rust
// not recommended
fn foo(input: Box<dyn SomeTrait>)

// preferred
fn foo(input: impl SomeTrait)
```

* Try to avoid redundant generic type definition, use `impl` instead. For example:
```Rust
// not recommended
fn foo<P: AsRef<Path>>(input: P)

// preferred
fn foo(input: impl AsRef<Path>)
```

## Functional programming

Use functional style instead of imperative one, especially when doing iteration or future chaining.
For example:
```Rust
fn process(i: u8) {
  println!("u8={}", i);
}

// not recommend
for i in vec {
  process(i);
}

// preferred
vec.iter().for_each(process);
```

## External function, trait and struct naming

When using functions from external dependent crates, reserve the mod name where a function belongs to.
This is a Rust tradition. For example:
```Rust
// not recommended
use external_crate::some_mod::some_func;
some_func();

// preferred
use external_crate::some_mod;
some_mod::some_func();
```
As for using Struct or Trait from external dependent crate, just directly use it. For example:
```Rust
// preferred
use external_crate::some_mod::{SomeStruct, SomeTrait};

let val = SomeStruct::new();

fn foo(bar: impl SomeTrait) {
  todo!();
}
```

Except for using `Result`, since so many crates define `Result` for theirselves,
so please highlight which `Result` is using. For example:
```Rust
// not recommended
use std::io::Result;

fn foo() -> Result<()> {
  todo!();
}

// preferred
use std::io;

fn foo() -> io::Result<()> {
  todo!();
}

```

## Relative mod path

When using internal mod's, please use relative mod path, instead of absolute path. For example:
```Rust
mod AAA {
  mod BBB {
    mod CCC {
      struct SomeStruct(u8);
    }
    
    mod DDD {
      // not recommended
      use crate::AAA::BBB::CCC::SomeStruct;
      
      // preferred
      use super::CCC::SomeStruct;
    }
  }
}
```

## Error handling

*DO NOT* defined customized error type, unless explicit reason. Use `anyhow` crate for default error handling.
* It's preferred to add context to `Result` by using `anyhow::Context`. For example:
```Rust
use anyhow::Context;
use std::io;

fn foo() -> io::Result<()> {
  todo!();
}

fn bar() -> anyhow::Result<()> {
  foo().context("more description for the error case")?;
}

```

* Every `assert!`, `assert_eq!` and `assert_nq!` should have a explanation about assertion failure. For example:
```Rust

// not recommended
assert!(some_condition);

// preferred
assert!(some_condition, "explanation about assertion failure");
```

* *DO NOT* use `unwrap()` of `Result` and `Option` directly, unless `Result` or `Option` are properly checked.
If you believe using `unwrap()` is safe, please add a comment for each `unwrap()`.
For example:
```Rust
let val = some_result.unwrap(); // safe to use unwrap() here, because ...
```


# Misc

Try to use safe code as much as possible, if no explicit reason, please don't use unsafe code.
