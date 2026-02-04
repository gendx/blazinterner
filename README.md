# Blazinterner: efficient and concurrent interning of generic data

[![Crate](https://img.shields.io/crates/v/blazinterner.svg?logo=rust)](https://crates.io/crates/blazinterner)
[![Documentation](https://img.shields.io/docsrs/blazinterner/0.2.3?logo=rust)](https://docs.rs/blazinterner/0.2.3/)
[![Minimum Rust 1.91.0](https://img.shields.io/crates/msrv/blazinterner/0.2.3.svg?logo=rust&color=orange)](https://releases.rs/docs/1.91.0/)
[![Lines of Code](https://www.aschey.tech/tokei/github/gendx/blazinterner?category=code&branch=0.2.3)](https://github.com/gendx/blazinterner/tree/0.2.3)
[![Dependencies](https://deps.rs/crate/blazinterner/0.2.3/status.svg)](https://deps.rs/crate/blazinterner/0.2.3)
[![License](https://img.shields.io/crates/l/blazinterner/0.2.3.svg)](https://github.com/gendx/blazinterner/blob/0.2.3/LICENSE)
[![Codecov](https://codecov.io/gh/gendx/blazinterner/branch/0.2.3/graph/badge.svg)](https://codecov.io/gh/gendx/blazinterner/tree/0.2.3)
[![Build Status](https://github.com/gendx/blazinterner/actions/workflows/build.yml/badge.svg?branch=0.2.3)](https://github.com/gendx/blazinterner/actions/workflows/build.yml)
[![Test Status](https://github.com/gendx/blazinterner/actions/workflows/tests.yml/badge.svg?branch=0.2.3)](https://github.com/gendx/blazinterner/actions/workflows/tests.yml)

Here are the main features offered by this crate.

- **Generic**: You can intern any data type that implements `Hash` and `Eq`, not
  just strings. The interned type doesn't even have to be `Sized` (for example
  `str`), as long as you provide a `Sized` storage type (such as `Box<str>`)
  that can be borrowed as the interned type.
- **Efficient**: Each `Interned` value contains only a 32-bit index. The
  corresponding `Arena` stores each value directly in an `AppendVec`, plus the
  32-bit index in a raw hash table (`DashTable`). To intern a value of type `T`
  using storage type `S`, you can pass any type that implements `Borrow<T>` and
  `Into<S>`, which allows avoiding unnecessary copies. For example, in an
  `Arena<str, Box<str>>` you can intern many string types: `&str`, `String`,
  `Box<str>`, `Cow<'_, str>`, etc.
- **Concurrent**: The `Arena` is `Sync`, and allows simultaneous reads and
  writes. More specifically, retrieving values via `Interned::lookup()` and
  `Interned::lookup_ref()` is always wait-free, even when a write happens
  concurrently! This is thanks to the underlying `AppendVec` implementation.
  However, only one write (using `Interned::from()`) can happen at a time on a
  given arena, due to an exclusive write lock.
