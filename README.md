# STL Adapters

> [Русская версия](README_RU.md)

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue?logo=cplusplus&logoColor=white)](https://en.cppreference.com/w/cpp/23)
[![CMake](https://img.shields.io/badge/CMake-3.12%2B-064F8C?logo=cmake&logoColor=white)](https://cmake.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Header-only C++23 pipeline library with lazy adapters for data processing — filter, transform, split, join, and file I/O, composed via `|`. Written as part of a C++ course at ITMO University.

Adapters are lazy by default (except `AggregateByKey`, `Join`, and `SplitExpected`).

```cpp
Dir("./data", /*recursive=*/false)
  | Filter([](fs::path& p){ return p.extension() == ".txt"; })
  | OpenFiles()
  | Split("\n ,.;")
  | Out(std::cout);
```

## Adapters

| Adapter | Description |
|---|---|
| `Dir(path, recursive)` | files in a directory, optionally recursive |
| `OpenFiles()` | opens each path as a file stream, yields lines |
| `Split(delimiters)` | splits strings by a set of delimiter characters |
| `Filter(pred)` | filters elements by predicate |
| `Transform(func)` | applies a function to each element |
| `AsDataFlow(vec)` | wraps a `std::vector` into a stream |
| `Out(ostream)` | prints elements to a stream *(terminal)* |
| `Write(ostream, delim)` | same, but with a custom delimiter between elements *(terminal)* |
| `AsVector()` | collects stream into a `std::vector` *(terminal)* |
| `Join(right, lkey, rkey)` | LEFT JOIN of two streams by key |
| `DropNullopt()` | filters `std::nullopt` from an `optional<T>` stream |
| `SplitExpected()` | splits an `expected<T,E>` stream into values and errors |
| `AggregateByKey(init, agg, key)` | aggregates values by key via an accumulator function |

## Build

```bash
git clone https://github.com/notakeith/stl-adapters.git
cd stl-adapters
cmake -B build && cmake --build build
ctest --test-dir build
```

Requirements: CMake ≥ 3.12, GCC 13+ or Clang 17+.
