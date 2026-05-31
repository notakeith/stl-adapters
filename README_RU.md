# STL Adapters

> [English version](README.md)

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue?logo=cplusplus&logoColor=white)](https://en.cppreference.com/w/cpp/23)
[![CMake](https://img.shields.io/badge/CMake-3.12%2B-064F8C?logo=cmake&logoColor=white)](https://cmake.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)


Header-only библиотека ленивых адаптеров для pipeline-обработки данных на C++23 — фильтрация, трансформация, разбиение, join и файловый ввод-вывод, соединённые через `|`. Написана в рамках курса по C++ в ИТМО.

Адаптеры работают лениво (кроме `AggregateByKey`, `Join` и `SplitExpected`).

```cpp
Dir("./data", /*recursive=*/false)
  | Filter([](fs::path& p){ return p.extension() == ".txt"; })
  | OpenFiles()
  | Split("\n ,.;")
  | Out(std::cout);
```

## Адаптеры

| Адаптер | Описание |
|---|---|
| `Dir(path, recursive)` | файлы в директории, опционально рекурсивно |
| `OpenFiles()` | открывает каждый путь как файловый поток, выдаёт строки |
| `Split(delimiters)` | разбивает строки по набору символов-делимитеров |
| `Filter(pred)` | фильтрация по предикату |
| `Transform(func)` | применяет функцию к каждому элементу |
| `AsDataFlow(vec)` | превращает `std::vector` в поток |
| `Out(ostream)` | выводит элементы в поток *(терминальный)* |
| `Write(ostream, delim)` | то же, но с заданным разделителем *(терминальный)* |
| `AsVector()` | собирает поток в `std::vector` *(терминальный)* |
| `Join(right, lkey, rkey)` | LEFT JOIN двух потоков по ключу |
| `DropNullopt()` | фильтрует `std::nullopt` из потока `optional<T>` |
| `SplitExpected()` | разделяет поток `expected<T,E>` на значения и ошибки |
| `AggregateByKey(init, agg, key)` | агрегация значений по ключу через функцию-агрегатор |

## Сборка

```bash
git clone https://github.com/notakeith/stl-adapters.git
cd stl-adapters
cmake -B build && cmake --build build
ctest --test-dir build
```

Требования: CMake ≥ 3.12, GCC 13+ или Clang 17+.
