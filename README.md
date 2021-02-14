# Parquette!
[![CI](https://github.com/parquette/parquette/workflows/CI/badge.svg)](https://github.com/parquette/parquette/actions)

Prototype of embedding Arrow & DataFusion Rust libraries in a Swift macOS app via cbindgen interfaces and sharing the zero-copy arrays with Rust's wasm-bindgen JavaScript equivalent.

Download the latest build from https://github.com/parquette/parquette/releases/latest/. It is deployed daily and automatically incorporates the latest DataFusion 4.0.0-SNAPSHOT (https://docs.rs/datafusion/).

The app can open `.csv`. & `.parquet` files and execute rudimentary SQL. 

(screenshot.png)

