# Parquette!
[![CI](https://github.com/parquette/parquette/workflows/CI/badge.svg)](https://github.com/parquette/parquette/actions)

Prototype of embedding Arrow & DataFusion Rust libraries in a Swift macOS app via cbindgen interfaces and sharing the zero-copy arrays with Rust's wasm-bindgen JavaScript equivalent.

Download the latest build from https://github.com/parquette/parquette/releases/latest/. It is deployed frequently and automatically incorporates the latest DataFusion 4.0.0-SNAPSHOT (https://docs.rs/datafusion/).

The app can open `.csv`. & `.parquet` files and execute rudimentary SQL. 

A good example file is a 515MB [NYC April 2010 Taxi Data](https://ursa-labs-taxi-data.s3.us-east-2.amazonaws.com/2010/04/data.parquet) parquet file. See (https://cran.r-project.org/web/packages/arrow/vignettes/dataset.html) for schema info.

![Screenshot](screenshot.png "Scren shot")

Limitations: Currently the interface only supports utf8 and int* types, so any floating point calculations need to be cast to ints to show up in the interface. E.g.: `select cast(max(tip_amount) as bigint), cast(avg(fare_amount) as bigint) from data`


