#!/bin/bash -ve

# translate Xcode's CONFIGURATION setting of "Release" or "Debug"
CARGO_MODE=$(echo ${CONFIGURATION:-"debug"} | tr '[A-Z]' '[a-z]')

# rustup should be installed in ~/.cargo/bin with `brew install rustup`
PATH=${HOME}/.cargo/bin:${PATH}

cd arcolyte/

# nightly is currently needed due to datafusion dependency on specialization: https://issues.apache.org/jira/browse/ARROW-10002
# rustup toolchain install nightly


# install macOS toolchains…
rustup target add x86_64-apple-darwin aarch64-apple-darwin

# install iOS toolchains…
# rustup target add aarch64-apple-ios x86_64-apple-ios
# install WASM toolchain…
# rustup target add wasm32-unknown-unknown

rustup toolchain install stable

rustup show


# build both architectures at once; unfortunately, this requires a nightly build
# cargo build -Zmultitarget --target x86_64-apple-darwin --target aarch64-apple-darwin ${CARGO_FLAGS}

if [ $CARGO_MODE = "release" ]; then CARGO_FLAGS="--release"; else CARGO_FLAGS=""; fi

cargo build --target x86_64-apple-darwin ${CARGO_FLAGS}
cargo build --target aarch64-apple-darwin ${CARGO_FLAGS}

# cargo build --target wasm32-unknown-unknown ${CARGO_FLAGS}
# cargo build --target x86_64-apple-ios ${CARGO_FLAGS}
# cargo build --target aarch64-apple-ios # INCOMPATIBLE with aarch64-apple-darwin ${CARGO_FLAGS}

# merge the files manually into a fat archive
# note that we can't merge an iOS & ARM macOS archive at the same time:
# fatal error: lipo: target/aarch64-apple-darwin/debug/libarcolyte.a and target/aarch64-apple-ios/debug/libarcolyte.a have the same architectures (arm64) and can't be in the same fat output file

lipo -create target/*-*/${CARGO_MODE}/libarcolyte.a -output target/libarcolyte.a

# generate a C header file for all the target items
# no need to do this manually; it is now performed in build.rs
# cbindgen -l C -o target/arcolyte.h

# cbindgen --help

# cat target/arcolyte.h

# print out some diagnostic info
file target/libarcolyte.a
ls -lah target/libarcolyte.a

# link the header to the parent (samedir required)
ln -fv target/arcolyte.h ../
