#!/bin/bash -e

# translate Xcode's CONFIGURATION setting of "Release" or "Debug"
#CARGO_MODE=$(echo ${CONFIGURATION:-"debug"} | tr '[A-Z]' '[a-z]')

# always run in release mode (~20x faster datafusion performance)
CARGO_MODE="release"
CARGO_MODE="debug"

# build for macOS10.12+
export MACOSX_DEPLOYMENT_TARGET=10.12

export CARGO_TARGET_DIR=${TARGET_BUILD_DIR:-"target"}

# enabling SIMD works for Intel, but fails on AARM:
# “'+sse4.2' is not a recognized feature for this target (ignoring feature)”
# export RUSTFLAGS="-C target-feature=+sse4.2"

echo "Building ${CARGO_MODE} arcolyte into ${CARGO_TARGET_DIR}…"

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

# generate a C header file for all the target items
# no need to do this manually; it is now performed in build.rs
# cbindgen -l C -o target/arcolyte.h

# cbindgen --help

# cat target/arcolyte.h

echo "Linking ${CARGO_TARGET_DIR}/arcolyte.h"
# link the header to the parent (samedir required)
ln -fv "${CARGO_TARGET_DIR}/arcolyte.h" ../
# cp -av "${CARGO_TARGET_DIR}/arcolyte.h" ../

FAT_ARCHIVE_PATH=${BUILT_PRODUCTS_DIR:-"target"}/libarcolyte.a

lipo -create ${CARGO_TARGET_DIR}/*-*/${CARGO_MODE}/libarcolyte.a -output ${FAT_ARCHIVE_PATH}


# print out some diagnostic info
file ${FAT_ARCHIVE_PATH}
ls -lah ${FAT_ARCHIVE_PATH}

