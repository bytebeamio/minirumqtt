#!/bin/sh
set -e

cargo run --release -- --mode 2 -q 1 | tee benchmarks.txt
cargo run --release -- --mode 2 -q 0 | tee -a benchmarks.txt
