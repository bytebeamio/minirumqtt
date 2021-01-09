#!/bin/sh
set -e

cargo run --release --bin one -- -i one-local-q1 --mode 3 -q 1 | tee benchmarks.txt
cargo run --release --bin one -- -i one-local-q0 --mode 3 -q 0 | tee -a benchmarks.txt
cargo run --release --bin one -- -i one-remote-q1 --mode 2 -q 1 | tee -a benchmarks.txt
cargo run --release --bin one -- -i one-remote-q0 --mode 2 -q 0 | tee -a benchmarks.txt
