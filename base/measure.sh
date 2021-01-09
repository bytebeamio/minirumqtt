#!/bin/sh
set -e

echo "First principles based experiments to speedup rumqtt" | tee README.md;

# with embedded server
cargo run --release --bin one -- -i one-embedded-q1 --mode 3 -q 1 | tee -a README.md
cargo run --release --bin one -- -i one-embedded-q0 --mode 3 -q 0 | tee -a README.md

# with other local brokers
cargo run --release --bin one -- -i one-remote-q1 --mode 2 -q 1 | tee -a README.md
cargo run --release --bin one -- -i one-remote-q0 --mode 2 -q 0 | tee -a README.md
