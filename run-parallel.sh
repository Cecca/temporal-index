#!/usr/bin/env bash

for THREADS in 1 2 4 8 16; do
	env RAYON_NUM_THREADS=$THREADS cargo run --release -- experiments/focus-parallel.yml
done
