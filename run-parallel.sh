#!/usr/bin/env bash

# reset the old results
sqlite3 temporal-index-results.sqlite "delete from parallel_raw;"

# Runs the parallel experiments using different number of threads to build the index.

for THREADS in 1 2 4 8 16 32; do
	env RAYON_NUM_THREADS=$THREADS cargo run --release -- experiments/parallel.yml
	env RAYON_NUM_THREADS=$THREADS cargo run --release -- experiments/parallel-queries.yml
done
