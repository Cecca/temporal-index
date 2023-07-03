Indexing temporal relations for range-duration queries
===============================

This project implements some indices for range-duration queries.
To run the code you need the rust toolchain installed, which can be obtained by following instructions here: https://rustup.rs/

Experiments specifications are in the `experiments` directory, and can be run with (for instance)

```
cargo run --release -- experiments/real-world.yml
```

This command will produce (or update) a sqlite database named `temporal-index-results.sqlite` that contains all experimental results. This can be analyized with the R scripts in the `R` directory.

In particular, running in an R console

```
renv::restore()
drake::r_make()
```

Should produce the plots and tables reported in the paper

