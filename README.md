Indexing temporal relations for range-duration queries
===============================

This project implements some indices for range-duration queries.
The code is in `rust` and can be run from a `docker` image.
Once you have `docker` up and running, all that is neede to compile the
code and run a first batch of experiments is

    ./dockerrun experiments/debug.yml

or any other configuration following the specification.
   
This will produce a sqlite database, `temporal-index-results.sqlite`,
containing the results for each parameter configuration described in the
`experiments/debug.yml` file.

