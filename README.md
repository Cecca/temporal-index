Experiments on temporal indices
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

Implemented indices
-------------------

### Linear scan

Just scan the entire dataset for each query, useful as a naive baseline and
to check correctness. To use it add

    - LinearScan

to the `algorithms` section of the configuration file.

### BTree

Implemented using the BTree in Rust's standard library, it's super fast!
Keys are the duration of queries, values are vectors of intervals with the same
duration. For a given duration linear scan is employed on the corresponding vector.
To use it, add

    - BTree

to the `algorithms` section of the configuration file.

### Interval Tree

I think that this implementation can be optimized a little bit. Nontheless,
it leverages the fact that the dataset is static to try keep the tree
balanced. At the root, it looks for the median of the middle points of
intervals and uses it as the split point. Then intervals which don't overlap
with it are assigned to the left and right subtrees, depending on where they
fall, recursively.
To use it, add

    - IntervalTree

to the `algorithms` section of the configuration file.

### Grid

A grid that adapts to the data distribution: boundaries of cells are chosen
according to the Empirical Cumulative Distribution Function of the start and 
end points in order to have a given number of intervals in each cell.
An interval corresponds to the point in 2D space with coordinates corresponding to
its start and end.

    end times     +.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
                  |.................|
     +------------------------------+ Query, with
                  |                   swapped endpoints.
                  |
                  |
                  |
                  +---------------------------------------+
                                                      start times

In the plot above, given the query, we look at the intervals falling in cells
that intersect the shaded area.
To use this index, add to the `algorithms` section of the configuration file

    - Grid:
        bucket_size: [4096, 8192]

where `bucket_size` is a list of bucket sizes to try out.

### Grid3D

Same as above, but with an additional dimension given by the duration. Use as

    - Grid3D:
        bucket_size: [4096, 8192]

### Period Index

The data structure described in the conference paper. This is not the adaptive version.
Use it as

    - PeriodIndex:
        num_buckets: [16, 32, 64, 128, 256]
        num_levels: [4]

The parameter `num_buckets` gives the number of buckets that should be used
to partition the timeline. The parameter `num_levels` gives the maximum number of levels
to be used in each bucket.
