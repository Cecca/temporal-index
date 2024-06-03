CREATE TABLE parallel_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    git_rev TEXT NOT NULL,
    hostname TEXT NOT NULL,
    conf_file TEXT,
    num_threads INTEGER,
    dataset_id INTEGER,
    queryset_id INTEGER,
    algorithm_id INTEGER,
    time_index_ms INT64 NOT NULL,
    time_query_ms INT64 NOT NULL,
    qps REAL NOT NULL,
    index_size_bytes INT NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES dataset_spec(dataset_id),
    FOREIGN KEY (queryset_id) REFERENCES queryset_spec(queryset_id),
    FOREIGN KEY (algorithm_id) REFERENCES algorithm_spec(algorithm_id)
);

CREATE VIEW parallel AS
SELECT *
FROM parallel_raw
NATURAL JOIN (SELECT dataset_id, name AS dataset_name, params AS dataset_params, version AS dataset_version FROM top_dataset)
NATURAL JOIN (SELECT queryset_id, name AS queryset_name, params AS queryset_params, version AS queryset_version FROM top_queryset)
NATURAL JOIN (SELECT algorithm_id, name AS algorithm_name, params AS algorithm_params, version AS algorithm_version FROM top_algorithm);
