CREATE TABLE focus_configuration_parallel_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    git_rev TEXT NOT NULL,
    hostname TEXT NOT NULL,
    conf_file TEXT,
    num_threads INTEGER,
    dataset_id INTEGER,
    queryset_id INTEGER,
    algorithm_id INTEGER,
    FOREIGN KEY (dataset_id) REFERENCES dataset_spec(dataset_id),
    FOREIGN KEY (queryset_id) REFERENCES queryset_spec(queryset_id),
    FOREIGN KEY (algorithm_id) REFERENCES algorithm_spec(algorithm_id)
);

CREATE TABLE query_focus_parallel (
    id INTEGER NOT NULL,
    query_index INT,
    query_time_ns INT64,
    matches INT64,
    examined INT64,
    FOREIGN KEY (id) REFERENCES focus_configuration_parallel_raw(id)
);

CREATE VIEW focus_configuration_parallel AS
SELECT *
FROM focus_configuration_parallel_raw
NATURAL JOIN (SELECT dataset_id, name AS dataset_name, params AS dataset_params, version AS dataset_version FROM top_dataset)
NATURAL JOIN (SELECT queryset_id, name AS queryset_name, params AS queryset_params, version AS queryset_version FROM top_queryset)
NATURAL JOIN (SELECT algorithm_id, name AS algorithm_name, params AS algorithm_params, version AS algorithm_version FROM top_algorithm);


CREATE VIEW query_focus_parallel_w_stats AS
SELECT *
FROM query_focus_parallel
NATURAL JOIN (
    SELECT id, query_index, selectivity, selectivity_duration, selectivity_time
    FROM queryset_info
    NATURAL JOIN focus_configuration_parallel
);
