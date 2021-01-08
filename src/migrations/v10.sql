-- The following three tables contain the triplets name, params and version
-- for datasets, querysets and algorithms
--
CREATE TABLE dataset_spec (
    dataset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    version INT NOT NULL,
    UNIQUE(name, params, version)
);
CREATE TABLE queryset_spec (
    queryset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    version INT NOT NULL,
    UNIQUE(name, params, version)
);
CREATE TABLE algorithm_spec (
    algorithm_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    version INT NOT NULL,
    UNIQUE(name, params, version)
);
--
-- Then we have a table containing all the configurations of the "focus" experiments
--
CREATE TABLE configuration_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    git_rev TEXT NOT NULL,
    hostname TEXT NOT NULL,
    conf_file TEXT,
    dataset_id INTEGER,
    queryset_id INTEGER,
    algorithm_id INTEGER,
    FOREIGN KEY (dataset_id) REFERENCES dataset_spec(dataset_id),
    FOREIGN KEY (queryset_id) REFERENCES queryset_spec(queryset_id),
    FOREIGN KEY (algorithm_id) REFERENCES algorithm_spec(algorithm_id)
);
--
-- And a table with the query focus results themselves
--
CREATE TABLE query_focus (
    id INTEGER NOT NULL,
    query_index INT,
    query_time_ns INT64,
    matches INT64,
    examined INT64,
    FOREIGN KEY (id) REFERENCES configuration_raw(id)
);
--
-- The information from the table above can be matched with information 
-- about individual queries
--
CREATE TABLE queryset_info (
    dataset_id INTEGER NOT NULL,
    queryset_id INTEGER NOT NULL,
    query_index INT,
    selectivity_time INT64,
    selectivity_duration INT64,
    selectivity INT64,
    FOREIGN KEY (dataset_id) REFERENCES dataset_spec(dataset_id),
    FOREIGN KEY (queryset_id) REFERENCES queryset_spec(queryset_id)
);