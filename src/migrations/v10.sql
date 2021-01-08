--
-- This set of migrations completely rearranges the dataset to consistently
-- support two types of experiments, `batch` and `focus`
-- 
----------------------------------------------------------------------------
--
-- First remove the old tables and views
--
DROP TABLE raw;
DROP VIEW main;
DROP VIEW top_algorithm_version;
DROP VIEW top_dataset_version;
DROP VIEW top_queryset_version;

----------------------------------------------------------------------------
--
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
CREATE TABLE focus_configuration_raw (
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
    FOREIGN KEY (id) REFERENCES focus_configuration_raw(id)
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

--
-- Then we have a table containing the configuration and results of the batch experiments
--
CREATE TABLE batch_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    git_rev TEXT NOT NULL,
    hostname TEXT NOT NULL,
    conf_file TEXT,
    dataset_id INTEGER,
    queryset_id INTEGER,
    algorithm_id INTEGER,
    time_index_ms INT64 NOT NULL,
    time_query_ms INT64 NOT NULL,
    index_size_bytes INT NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES dataset_spec(dataset_id),
    FOREIGN KEY (queryset_id) REFERENCES queryset_spec(queryset_id),
    FOREIGN KEY (algorithm_id) REFERENCES algorithm_spec(algorithm_id)
);

--
----------------------------------------------------------------------
--
--                       MOST RECENT VERSIONS
--
--
CREATE VIEW top_algorithm AS
SELECT *
FROM algorithm_spec
WHERE (name, version) IN (
    SELECT name, MAX(version)
    FROM algorithm_spec
    GROUP BY name
);

CREATE VIEW top_dataset AS
SELECT *
FROM dataset_spec
WHERE (name, version) IN (
    SELECT name, MAX(version)
    FROM dataset_spec
    GROUP BY name
);

CREATE VIEW top_queryset AS
SELECT *
FROM queryset_spec
WHERE (name, version) IN (
    SELECT name, MAX(version)
    FROM queryset_spec
    GROUP BY name
);

CREATE VIEW batch AS
SELECT *
FROM batch_raw
NATURAL JOIN (SELECT dataset_id, name AS dataset_name, params AS dataset_params, version AS dataset_version FROM top_dataset)
NATURAL JOIN (SELECT queryset_id, name AS queryset_name, params AS queryset_params, version AS queryset_version FROM top_queryset)
NATURAL JOIN (SELECT algorithm_id, name AS algorithm_name, params AS algorithm_params, version AS algorithm_version FROM top_algorithm);

CREATE VIEW focus_configuration AS
SELECT *
FROM focus_configuration_raw
NATURAL JOIN (SELECT dataset_id, name AS dataset_name, params AS dataset_params, version AS dataset_version FROM top_dataset)
NATURAL JOIN (SELECT queryset_id, name AS queryset_name, params AS queryset_params, version AS queryset_version FROM top_queryset)
NATURAL JOIN (SELECT algorithm_id, name AS algorithm_name, params AS algorithm_params, version AS algorithm_version FROM top_algorithm);

----------------------------------------------------------------------
--
--                     FOCUS QUERIES WITH STATS
--
CREATE VIEW query_focus_w_stats AS
SELECT *
FROM query_focus
NATURAL JOIN (
    SELECT id, query_index, selectivity, selectivity_duration, selectivity_time
    FROM queryset_info
    NATURAL JOIN focus_configuration
);
