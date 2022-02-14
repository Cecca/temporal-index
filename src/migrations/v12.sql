--
-- Then we have a table containing all the configurations of the "focus" experiments
--
CREATE TABLE insertions_configuration_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        git_rev TEXT NOT NULL,
        hostname TEXT NOT NULL,
        conf_file TEXT,
        batch_size INTEGER,
        dataset_id INTEGER,
        algorithm_id INTEGER,
        FOREIGN KEY (dataset_id) REFERENCES dataset_spec(dataset_id),
        FOREIGN KEY (algorithm_id) REFERENCES algorithm_spec(algorithm_id)
);

--
-- And a table with the query focus results themselves
--
CREATE TABLE insertions_raw (
        id INTEGER NOT NULL,
        batch INT,
        batch_time_ns INT64,
        FOREIGN KEY (id) REFERENCES insertions_configuration_raw(id)
);

CREATE VIEW insertions_configuration AS
SELECT
        *
FROM
        insertions_configuration_raw NATURAL
        JOIN (
                SELECT
                        dataset_id,
                        name AS dataset_name,
                        params AS dataset_params,
                        version AS dataset_version
                FROM
                        top_dataset
        ) NATURAL
        JOIN (
                SELECT
                        algorithm_id,
                        name AS algorithm_name,
                        params AS algorithm_params,
                        version AS algorithm_version
                FROM
                        top_algorithm
        );

CREATE VIEW insertions AS
SELECT
        *
FROM
        insertions_raw NATURAL
        JOIN insertions_configuration;