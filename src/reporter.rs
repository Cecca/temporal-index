use crate::configuration::*;
use crate::dataset::{Dataset, Queryset};
use crate::types::*;
use anyhow::{Context, Result};
use chrono::prelude::*;
use rusqlite::*;
use rusqlite::{params, Connection};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

pub struct Reporter {
    date: DateTime<Utc>,
    config: ExperimentConfiguration,
    config_file: PathBuf,
}

impl Reporter {
    pub fn new<P: AsRef<Path>>(config_file: P, config: ExperimentConfiguration) -> Result<Self> {
        let date = Utc::now();

        Ok(Self {
            date,
            config: config,
            config_file: config_file.as_ref().to_owned(),
        })
    }

    fn get_db_path() -> std::path::PathBuf {
        std::path::PathBuf::from("temporal-index-results.sqlite")
    }

    pub fn already_run(&self, mode: ExperimentMode) -> Result<Option<i64>> {
        let algorithm = &self.config.algorithm.borrow();
        let dataset = &self.config.dataset;
        let queryset = &self.config.queries;

        let dbpath = Self::get_db_path();
        let hostname = get_hostname()?;
        let conn = Connection::open(dbpath).context("error connecting to the database")?;

        let dataset_id: Option<u32> = conn.query_row(
            "SELECT dataset_id from dataset_spec WHERE name == ?1 AND params == ?2 AND version == ?",
            params![dataset.name(), dataset.parameters(), dataset.version()],
            |row| row.get(0)
        ).optional()
        .context("query dataset id")?;
        let queryset_id: Option<u32> = conn.query_row(
            "SELECT queryset_id from queryset_spec WHERE name == ?1 AND params == ?2 AND version == ?",
            params![queryset.name(), queryset.parameters(), queryset.version()],
            |row| row.get(0)
        ).optional()
        .context("query queryset id")?;
        let algorithm_id: Option<u32> = conn.query_row(
            "SELECT algorithm_id from algorithm_spec WHERE name == ?1 AND params == ?2 AND version == ?",
            params![algorithm.name(), algorithm.parameters(), algorithm.version()],
            |row| row.get(0)
        ).optional()
        .context("query algorithm id")?;

        if dataset_id.is_none() || queryset_id.is_none() || algorithm_id.is_none() {
            // This configuration has not been run, neither in batch nor in focus mode
            return Ok(None);
        }

        match mode {
            ExperimentMode::Batch => conn
                .query_row(
                    "SELECT id FROM batch_raw
                    WHERE hostname == ?1
                    AND dataset_id == ?2
                    AND queryset_id == ?3
                    AND algorithm_id == ?4
                    ",
                    params![hostname, dataset_id, queryset_id, algorithm_id],
                    |row| row.get(0),
                )
                .optional()
                .context("problem checking if the algorithm already ran, batch mode"),
            ExperimentMode::Focus { samples: _ } => conn
                .query_row(
                    "SELECT id FROM focus_configuration_raw
                    WHERE hostname == ?1
                    AND dataset_id == ?2
                    AND queryset_id == ?3
                    AND algorithm_id == ?4
                    ",
                    params![hostname, dataset_id, queryset_id, algorithm_id],
                    |row| row.get(0),
                )
                .optional()
                .context("problem checking if the algorithm already ran, focus mode"),
        }
    }

    fn get_or_insert_dataset(tx: &Transaction, dataset: &Rc<dyn Dataset>) -> Result<u32> {
        tx.execute(
            "INSERT OR IGNORE INTO dataset_spec (name, params, version) VALUES (?1, ?2, ?3)",
            params![dataset.name(), dataset.parameters(), dataset.version()],
        )
        .context("insert into dataset_spec")?;
        tx.query_row(
            "SELECT dataset_id from dataset_spec WHERE name == ?1 AND params == ?2 AND version == ?",
            params![dataset.name(), dataset.parameters(), dataset.version()],
            |row| row.get(0)
        ).context("query dataset id")
    }

    fn get_or_insert_queryset(tx: &Transaction, queryset: &Rc<dyn Queryset>) -> Result<u32> {
        tx.execute(
            "INSERT OR IGNORE INTO queryset_spec (name, params, version) VALUES (?1, ?2, ?3)",
            params![queryset.name(), queryset.parameters(), queryset.version()],
        )
        .context("insert into queryset_spec")?;
        tx.query_row(
            "SELECT queryset_id from queryset_spec WHERE name == ?1 AND params == ?2 AND version == ?",
            params![queryset.name(), queryset.parameters(), queryset.version()],
            |row| row.get(0)
        ).context("query queryset id")
    }

    fn get_or_insert_algorithm(
        tx: &Transaction,
        algorithm: &Rc<RefCell<dyn Algorithm>>,
    ) -> Result<u32> {
        let algorithm = algorithm.borrow();
        tx.execute(
            "INSERT OR IGNORE INTO algorithm_spec (name, params, version) VALUES (?1, ?2, ?3)",
            params![
                algorithm.name(),
                algorithm.parameters(),
                algorithm.version()
            ],
        )
        .context("insert into algorithm_spec")?;
        tx.query_row(
            "SELECT algorithm_id from algorithm_spec WHERE name == ?1 AND params == ?2 AND version == ?",
            params![algorithm.name(), algorithm.parameters(), algorithm.version()],
            |row| row.get(0)
        ).context("query algorithm id")
    }

    pub fn report_focus(&self, results: Vec<FocusResult>) -> Result<()> {
        let dbpath = Self::get_db_path();
        let mut conn = Connection::open(dbpath).context("error connecting to the database")?;
        let hostname = get_hostname()?;

        let algorithm = &self.config.algorithm;
        let dataset = &self.config.dataset;
        let queryset = &self.config.queries;

        let conf_file = self
            .config_file
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("config file path could not be converted to string"))?
            .to_owned();

        let tx = conn.transaction()?;
        {
            // First get the component spec ids, inserting them if necessary
            info!("Getting dataset, queryset, and algorithm IDs");
            let dataset_id = Self::get_or_insert_dataset(&tx, dataset)?;
            let queryset_id = Self::get_or_insert_queryset(&tx, queryset)?;
            let algorithm_id = Self::get_or_insert_algorithm(&tx, algorithm)?;

            // Then insert into the configuration_raw table
            info!("Inserting into the configuration table");
            let updated_cnt = tx
                .execute(
                    "INSERT INTO focus_configuration_raw ( 
                            date, 
                            git_rev, 
                            hostname, 
                            conf_file,
                            dataset_id,
                            queryset_id,
                            algorithm_id)
                    VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7 )",
                    params![
                        self.date.to_rfc3339(),
                        env!("VERGEN_SHA_SHORT"),
                        hostname,
                        conf_file,
                        dataset_id,
                        queryset_id,
                        algorithm_id
                    ],
                )
                .context("error inserting into main table")?;
            if updated_cnt == 0 {
                anyhow::bail!("Insertion didn't happen");
            }

            // Then insert into the query_focus table
            info!("Inserting into the query_focus table");
            let id = tx.last_insert_rowid();
            info!("last inserted row id is {}", id);
            let mut stmt = tx.prepare(
                "INSERT INTO query_focus (
                    id,
                    query_index,
                    query_time_ns,
                    matches,
                    examined )
                    VALUES ( ?1, ?2, ?3, ?4, ?5 )",
            )?;
            for (query_index, res) in results.into_iter().enumerate() {
                stmt.execute(params![
                    id,
                    query_index as u32,
                    res.query_time.as_nanos() as i64,
                    res.n_matches,
                    res.n_examined
                ])?;
            }

            // Finally insert query statistics in the appropriate table
            let count_existing: u32 = tx.query_row(
                "SELECT COUNT(*) FROM queryset_info WHERE dataset_id == ?1 AND queryset_id == ?2",
                params![dataset_id, queryset_id],
                |row| row.get(0),
            )?;
            if count_existing == 0 {
                info!("Inserting stats into queryset_info");
                let mut stmt = tx.prepare(
                    "INSERT INTO queryset_info (
                        dataset_id,
                        queryset_id,
                        query_index,
                        selectivity_time,
                        selectivity_duration,
                        selectivity 
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )?;
                for (query_index, query_stats) in queryset.stats(&dataset.get()?) {
                    stmt.execute(params![
                        dataset_id,
                        queryset_id,
                        query_index,
                        query_stats.selectivity_time,
                        query_stats.selectivity_duration,
                        query_stats.selectivity
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn report_batch(
        &self,
        elapsed_index: i64,
        elapsed_query: i64,
        index_size_bytes: usize,
    ) -> Result<()> {
        let dbpath = Self::get_db_path();
        let mut conn = Connection::open(dbpath).context("error connecting to the database")?;
        let hostname = get_hostname()?;

        let algorithm = &self.config.algorithm;
        let dataset = &self.config.dataset;
        let queryset = &self.config.queries;

        // elapsed query is in milliseconds, we turn it to seconds
        let qps = queryset.get().len() as f64 / (elapsed_query as f64 / 1000.0);

        info!("Queries per second {}", qps);

        let conf_file = self
            .config_file
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("config file path could not be converted to string"))?
            .to_owned();

        let tx = conn.transaction()?;
        {
            let dataset_id = Self::get_or_insert_dataset(&tx, dataset)?;
            let queryset_id = Self::get_or_insert_queryset(&tx, queryset)?;
            let algorithm_id = Self::get_or_insert_algorithm(&tx, algorithm)?;

            let updated_cnt = tx
                .execute(
                    "INSERT INTO batch_raw ( 
                        date, 
                        git_rev, 
                        hostname, 
                        conf_file,
                        dataset_id,
                        queryset_id,
                        algorithm_id,
                        time_index_ms, 
                        time_query_ms, 
                        qps,
                        index_size_bytes )
                    VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11 )",
                    params![
                        self.date.to_rfc3339(),
                        env!("VERGEN_SHA_SHORT"),
                        hostname,
                        conf_file,
                        dataset_id,
                        queryset_id,
                        algorithm_id,
                        elapsed_index,
                        elapsed_query,
                        qps,
                        index_size_bytes as i64
                    ],
                )
                .context("error inserting into main table")?;
            if updated_cnt == 0 {
                anyhow::bail!("Insertion didn't happen");
            }

            let id = tx.last_insert_rowid();
            info!("last inserted row id is {}", id);
        }

        tx.commit()?;
        conn.close()
            .map_err(|e| e.1)
            .context("error inserting into the database")?;
        Ok(())
    }

    pub fn report_period_index_buckets<I: IntoIterator<Item = (usize, usize)>>(
        &self,
        _bucket_info: I,
    ) -> Result<()> {
        // let sha = self.sha.clone();
        // let dbpath = Self::get_db_path();
        // let conn = Connection::open(dbpath).context("error connecting to the database")?;
        // let mut stmt = conn.prepare(
        //     "INSERT INTO period_index_buckets (sha, bucket_index, count)
        //     VALUES (?1, ?2, ?3)",
        // )?;
        // for (index, count) in bucket_info.into_iter() {
        //     stmt.execute(params![sha.clone(), index as u32, count as u32])?;
        // }
        warn!("Reporting of buckets yet to be implemented");

        Ok(())
    }

    pub fn backup(tag: Option<String>) -> Result<()> {
        use chrono::prelude::*;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::fs::File;

        let dbpath = Self::get_db_path();
        let mut backup_path = dbpath.clone();
        backup_path.set_extension(format!(
            "bak{}.{}.gz",
            tag.map(|t| format!(".{}", t)).unwrap_or_else(String::new),
            Utc::now().format("%Y%m%d%H%M")
        ));
        let mut input = File::open(&dbpath)?;
        let mut output = GzEncoder::new(File::create(&backup_path)?, Compression::default());
        std::io::copy(&mut input, &mut output)?;

        Ok(())
    }
}

pub fn get_hostname() -> Result<String> {
    std::env::var("HOST_HOSTNAME").map_or_else(
        |_err| {
            let output = std::process::Command::new("hostname")
                .output()
                .context("Failed to run the hostname command")?;
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
        },
        |host_hostname| Ok(format!("(docker){}", host_hostname)),
    )
}

fn bump(conn: &Connection, ver: u32) -> Result<()> {
    info!("Bumping database to version {}", ver);
    conn.pragma_update(None, "user_version", &ver)
        .context("error updating version")
}

pub fn db_setup() -> Result<()> {
    let mut conn =
        Connection::open(Reporter::get_db_path()).context("error connecting to the database")?;
    let version: u32 = conn
        .query_row(
            "SELECT user_version FROM pragma_user_version",
            params![],
            |row| row.get(0),
        )
        .context("cannot get version of the database")?;
    info!("Current database version is {}", version);

    if version < 1 {
        info!("applying changes for version 1");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw (
            sha               TEXT PRIMARY KEY,
            date              TEXT NOT NULL,
            git_rev           TEXT NOT NULL,
            hostname          TEXT NOT NULL,
            conf_file         TEXT,
            dataset           TEXT NOT NULL,
            dataset_params    TEXT NOT NULL,
            dataset_version   INT NOT NULL,
            queryset           TEXT NOT NULL,
            queryset_params    TEXT NOT NULL,
            queryset_version   INT NOT NULL,
            algorithm         TEXT NOT NULL,
            algorithm_params  TEXT NOT NULL,
            algorithm_version INT NOT NULL,
            time_index_ms      INT64 NOT NULL,
            time_query_ms      INT64 NOT NULL,
            index_size_bytes   INT NOT NULL
            )",
            params![],
        )
        .context("Error creating main table")?;

        conn.execute(
            "CREATE TABLE query_stats (
                sha               TEXT NOT NULL,
                query_index       INT,
                query_time_ms     INT64,
                query_count       INT64,
                FOREIGN KEY (sha) REFERENCES raw(sha)
            )",
            [],
        )?;

        conn.execute(
            "CREATE VIEW top_algorithm_version AS 
            SELECT algorithm, MAX(algorithm_version) as algorithm_version 
            FROM raw GROUP BY algorithm",
            params![],
        )?;
        conn.execute(
            "CREATE VIEW top_dataset_version AS 
            SELECT dataset, MAX(dataset_version) as dataset_version 
            FROM raw GROUP BY dataset",
            params![],
        )?;
        conn.execute(
            "CREATE VIEW top_queryset_version AS 
            SELECT queryset, MAX(queryset_version) as queryset_version 
            FROM raw GROUP BY queryset",
            params![],
        )?;

        conn.execute(
            "CREATE VIEW main AS
            SELECT * FROM raw 
            NATURAL JOIN top_algorithm_version 
            NATURAL JOIN top_queryset_version 
            NATURAL JOIN top_dataset_version",
            params![],
        )?;

        bump(&conn, 1)?;
    }
    if version < 2 {
        info!("Appliying changes for version 2");
        // Add support for dataset information
        conn.execute(
            "CREATE TABLE data_description (
                data_id          INTEGER PRIMARY KEY,
                description      TEXT NOT NULL
            )",
            [],
        )
        .context("data description table creation")?;

        conn.execute(
            "CREATE TABLE duration_distribution (
                data_id            INTEGER NOT NULL,
                duration           INTEGER NOT NULL,
                count              INTEGER NOT NULL,
                FOREIGN KEY (data_id) REFERENCES data_description(data_id)
            )",
            [],
        )
        .context("duration distribution creation")?;

        conn.execute(
            "CREATE TABLE start_distribution (
                data_id            INTEGER NOT NULL,
                start_time         INTEGER NOT NULL,
                count              INTEGER NOT NULL,
                FOREIGN KEY (data_id) REFERENCES data_description(data_id)
            )",
            [],
        )
        .context("duration distribution creation")?;

        conn.execute(
            "CREATE TABLE end_distribution (
                data_id            INTEGER NOT NULL,
                end_time           INTEGER NOT NULL,
                count              INTEGER NOT NULL,
                FOREIGN KEY (data_id) REFERENCES data_description(data_id)
            )",
            [],
        )
        .context("duration distribution creation")?;

        bump(&conn, 2)?;
    }
    if version < 3 {
        info!("Appliying changes for version 3");
        // The information introduced with the previous version takes up too much space
        // in the database.
        conn.execute("DROP TABLE duration_distribution", [])?;
        conn.execute("DROP TABLE start_distribution", [])?;
        conn.execute("DROP TABLE end_distribution", [])?;
        conn.execute("DROP TABLE data_description", [])?;

        bump(&conn, 3)?;
    }
    if version < 4 {
        // Change the resolution of the clock to nanoseconds, dropping all the old
        // results, which don't have a high enough resolution
        conn.execute("DROP TABLE query_stats", [])?;
        conn.execute(
            "CREATE TABLE query_stats (
                sha               TEXT NOT NULL,
                query_index       INT,
                query_time_ns     INT64,
                query_count       INT64,
                FOREIGN KEY (sha) REFERENCES raw(sha)
            )",
            [],
        )?;

        bump(&conn, 4)?;
    }
    if version < 5 {
        // Also record the number of examined intervals
        conn.execute("DROP TABLE query_stats", [])?;
        conn.execute(
            "CREATE TABLE query_stats (
                sha               TEXT NOT NULL,
                query_index       INT,
                query_time_ns     INT64,
                query_count       INT64,
                query_examined    INT64,
                FOREIGN KEY (sha) REFERENCES raw(sha)
            )",
            [],
        )?;

        bump(&conn, 5)?;
    }
    if version < 6 {
        conn.execute(
            "CREATE TABLE period_index_buckets (
            sha             TEXT NOT NULL,
            bucket_index    INT,
            count           INT64 
        )",
            [],
        )?;
        bump(&conn, 6)?;
    }
    if version < 7 {
        conn.execute(
            "ALTER TABLE raw ADD COLUMN is_estimate BOOLEAN DEFAULT FALSE",
            [],
        )?;
        bump(&conn, 7)?;
    }
    if version < 8 {
        // Change the id from sha to rowid
        {
            let tx = conn.transaction()?;
            info!(" . Creating new schemas");
            // We set it autoincrement to ensure that deleted IDs don't get reused
            tx.execute(
                "CREATE TABLE raw_new (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            date              TEXT NOT NULL,
            git_rev           TEXT NOT NULL,
            hostname          TEXT NOT NULL,
            conf_file         TEXT,
            dataset           TEXT NOT NULL,
            dataset_params    TEXT NOT NULL,
            dataset_version   INT NOT NULL,
            queryset           TEXT NOT NULL,
            queryset_params    TEXT NOT NULL,
            queryset_version   INT NOT NULL,
            algorithm         TEXT NOT NULL,
            algorithm_params  TEXT NOT NULL,
            algorithm_version INT NOT NULL,
            time_index_ms      INT64 NOT NULL,
            time_query_ms      INT64 NOT NULL,
            index_size_bytes   INT NOT NULL, 
            is_estimate BOOLEAN DEFAULT FALSE)",
                [],
            )?;
            tx.execute(
                "CREATE TABLE query_stats_new (
            id               INTEGER NOT NULL,
            query_index       INT,
            query_time_ns     INT64,
            query_count       INT64,
            query_examined    INT64,
            FOREIGN KEY (id) REFERENCES raw_new(id))",
                [],
            )?;
            info!(" . Copy data to the new tables");
            tx.execute(
                "insert into raw_new
            select rowid as id, date, git_rev, hostname, conf_file, dataset,
                   dataset_params, dataset_version, queryset, queryset_params,
                   queryset_version, algorithm, algorithm_params, algorithm_version,
                   time_index_ms, time_query_ms, index_size_bytes, is_estimate
            from raw",
                [],
            )?;
            tx.execute(
                "insert into query_stats_new
            select id, query_index, query_time_ns, query_count, query_examined
            from query_stats natural join (select rowid as id, sha from raw)",
                [],
            )?;
            // Now drop everything old
            info!(" . Drop old tables and views");
            tx.execute("drop view main", [])?;
            tx.execute("drop view top_queryset_version", [])?;
            tx.execute("drop view top_algorithm_version", [])?;
            tx.execute("drop view top_dataset_version", [])?;
            tx.execute("drop table period_index_buckets", [])?;
            tx.execute("drop table query_stats", [])?;
            tx.execute("drop table raw", [])?;

            // rename the new tables
            info!(" . Rename new tables");
            tx.execute("alter table raw_new rename to raw", [])?;
            tx.execute("alter table query_stats_new rename to query_stats", [])?;

            // And recreate the views
            info!(" . Recreate the views");
            tx.execute(
                "CREATE VIEW top_algorithm_version AS
            SELECT algorithm, MAX(algorithm_version) as algorithm_version
            FROM raw GROUP BY algorithm",
                params![],
            )?;
            tx.execute(
                "CREATE VIEW top_dataset_version AS
            SELECT dataset, MAX(dataset_version) as dataset_version
            FROM raw GROUP BY dataset",
                params![],
            )?;
            tx.execute(
                "CREATE VIEW top_queryset_version AS
            SELECT queryset, MAX(queryset_version) as queryset_version
            FROM raw GROUP BY queryset",
                params![],
            )?;
            tx.execute(
                "CREATE VIEW main AS
            SELECT * FROM raw
            NATURAL JOIN top_algorithm_version
            NATURAL JOIN top_queryset_version
            NATURAL JOIN top_dataset_version",
                params![],
            )?;
            tx.commit()?;
        }
        info!(" . Clean up and reclaim space");
        conn.execute("VACUUM", [])?;
        bump(&conn, 8)?;
    }
    if version < 9 {
        Reporter::backup(Some("v9".to_owned()))?;
        info!("Dropping table query_stats");
        conn.execute("DROP TABLE query_stats;", [])?;
        info!(" . Clean up and reclaim space");
        conn.execute("VACUUM", [])?;
        bump(&conn, 9)?;
    }
    if version < 10 {
        // This version introduces a new set of tables that allows to inspect query behaviour
        Reporter::backup(Some("v10".to_owned()))?;
        let tx = conn.transaction()?;
        tx.execute_batch(include_str!("migrations/v10.sql"))?;
        tx.commit()?;
        info!("Cleaning up database");
        conn.execute("VACUUM", [])?;
        bump(&conn, 10)?;
    }
    if version < 11 {
        Reporter::backup(Some("v11".to_owned()))?;
        let tx = conn.transaction()?;
        tx.execute_batch(include_str!("migrations/v11.sql"))?;
        tx.commit()?;
        info!("Cleaning up database");
        conn.execute("VACUUM", [])?;
        bump(&conn, 11)?;
    }

    info!("database schema up to date");
    Ok(())
}
