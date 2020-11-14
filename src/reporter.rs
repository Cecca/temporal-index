use crate::configuration::*;
use crate::types::*;
use anyhow::{Context, Result};
use chrono::prelude::*;
use rusqlite::*;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct Reporter {
    date: DateTime<Utc>,
    config: ExperimentConfiguration,
    config_file: PathBuf,
    sha: String,
}

impl Reporter {
    pub fn new<P: AsRef<Path>>(config_file: P, config: ExperimentConfiguration) -> Result<Self> {
        let date = Utc::now();
        let datestr = date.to_rfc2822();
        let mut sha = Sha256::new();
        let hostname = get_hostname()?;
        sha.update(datestr);
        sha.update(hostname);
        sha.update(config.algorithm.borrow().descr());
        sha.update(config.dataset.descr());
        sha.update(config.queries.descr());

        let sha = format!("{:x}", sha.finalize());
        Ok(Self {
            date,
            config: config,
            config_file: config_file.as_ref().to_owned(),
            sha,
        })
    }

    fn get_db_path() -> std::path::PathBuf {
        std::path::PathBuf::from("temporal-index-results.sqlite")
    }

    pub fn already_run(&self) -> Result<Option<String>> {
        let algorithm = &self.config.algorithm;
        let dataset = &self.config.dataset;
        let queryset = &self.config.queries;

        let dbpath = Self::get_db_path();
        let hostname = get_hostname()?;
        let conn = Connection::open(dbpath).context("error connecting to the database")?;
        conn.query_row(
            "SELECT sha FROM main
            WHERE hostname == ?1
              AND dataset == ?2
              AND dataset_params == ?3
              AND dataset_version == ?4
              AND queryset == ?5
              AND queryset_params == ?6
              AND queryset_version == ?7
              AND algorithm == ?8
              AND algorithm_params == ?9
              AND algorithm_version == ?10
            ",
            params![
                hostname,
                dataset.name(),
                dataset.parameters(),
                dataset.version(),
                queryset.name(),
                queryset.parameters(),
                queryset.version(),
                algorithm.borrow().name(),
                algorithm.borrow().parameters(),
                algorithm.borrow().version()
            ],
            |row| row.get(0),
        )
        .optional()
        .context("problem checking if the algorithm already ran")
    }

    pub fn report(
        &self,
        elapsed_index: i64,
        elapsed_query: i64,
        index_size_bytes: u32,
        answers: std::result::Result<Vec<QueryAnswer>, Duration>,
    ) -> Result<()> {
        let sha = self.sha.clone();
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
            let elapsed_query = if answers.is_err() {
                warn!("using estimated query time");
                answers.as_ref().unwrap_err().as_millis() as i64
            } else {
                elapsed_query
            };

            tx.execute(
                "INSERT INTO raw ( sha, date, git_rev, hostname, conf_file,
                                    dataset, dataset_params, dataset_version, 
                                    queryset, queryset_params, queryset_version,
                                    algorithm, algorithm_params, algorithm_version,
                                    time_index_ms, time_query_ms, index_size_bytes, is_estimate )
                VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18 )",
                params![
                    sha,
                    self.date.to_rfc3339(),
                    env!("VERGEN_SHA_SHORT"),
                    hostname,
                    conf_file,
                    dataset.name(),
                    dataset.parameters(),
                    dataset.version(),
                    queryset.name(),
                    queryset.parameters(),
                    queryset.version(),
                    algorithm.borrow().name(),
                    algorithm.borrow().parameters(),
                    algorithm.borrow().version(),
                    elapsed_index,
                    elapsed_query,
                    index_size_bytes,
                    answers.is_err()
                ],
            )
            .context("error inserting into main table")?;

            if let Ok(answers) = answers {
                let mut stmt = tx.prepare(
                "INSERT INTO query_stats (sha, query_index, query_time_ns, query_count, query_examined)
                VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
                for (i, ans) in answers.into_iter().enumerate() {
                    stmt.execute(params![
                        sha,
                        i as u32,
                        ans.elapsed_nanos(),
                        ans.num_matches(),
                        ans.num_examined(),
                    ])?;
                }
            }
        }

        tx.commit()?;
        conn.close()
            .map_err(|e| e.1)
            .context("error inserting into the database")?;
        Ok(())
    }

    pub fn report_period_index_buckets<I: IntoIterator<Item = (usize, usize)>>(
        &self,
        bucket_info: I,
    ) -> Result<()> {
        let sha = self.sha.clone();
        let dbpath = Self::get_db_path();
        let conn = Connection::open(dbpath).context("error connecting to the database")?;
        let mut stmt = conn.prepare(
            "INSERT INTO period_index_buckets (sha, bucket_index, count)
            VALUES (?1, ?2, ?3)",
        )?;
        for (index, count) in bucket_info.into_iter() {
            stmt.execute(params![sha.clone(), index as u32, count as u32])?;
        }

        Ok(())
    }

    pub fn backup() -> Result<()> {
        use chrono::prelude::*;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::fs::File;

        let dbpath = Self::get_db_path();
        let mut backup_path = dbpath.clone();
        backup_path.set_extension(format!("bak.{}.gz", Utc::now().format("%Y%m%d%H%M")));
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
    let conn =
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
            NO_PARAMS,
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
            NO_PARAMS,
        )
        .context("data description table creation")?;

        conn.execute(
            "CREATE TABLE duration_distribution (
                data_id            INTEGER NOT NULL,
                duration           INTEGER NOT NULL,
                count              INTEGER NOT NULL,
                FOREIGN KEY (data_id) REFERENCES data_description(data_id)
            )",
            NO_PARAMS,
        )
        .context("duration distribution creation")?;

        conn.execute(
            "CREATE TABLE start_distribution (
                data_id            INTEGER NOT NULL,
                start_time         INTEGER NOT NULL,
                count              INTEGER NOT NULL,
                FOREIGN KEY (data_id) REFERENCES data_description(data_id)
            )",
            NO_PARAMS,
        )
        .context("duration distribution creation")?;

        conn.execute(
            "CREATE TABLE end_distribution (
                data_id            INTEGER NOT NULL,
                end_time           INTEGER NOT NULL,
                count              INTEGER NOT NULL,
                FOREIGN KEY (data_id) REFERENCES data_description(data_id)
            )",
            NO_PARAMS,
        )
        .context("duration distribution creation")?;

        bump(&conn, 2)?;
    }
    if version < 3 {
        info!("Appliying changes for version 3");
        // The information introduced with the previous version takes up too much space
        // in the database.
        conn.execute("DROP TABLE duration_distribution", NO_PARAMS)?;
        conn.execute("DROP TABLE start_distribution", NO_PARAMS)?;
        conn.execute("DROP TABLE end_distribution", NO_PARAMS)?;
        conn.execute("DROP TABLE data_description", NO_PARAMS)?;

        bump(&conn, 3)?;
    }
    if version < 4 {
        // Change the resolution of the clock to nanoseconds, dropping all the old
        // results, which don't have a high enough resolution
        conn.execute("DROP TABLE query_stats", NO_PARAMS)?;
        conn.execute(
            "CREATE TABLE query_stats (
                sha               TEXT NOT NULL,
                query_index       INT,
                query_time_ns     INT64,
                query_count       INT64,
                FOREIGN KEY (sha) REFERENCES raw(sha)
            )",
            NO_PARAMS,
        )?;

        bump(&conn, 4)?;
    }
    if version < 5 {
        // Also record the number of examined intervals
        conn.execute("DROP TABLE query_stats", NO_PARAMS)?;
        conn.execute(
            "CREATE TABLE query_stats (
                sha               TEXT NOT NULL,
                query_index       INT,
                query_time_ns     INT64,
                query_count       INT64,
                query_examined    INT64,
                FOREIGN KEY (sha) REFERENCES raw(sha)
            )",
            NO_PARAMS,
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
            NO_PARAMS,
        )?;
        bump(&conn, 6)?;
    }
    if version < 7 {
        conn.execute(
            "ALTER TABLE raw ADD COLUMN is_estimate BOOLEAN DEFAULT FALSE",
            NO_PARAMS,
        )?;
        bump(&conn, 7)?;
    }

    info!("database schema up tp date");
    Ok(())
}
