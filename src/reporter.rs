use crate::types::*;
use crate::ExperimentConfiguration;
use anyhow::{Context, Result};
use chrono::prelude::*;
use rusqlite::*;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub struct Reporter {
    date: DateTime<Utc>,
    config: ExperimentConfiguration,
    config_file: PathBuf,
}

impl Reporter {
    pub fn new<P: AsRef<Path>>(config_file: P, config: ExperimentConfiguration) -> Self {
        Self {
            date: Utc::now(),
            config: config,
            config_file: config_file.as_ref().to_owned(),
        }
    }

    fn sha(&self) -> String {
        let datestr = self.date.to_rfc2822();
        let mut sha = Sha256::new();
        sha.update(datestr);
        // I know that the following is implementation-dependent, but I just need
        // to have a identifier to join different tables created in this run.
        sha.update(format!("{:?}", self.config));

        format!("{:x}", sha.finalize())[..6].to_owned()
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
        answers: Vec<QueryAnswer>,
    ) -> Result<()> {
        let sha = self.sha();
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
            tx.execute(
                "INSERT INTO raw ( sha, date, git_rev, hostname, conf_file,
                                    dataset, dataset_params, dataset_version, 
                                    queryset, queryset_params, queryset_version,
                                    algorithm, algorithm_params, algorithm_version,
                                    time_index_ms, time_query_ms )
                VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16 )",
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
                    elapsed_query
                ],
            )
            .context("error inserting into main table")?;

            let mut stmt = tx.prepare(
                "INSERT INTO query_stats (sha, query_index, query_time_ms, query_count)
            VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (i, ans) in answers.into_iter().enumerate() {
                stmt.execute(params![
                    sha,
                    i as u32,
                    ans.elapsed_millis(),
                    ans.num_matches()
                ])?;
            }
        }

        tx.commit()?;
        conn.close()
            .map_err(|e| e.1)
            .context("error inserting into the database")?;
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
            time_query_ms      INT64 NOT NULL
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

    info!("database schema up tp date");
    Ok(())
}
