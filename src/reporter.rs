use anyhow::{Context,Result};
use crate::Config;
use chrono::prelude::*;
use rusqlite::*;
use rusqlite::{params, Connection, Result as SQLResult};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Result as IOResult, Write};
use std::path::Path;
use std::time::Duration;

pub struct Reporter {
    date: DateTime<Utc>,
    config: Config,
}

impl Reporter {
    pub fn new(config: Config) -> Self {
        Self {
            date: Utc::now(),
            config: config,
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

    pub fn already_run(&self) -> Option<String> {
        if self.config.rerun {
            return None;
        }
        // let dbpath = Self::get_db_path();
        // let conn = Connection::open(dbpath).expect("error connecting to the database");
        // conn.query_row(
        //     "SELECT sha FROM main WHERE
        //         threads == ?2 AND 
        //         hosts == ?3 AND 
        //         dataset == ?4 AND
        //         algorithm == ?5 AND 
        //         parameters == ?6 AND
        //         offline == ?7",
        //     params![
        //         format!("{}", self.config.seed()),
        //         self.config.threads.unwrap_or(1) as u32,
        //         self.config.hosts_string(),
        //         self.config.dataset,
        //         self.config.algorithm.name(),
        //         self.config.algorithm.parameters_string(),
        //         self.config.offline
        //     ],
        //     |row| row.get(0),
        // )
        // .optional()
        // .unwrap_or(None)
        todo!()
    }

    pub fn report(&self, elapsed: i64) -> Result<()> {
        let sha = self.sha();
        let dbpath = Self::get_db_path();
        let conn = Connection::open(dbpath).expect("error connecting to the database");
        let hostname = get_hostname();

        let algorithm = self.config.get_algorithm()?;
        let dataset = self.config.get_dataset()?;
        let queryset = self.config.get_queryset()?;

        conn.execute(
            "INSERT INTO main ( sha, date, git_rev, hostname, 
                                     dataset, dataset_params, dataset_version, 
                                     queryset, queryset_params, queryset_version,
                                     algorithm, algorithm_params, algorithm_version,
                                     total_time_ms )
                VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14 )",
            params![
                sha,
                self.date.to_rfc3339(),
                env!("VERGEN_SHA_SHORT"),
                hostname,
                dataset.name(),
                dataset.parameters(),
                dataset.version(),
                queryset.name(),
                queryset.parameters(),
                queryset.version(),
                algorithm.name(),
                algorithm.parameters(),
                algorithm.version(),
                elapsed,
            ],
        )
        .context("error inserting into main table")?;
        conn.close().expect("error inserting into the database");
        Ok(())
    }
}

pub fn get_hostname() -> String {
    let output = std::process::Command::new("hostname")
        .output()
        .expect("Failed to run the hostname command");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn bump(conn: &Connection, ver: u32) {
    conn.pragma_update(None, "user_version", &ver)
        .expect("error updating version");
}

pub fn db_setup() {
    let conn = Connection::open(Reporter::get_db_path()).expect("error connecting to the database");
    let version: u32 = conn
        .query_row(
            "SELECT user_version FROM pragma_user_version",
            params![],
            |row| row.get(0),
        )
        .unwrap();
    info!("Current database version is {}", version);

    if version < 1 {
        info!("applying changes for version 1");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS main (
            sha               TEXT PRIMARY KEY,
            date              TEXT NOT NULL,
            git_rev           TEXT NOT NULL,
            hostname          TEXT NOT NULL,
            dataset           TEXT NOT NULL,
            dataset_params    TEXT NOT NULL,
            dataset_version   INT NOT NULL,
            queryset           TEXT NOT NULL,
            queryset_params    TEXT NOT NULL,
            queryset_version   INT NOT NULL,
            algorithm         TEXT NOT NULL,
            algorithm_params  TEXT NOT NULL,
            algorithm_version INT NOT NULL,
            total_time_ms      INT64 NOT NULL
            )",
            params![],
        )
        .expect("Error creating main table");

        bump(&conn, 1);
    }

    info!("database schema up tp date");
}
