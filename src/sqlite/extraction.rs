use crate::config::Config;
use anyhow::Result;
use core::ops::Sub;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace};

const DEFAULT_RANGE: usize = 7;

/// How often to run the retention job.  Currently 60 seconds.
const INTERVAL: u64 = 3;

/// The time to sleep between retention runs if not all old events
/// were deleted.
const REPEAT_INTERVAL: u64 = 1;

/// Number of events to delete per run.
const LIMIT: usize = 1000;

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldConfig {
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub indexed: bool,
}

type ExtractionRules = HashMap<String, HashMap<String, FieldConfig>>;

pub fn start_extraction_task(config: Config, conn: Arc<Mutex<Connection>>) -> anyhow::Result<()> {
    if let Some(extraction_config) = config
        .get_value::<ExtractionRules>("extraction_rules")
        .map_err(|err| anyhow::anyhow!("Bad extraction_rules configuration: {:?}", err))?
    {
        info!("Starting extraction task");
        tokio::task::spawn_blocking(|| {
            extraction_task(extraction_config, conn);
        });
    }

    Ok(())
}

fn extraction_task(config: ExtractionRules, conn: Arc<Mutex<rusqlite::Connection>>) {
    let default_delay = Duration::from_secs(INTERVAL);
    let report_interval = Duration::from_secs(60);
    let filename = conn
        .lock()
        .map(|conn| conn.path().map(|p| p.to_string()))
        .unwrap();

    // Delay on startup.
    std::thread::sleep(default_delay);

    // 根据配置动态创建数据库
    if extraction_create(config, &conn).is_err() {
        error!("Failed to create extraction tables");
    }

    let mut last_report = Instant::now();
    let mut count: usize = 0;

    loop {
        let mut delay = default_delay;

        std::thread::sleep(delay);
    }
}

fn extraction_create(
    config: ExtractionRules,
    conn: &Arc<Mutex<rusqlite::Connection>>,
) -> Result<(), rusqlite::Error> {
    let mut conn = conn.lock().unwrap();
    let tx = conn.transaction()?;

    info!("Creating extraction tables");

    for (table, rule) in config.iter() {
        let mut create_sql = format!(
            "create table if not exists '{}' (id int primary key, create_time text, update_time text, match_cnt bigint, user text",
            table
        );

        for (field, field_config) in rule.iter() {
            create_sql += format!(", '{}' {}", field, field_config.field_type).as_str();
        }
        create_sql += ");";

        info!("Creating table {}: {}", table, create_sql);

        tx.execute(create_sql.as_str(), [])?;
    }

    tx.commit()?;

    Ok(())
}

fn delete_to_size(conn: &Arc<Mutex<Connection>>, filename: &str, bytes: usize) -> Result<usize> {
    let file_size = crate::file::file_size(filename)? as usize;
    if file_size < bytes {
        trace!("Database less than max size of {} bytes", bytes);
        return Ok(0);
    }

    let mut deleted = 0;
    loop {
        let file_size = crate::file::file_size(filename)? as usize;
        if file_size < bytes {
            return Ok(deleted);
        }

        trace!("Database file size of {} bytes is greater than max allowed size of {} bytes, deleting events",
	       file_size, bytes);
        deleted += delete_events(conn, 1000)?;
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn delete_by_range(conn: &Arc<Mutex<Connection>>, range: usize, limit: usize) -> Result<usize> {
    let now = time::OffsetDateTime::now_utc();
    let period = std::time::Duration::from_secs(range as u64 * 86400);
    let older_than = now.sub(period);
    let mut conn = conn.lock().unwrap();
    let timer = Instant::now();
    trace!("Deleting events older than {range} days");
    let tx = conn.transaction()?;
    let sql = r#"DELETE FROM events
                WHERE rowid IN
                    (SELECT rowid FROM events WHERE timestamp < ? and escalated = 0 ORDER BY timestamp ASC LIMIT ?)"#;
    let n = tx.execute(
        sql,
        params![older_than.unix_timestamp_nanos() as i64, limit as i64],
    )?;
    tx.commit()?;
    if n > 0 {
        debug!(
            "Deleted {n} events older than {} ({range} days) in {} ms",
            &older_than,
            timer.elapsed().as_millis()
        );
    }
    Ok(n)
}

fn delete_events(conn: &Arc<Mutex<rusqlite::Connection>>, limit: usize) -> Result<usize> {
    let sql = "delete from events where rowid in (select rowid from events where escalated = 0 order by timestamp asc limit ?)";
    let conn = conn.lock().unwrap();
    let timer = Instant::now();
    let mut st = conn.prepare(sql)?;
    let n = st.execute(params![limit])?;
    trace!("Deleted {n} events in {} ms", timer.elapsed().as_millis());
    Ok(n)
}
