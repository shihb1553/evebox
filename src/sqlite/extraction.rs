use crate::config::Config;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, trace};

use crate::sqlite::prelude::*;

/// How often to run the retention job.  Currently 60 seconds.
const INTERVAL: u64 = 60;

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldConfig {
    #[serde(rename = "type")]
    pub field_type: String,
}

type ExtractionRules = HashMap<String, HashMap<String, FieldConfig>>;

pub(crate) async fn start_extraction_task(
    config: Config,
    conn: Arc<tokio::sync::Mutex<SqliteConnection>>,
) -> anyhow::Result<()> {
    if let Some(extraction_config) = config
        .get_value::<ExtractionRules>("extraction_rules")
        .map_err(|err| anyhow::anyhow!("Bad extraction_rules configuration: {:?}", err))?
    {
        trace!("Starting extraction task");
        let delay = config
            .get::<u64>("database.extraction.interval")?
            .unwrap_or(INTERVAL);
        tokio::task::spawn_blocking(async move || {
            extraction_task(extraction_config, conn, delay).await;
        });
    }

    Ok(())
}

async fn extraction_task(config: ExtractionRules, conn: Arc<tokio::sync::Mutex<SqliteConnection>>, delay: u64) {
    let default_delay = Duration::from_secs(INTERVAL);
    let delay = Duration::from_secs(delay);

    // Delay on startup.
    std::thread::sleep(default_delay);

    // 根据配置动态创建数据库
    if extraction_create(&config, conn.clone()).await.is_err() {
        error!("Failed to create extraction tables");
    }

    loop {
        match extraction_handle(&config, conn.clone()).await {
            Ok(_) => {}
            Err(err) => {
                error!("Error in extraction task: {}", err);
            }
        }

        std::thread::sleep(delay);
    }
}

async fn extraction_create(
    config: &ExtractionRules,
    conn: Arc<tokio::sync::Mutex<SqliteConnection>>,
) -> Result<(), sqlx::Error> {
    let mut conn = conn.lock().await;

    trace!("Creating extraction tables");

    for (table, rule) in config.iter() {
        let mut create_sql = format!(
            "create table if not exists '{}' (id integer primary key AUTOINCREMENT, create_time text default (datetime('now')), match_cnt bigint, user text default ''",
            table
        );

        for (field, field_config) in rule.iter() {
            create_sql += format!(", '{}' {}", field, field_config.field_type).as_str();
        }
        create_sql += ");";

        trace!("Creating table {}: {}", table, create_sql);

        sqlx::query(&create_sql).execute(&mut *conn).await?;
    }

    Ok(())
}

async fn extraction_handle(
    config: &ExtractionRules,
    conn: Arc<tokio::sync::Mutex<SqliteConnection>>,
) -> Result<(), sqlx::Error> {
    let mut conn = conn.lock().await;

    trace!("Handle extraction tables");

    for (table, rule) in config.iter() {
        let mut fields: Vec<String> = Vec::new();
        let mut filters: Vec<String> = Vec::new();
        let mut groups: Vec<String> = Vec::new();
        for (field, _) in rule.iter() {
            fields.push(format!("'{}'", field));
            groups.push(format!("field{}", filters.len()));
            filters.push(format!(
                "json_extract(events.source, '$.{}') as field{}",
                field,
                filters.len()
            ));
        }

        let mut handle_sql = format!("insert into '{}' (match_cnt, ", table);
        handle_sql += fields.join(", ").as_str();
        handle_sql += ") select count(*), ";
        handle_sql += filters.join(", ").as_str();
        handle_sql += &format!(" from events where json_extract(events.source, '$.event_type') = 'alert' and json_extract(events.source, '$.alert.signature_id') = {} and escalated != 1 group by ", table).to_string();
        handle_sql += groups.join(", ").as_str();

        let n = sqlx::query(&handle_sql).execute(&mut *conn).await?.rows_affected();
        trace!("Inserted {} rows into {}", n, table);

        let delete_sql = format!(
            "delete from events where json_extract(events.source, '$.event_type') = 'alert' and json_extract(events.source, '$.alert.signature_id') = {} and escalated != 1",
            table
        );

        let n = sqlx::query(&delete_sql).execute(&mut *conn).await?.rows_affected();
        trace!("Deleted {} rows from events", n);
    }

    Ok(())
}
