use crate::config::Config;
use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{error, info, trace};

/// How often to run the retention job.  Currently 60 seconds.
const INTERVAL: u64 = 3;

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
        trace!("Starting extraction task");
        tokio::task::spawn_blocking(|| {
            extraction_task(extraction_config, conn);
        });
    }

    Ok(())
}

fn extraction_task(config: ExtractionRules, conn: Arc<Mutex<rusqlite::Connection>>) {
    let default_delay = Duration::from_secs(INTERVAL);

    // Delay on startup.
    std::thread::sleep(default_delay);

    // 根据配置动态创建数据库
    if extraction_create(&config, &conn).is_err() {
        error!("Failed to create extraction tables");
    }

    loop {
        let _ = extraction_handle(&config, &conn);

        std::thread::sleep(default_delay);
    }
}

fn extraction_create(
    config: &ExtractionRules,
    conn: &Arc<Mutex<rusqlite::Connection>>,
) -> Result<(), rusqlite::Error> {
    let mut conn = conn.lock().unwrap();
    let tx = conn.transaction()?;

    trace!("Creating extraction tables");

    for (table, rule) in config.iter() {
        let mut create_sql = format!(
            "create table if not exists '{}' (id int primary key, create_time text, update_time text, match_cnt bigint, user text",
            table
        );

        for (field, field_config) in rule.iter() {
            create_sql += format!(", '{}' {}", field, field_config.field_type).as_str();
        }
        create_sql += ");";

        trace!("Creating table {}: {}", table, create_sql);

        tx.execute(create_sql.as_str(), [])?;
    }

    tx.commit()?;

    Ok(())
}

use serde_json::Value;
fn extraction_handle(
    config: &ExtractionRules,
    conn: &Arc<Mutex<rusqlite::Connection>>,
) -> Result<(), rusqlite::Error> {
    let mut conn = conn.lock().unwrap();
    let tx = conn.transaction()?;

    info!("Handle extraction tables");

    for (table, rule) in config.iter() {
        let mut count = 0;

        let mut stmt = tx.prepare(
            "select rowid, source from events where escalated = 0 and source like '%?%'",
        )?;
        let mut rows = stmt.query([table]).unwrap();

        while let Some(row) = rows.next()? { 
            let row_id = row.get::<_, i64>(0)?;
            let source = row.get::<_, String>(1)?;

            let obj: Value = serde_json::from_str(&source).unwrap();

            for (field, field_config) in rule.iter() {
                let value = obj.get(field);
                if value.is_none() {
                    continue;
                }

                let value = value.unwrap();

                let value = match field_config.field_type.as_str() {
                    "text" => value.to_string(),
                    "integer" => value.to_string(),
                    "real" => value.to_string(),
                }
            }

            // 查询table中是否有数据
            let mut stmt = tx.prepare("select * from '{}' where id = ?", table)?;
            let mut rows = stmt.query([row_id])?;
            if rows.next()?.is_none() {
                // 没有数据, 插入数据
                let mut create_sql = format!(
                    "insert into '{}' (id, create_time, update_time, match_cnt, user) values (?, ?, ?, ?, ?)",
                    table
                );
                for (field, field_config) in rule.iter() {
                    create_sql += format!(", '{}'", field).as_str();
                }
                create_sql += ");";
                tx.execute(create_sql.as_str(), [row_id, "2021-01-01", "2021-01-01", 0, "admin"])?;
            } else {
                // 有数据, 更新数据
                let mut update_sql = format!(
                    "update '{}' set update_time = ?, match_cnt = ?, user = ?",
                    table
                );
                for (field, field_config) in rule.iter() {
                    update_sql += format!(", '{}' = ?", field).as_str();
                }
                update_sql += " where id = ?";
            }

            
            let mut create_sql = format!(
                "insert into '{}' (id, create_time, update_time, match_cnt, user) values (?, ?, ?, ?, ?)",
                table
            );

            count += 1;
        }

        tx.execute(create_sql.as_str(), [])?;
    }

    tx.commit()?;

    Ok(())
}
