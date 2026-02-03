PRAGMA trusted_schema=1;

-- 2210054
CREATE TABLE if NOT EXISTS '2210054' (
    id integer primary key AUTOINCREMENT,
    create_time text default (datetime('now')),
    -- update_time text,
    match_cnt bigint,
    user text default 'shihb',
    'flow.src_ip' text,
    'flow.dest_ip' text
    -- PRIMARY KEY (src_ip, dest_ip)
);
CREATE INDEX if not exists 'idx_2210054' on '2210054' ('flow.src_ip', 'flow.dest_ip');

-- BEGIN TRANSACTION;
INSERT INTO '2210054' (match_cnt, 'flow.src_ip', 'flow.dest_ip')
SELECT count(*), json_extract(events.source, '$.flow.src_ip') as src_ip,
       json_extract(events.source, '$.flow.dest_ip') as dest_ip
FROM events
WHERE json_extract(events.source, '$.event_type') = 'alert'
  AND json_extract(events.source, '$.alert.signature_id') = 2210054
  AND escalated != 1
GROUP BY src_ip, dest_ip;

DELETE FROM events
WHERE json_extract(events.source, '$.event_type') = 'alert'
  AND json_extract(events.source, '$.alert.signature_id') = 2210054
  AND escalated != 1;

-- COMMIT;
