-- Audit store schema, MySQL 8.0.30+ / 8.4 LTS, InnoDB, utf8mb4.
-- See docs/audit-pipeline-design.md section 5.
--
-- Notes:
--  * Partitioned InnoDB tables support neither FOREIGN KEYs nor FULLTEXT indexes.
--  * Every UNIQUE/PRIMARY key on a partitioned table must contain the partition column, hence
--    PRIMARY KEY (occurred_at, event_id). occurred_at is fixed at record time in the producer outbox,
--    so retries of the same event always collide on this key (dedupe).
--  * Partition names are pYYYY_MM; Audit.Maintenance keeps >= 3 future months.

SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO';

-- ---------------------------------------------------------------------------------------------
-- Dimension tables (dictionary encoding keeps the hot table and its indexes small)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE dim_service (
  service_id   SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name         VARCHAR(64)  NOT NULL,
  PRIMARY KEY (service_id),
  UNIQUE KEY ux_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE dim_action (
  action_id    SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name         VARCHAR(128) NOT NULL,           -- e.g. order.cancelled
  PRIMARY KEY (action_id),
  UNIQUE KEY ux_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE dim_entity_type (
  entity_type_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name           VARCHAR(64) NOT NULL,          -- e.g. order
  PRIMARY KEY (entity_type_id),
  UNIQUE KEY ux_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ---------------------------------------------------------------------------------------------
-- Hot table: narrow, indexed, used by every list/filter query
-- ---------------------------------------------------------------------------------------------
CREATE TABLE audit_event (
  occurred_at     DATETIME(6)       NOT NULL,                 -- UTC, set once by producer, µs-truncated
  event_id        BINARY(16)        NOT NULL,                 -- UUIDv7 (or v5 from idempotency key)
  ingested_at     DATETIME(6)       NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  service_id      SMALLINT UNSIGNED NOT NULL,
  action_id       SMALLINT UNSIGNED NOT NULL,
  entity_type_id  SMALLINT UNSIGNED NOT NULL,
  entity_id       VARCHAR(128)      NOT NULL,
  actor_type      TINYINT UNSIGNED  NOT NULL,                 -- 1 user, 2 service, 3 system, 4 anonymous
  actor_id        VARCHAR(128)      NOT NULL,
  outcome         TINYINT UNSIGNED  NOT NULL,                 -- 1 success, 2 failure, 3 denied
  correlation_id  BINARY(16)        NULL,
  source_ip       VARBINARY(16)     NULL,                     -- INET6_ATON form
  summary         VARCHAR(512)      NULL,
  content_hash    BINARY(32)        NOT NULL,                 -- SHA-256 of canonical (JCS) event JSON
  PRIMARY KEY (occurred_at, event_id),
  KEY ix_entity          (entity_type_id, entity_id, occurred_at),
  KEY ix_actor           (actor_id, occurred_at),
  KEY ix_service_action  (service_id, action_id, occurred_at),
  KEY ix_correlation     (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin
PARTITION BY RANGE COLUMNS (occurred_at) (
  PARTITION p2025_11 VALUES LESS THAN ('2025-12-01'),
  PARTITION p2025_12 VALUES LESS THAN ('2026-01-01'),
  PARTITION p2026_01 VALUES LESS THAN ('2026-02-01'),
  PARTITION p2026_02 VALUES LESS THAN ('2026-03-01'),
  PARTITION p2026_03 VALUES LESS THAN ('2026-04-01'),
  PARTITION p2026_04 VALUES LESS THAN ('2026-05-01'),
  PARTITION p2026_05 VALUES LESS THAN ('2026-06-01'),
  PARTITION p2026_06 VALUES LESS THAN ('2026-07-01'),
  PARTITION p2026_07 VALUES LESS THAN ('2026-08-01'),
  PARTITION p2026_08 VALUES LESS THAN ('2026-09-01'),
  PARTITION p2026_09 VALUES LESS THAN ('2026-10-01'),
  PARTITION p2026_10 VALUES LESS THAN ('2026-11-01'),
  PARTITION p2026_11 VALUES LESS THAN ('2026-12-01'),
  PARTITION p2026_12 VALUES LESS THAN ('2027-01-01'),
  PARTITION p2027_01 VALUES LESS THAN ('2027-02-01'),
  PARTITION p2027_02 VALUES LESS THAN ('2027-03-01'),
  PARTITION pmax     VALUES LESS THAN (MAXVALUE)      -- safety net only; alert if it holds rows
);

-- ---------------------------------------------------------------------------------------------
-- Detail table: full original event JSON, same PK/partitioning, read only on demand
-- ---------------------------------------------------------------------------------------------
CREATE TABLE audit_event_detail (
  occurred_at  DATETIME(6) NOT NULL,
  event_id     BINARY(16)  NOT NULL,
  event_json   JSON        NOT NULL,
  PRIMARY KEY (occurred_at, event_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE COLUMNS (occurred_at) (
  PARTITION p2025_11 VALUES LESS THAN ('2025-12-01'),
  PARTITION p2025_12 VALUES LESS THAN ('2026-01-01'),
  PARTITION p2026_01 VALUES LESS THAN ('2026-02-01'),
  PARTITION p2026_02 VALUES LESS THAN ('2026-03-01'),
  PARTITION p2026_03 VALUES LESS THAN ('2026-04-01'),
  PARTITION p2026_04 VALUES LESS THAN ('2026-05-01'),
  PARTITION p2026_05 VALUES LESS THAN ('2026-06-01'),
  PARTITION p2026_06 VALUES LESS THAN ('2026-07-01'),
  PARTITION p2026_07 VALUES LESS THAN ('2026-08-01'),
  PARTITION p2026_08 VALUES LESS THAN ('2026-09-01'),
  PARTITION p2026_09 VALUES LESS THAN ('2026-10-01'),
  PARTITION p2026_10 VALUES LESS THAN ('2026-11-01'),
  PARTITION p2026_11 VALUES LESS THAN ('2026-12-01'),
  PARTITION p2026_12 VALUES LESS THAN ('2027-01-01'),
  PARTITION p2027_01 VALUES LESS THAN ('2027-02-01'),
  PARTITION p2027_02 VALUES LESS THAN ('2027-03-01'),
  PARTITION pmax     VALUES LESS THAN (MAXVALUE)
);

-- ---------------------------------------------------------------------------------------------
-- Immutability: block row-level changes (does NOT stop DROP PARTITION / TRUNCATE; see design 5.5)
-- ---------------------------------------------------------------------------------------------
DELIMITER //
CREATE TRIGGER trg_audit_event_no_update BEFORE UPDATE ON audit_event FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'audit_event is append-only';//
CREATE TRIGGER trg_audit_event_no_delete BEFORE DELETE ON audit_event FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'audit_event is append-only';//
CREATE TRIGGER trg_audit_detail_no_update BEFORE UPDATE ON audit_event_detail FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'audit_event_detail is append-only';//
CREATE TRIGGER trg_audit_detail_no_delete BEFORE DELETE ON audit_event_detail FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'audit_event_detail is append-only';//
DELIMITER ;

-- ---------------------------------------------------------------------------------------------
-- Rollups (recomputed, never incremented — at-least-once consumers double count)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE audit_rollup_hourly (
  hour         DATETIME          NOT NULL,
  service_id   SMALLINT UNSIGNED NOT NULL,
  action_id    SMALLINT UNSIGNED NOT NULL,
  outcome      TINYINT UNSIGNED  NOT NULL,
  event_count  INT UNSIGNED      NOT NULL,
  PRIMARY KEY (hour, service_id, action_id, outcome),
  KEY ix_service_action (service_id, action_id, hour)
) ENGINE=InnoDB;

CREATE TABLE audit_dirty_hour (
  hour       DATETIME    NOT NULL,
  marked_at  DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  PRIMARY KEY (hour)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------------------------
-- Tamper evidence: hourly seals (also written to WORM object storage)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE audit_seal (
  hour          DATETIME         NOT NULL,
  seal_version  SMALLINT UNSIGNED NOT NULL,   -- >1 = amendment for events that arrived after sealing
  event_count   INT UNSIGNED     NOT NULL,
  digest        BINARY(32)       NOT NULL,    -- SHA-256 over ordered (event_id || content_hash)
  prev_digest   BINARY(32)       NULL,        -- chains seals (cheap: one row/hour, single writer)
  sealed_at     DATETIME(6)      NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  worm_uri      VARCHAR(512)     NOT NULL,
  PRIMARY KEY (hour, seal_version)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------------------------
-- Export jobs (the only mutable table the API writes to)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE audit_export_job (
  export_id      BINARY(16)   NOT NULL,
  requested_by   VARCHAR(128) NOT NULL,
  requested_at   DATETIME(6)  NOT NULL,       -- also the ingested_at snapshot bound
  status         ENUM('queued','running','completed','failed','cancelled','expired') NOT NULL,
  request_json   JSON         NOT NULL,       -- audit-export-request.v1
  rows_written   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  bytes_written  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  object_key     VARCHAR(512) NULL,
  error          VARCHAR(1024) NULL,
  started_at     DATETIME(6)  NULL,
  completed_at   DATETIME(6)  NULL,
  expires_at     DATETIME(6)  NULL,
  PRIMARY KEY (export_id),
  KEY ix_status (status, requested_at),
  KEY ix_user (requested_by, requested_at)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------------------------
-- Least-privilege accounts
-- ---------------------------------------------------------------------------------------------
-- CREATE USER 'audit_ingestor'@'%' IDENTIFIED BY '...';
-- GRANT SELECT, INSERT ON audit.audit_event         TO 'audit_ingestor'@'%';
-- GRANT SELECT, INSERT ON audit.audit_event_detail  TO 'audit_ingestor'@'%';
-- GRANT SELECT, INSERT ON audit.dim_service         TO 'audit_ingestor'@'%';
-- GRANT SELECT, INSERT ON audit.dim_action          TO 'audit_ingestor'@'%';
-- GRANT SELECT, INSERT ON audit.dim_entity_type     TO 'audit_ingestor'@'%';
-- GRANT INSERT, UPDATE ON audit.audit_dirty_hour    TO 'audit_ingestor'@'%';  -- INSERT ... ON DUPLICATE KEY
--
-- CREATE USER 'audit_api'@'%' IDENTIFIED BY '...';          -- connects to the READ REPLICA for queries
-- GRANT SELECT ON audit.* TO 'audit_api'@'%';
-- GRANT INSERT, UPDATE ON audit.audit_export_job TO 'audit_api'@'%';  -- on the primary
--
-- DDL (partition maintenance) runs as 'audit_maint' — monitored, no app uses it.

-- ---------------------------------------------------------------------------------------------
-- Producer-side outbox (lives in EACH service's own database; created by Audit.Client migration)
-- ---------------------------------------------------------------------------------------------
-- CREATE TABLE audit_outbox (
--   id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
--   event_id     BINARY(16)   NOT NULL,
--   occurred_at  DATETIME(6)  NOT NULL,
--   message_key  VARCHAR(200) NOT NULL,       -- {entity.type}:{entity.id}
--   payload      JSON         NOT NULL,       -- audit-event.v1, already serialized
--   attempts     INT UNSIGNED NOT NULL DEFAULT 0,
--   lease_owner  VARCHAR(128) NULL,
--   lease_until  DATETIME(6)  NULL,
--   PRIMARY KEY (id),
--   UNIQUE KEY ux_event (event_id),
--   KEY ix_lease (lease_until, id)
-- ) ENGINE=InnoDB;
