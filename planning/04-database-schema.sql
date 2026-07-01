-- =============================================================================
-- Distributed Message Broker — Relational Schema
-- =============================================================================
-- Database: PostgreSQL 15+
-- Encoding: UTF-8
-- Collation: C (byte-order, deterministic comparisons)
--
-- DESIGN RATIONALE
-- ----------------
-- This schema persists the durable, operational state of the broker cluster.
-- Hot-path message data lives in the append-only segment files on disk (the
-- log/ module); this schema covers everything that must survive a full cluster
-- restart and cannot be trivially reconstructed from the segment files alone:
--
--   1. Cluster topology          — brokers, topics, partitions, replica assignments
--   2. Raft durable state        — currentTerm, votedFor, per-partition
--   3. Consumer group offsets    — committed read positions per (group, partition)
--   4. Idempotent producer state — last committed sequence per (producer, partition)
--   5. Metadata / leadership     — current leader epoch per partition
--   6. Retention policy config   — per-topic overrides
--   7. Audit / event log         — leadership changes, broker joins/leaves
--
-- NORMALIZATION
-- -------------
-- The schema is in Third Normal Form (3NF) throughout. Junction tables are
-- used for the many-to-many relationship between partitions and brokers
-- (replica assignments). No computed columns are stored redundantly; all
-- derivable values are left to the application or views.
--
-- MIGRATION STRUCTURE
-- -------------------
-- Migrations are managed by Flyway (classpath: db/migration/).
-- Naming convention: V{version}__{description}.sql
--   V1__create_cluster_topology.sql
--   V2__create_raft_state.sql
--   V3__create_consumer_group_offsets.sql
--   V4__create_idempotent_producer_state.sql
--   V5__create_retention_policy.sql
--   V6__create_audit_log.sql
--   V7__create_indexes.sql
--   V8__create_views.sql
-- Each migration is idempotent where possible (CREATE TABLE IF NOT EXISTS,
-- CREATE INDEX IF NOT EXISTS). Destructive migrations (DROP, ALTER … DROP)
-- require a separate Vn__rollback_*.sql companion kept in db/rollback/.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schema namespace
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS broker;

SET search_path = broker, public;

-- ---------------------------------------------------------------------------
-- Shared domain types
-- ---------------------------------------------------------------------------

-- Broker node identifier: small positive integer assigned at cluster bootstrap.
-- Matches the integer broker-id used throughout the Raft and protocol layers.
CREATE DOMAIN broker_id AS SMALLINT
    CHECK (VALUE > 0);

-- Logical offset within a partition log: non-negative 64-bit integer.
CREATE DOMAIN log_offset AS BIGINT
    CHECK (VALUE >= 0);

-- Raft term: strictly positive 64-bit integer.
CREATE DOMAIN raft_term AS BIGINT
    CHECK (VALUE > 0);

-- Leader epoch: alias for raft_term; kept separate for clarity at the
-- application layer even though the values are identical.
CREATE DOMAIN leader_epoch AS BIGINT
    CHECK (VALUE > 0);

-- =============================================================================
-- 1. CLUSTER TOPOLOGY
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1.1  brokers
-- ---------------------------------------------------------------------------
-- One row per broker node in the static cluster membership.
-- "Static" means membership is configured at bootstrap via Docker Compose /
-- environment variables; there is no dynamic gossip. This table is the
-- authoritative source of broker addresses for the MetadataService.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS brokers (
    broker_id           broker_id       NOT NULL,
    host                VARCHAR(253)    NOT NULL,   -- RFC-1123 hostname or IP
    port                INTEGER         NOT NULL
                            CHECK (port BETWEEN 1 AND 65535),
    rack                VARCHAR(64)     NULL,        -- optional AZ / rack label
    status              VARCHAR(16)     NOT NULL
                            DEFAULT 'ONLINE'
                            CHECK (status IN ('ONLINE', 'OFFLINE', 'DECOMMISSIONED')),
    registered_at       TIMESTAMPTZ     NOT NULL DEFAULT now(),
    last_heartbeat_at   TIMESTAMPTZ     NULL,        -- updated by health-check path

    CONSTRAINT pk_brokers PRIMARY KEY (broker_id),
    CONSTRAINT uq_brokers_host_port UNIQUE (host, port)
);

COMMENT ON TABLE  brokers                  IS 'Static cluster membership: one row per broker node.';
COMMENT ON COLUMN brokers.broker_id        IS 'Immutable integer node identifier, matches Raft peerId.';
COMMENT ON COLUMN brokers.rack             IS 'Optional placement label used for replica rack-awareness (future).';
COMMENT ON COLUMN brokers.last_heartbeat_at IS 'Wall-clock time of the most recent heartbeat received by the metadata service.';

-- ---------------------------------------------------------------------------
-- 1.2  topics
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS topics (
    topic_id            SERIAL          NOT NULL,
    topic_name          VARCHAR(249)    NOT NULL,   -- Kafka-compatible max length
    partition_count     SMALLINT        NOT NULL
                            CHECK (partition_count > 0),
    replication_factor  SMALLINT        NOT NULL
                            CHECK (replication_factor > 0),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ     NULL,       -- soft-delete; NULL = active

    CONSTRAINT pk_topics PRIMARY KEY (topic_id),
    CONSTRAINT uq_topics_name UNIQUE (topic_name),
    CONSTRAINT chk_topics_rf_lte_brokers
        CHECK (replication_factor <= 7)             -- sanity cap; enforced in app too
);

COMMENT ON TABLE  topics                   IS 'Named topics; partition_count and replication_factor are immutable after creation in this MVP.';
COMMENT ON COLUMN topics.deleted_at        IS 'Non-NULL signals a pending deletion; the retention enforcer removes segment files before the row is hard-deleted.';

-- ---------------------------------------------------------------------------
-- 1.3  partitions
-- ---------------------------------------------------------------------------
-- One row per (topic, partition-index) pair.
-- The partition_index is the zero-based integer used in all wire-protocol
-- messages (PUBLISH_REQ.partitionId, POLL_REQ.partitionId, etc.).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS partitions (
    partition_id        SERIAL          NOT NULL,
    topic_id            INTEGER         NOT NULL,
    partition_index     SMALLINT        NOT NULL
                            CHECK (partition_index >= 0),
    -- Current committed high-watermark: the highest offset that has been
    -- majority-replicated and acknowledged to a producer.  Updated by the
    -- leader after each CommitAdvancer cycle.  Followers read this value
    -- on restart to know where to resume polling.
    high_watermark      log_offset      NOT NULL DEFAULT 0,
    -- The leader epoch at the time the high_watermark was last advanced.
    -- Used to detect stale cached values (INV-4).
    hwm_leader_epoch    leader_epoch    NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_partitions PRIMARY KEY (partition_id),
    CONSTRAINT uq_partitions_topic_index UNIQUE (topic_id, partition_index),
    CONSTRAINT fk_partitions_topic
        FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
        ON DELETE RESTRICT                          -- must delete partitions first
);

COMMENT ON TABLE  partitions               IS 'One row per (topic, partition-index). Partition-index is the zero-based integer on the wire.';
COMMENT ON COLUMN partitions.high_watermark IS 'Highest majority-replicated offset; consumers may not read past this (INV-5).';
COMMENT ON COLUMN partitions.hwm_leader_epoch IS 'Leader epoch when high_watermark was last written; stale writes are rejected.';

-- ---------------------------------------------------------------------------
-- 1.4  partition_replicas  (junction: partitions ↔ brokers)
-- ---------------------------------------------------------------------------
-- Records which brokers hold a replica of each partition and which one is
-- currently the leader.  In the static-membership MVP, this table is
-- populated at bootstrap and the leader column is updated by Raft elections.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS partition_replicas (
    partition_id        INTEGER         NOT NULL,
    broker_id           broker_id       NOT NULL,
    is_leader           BOOLEAN         NOT NULL DEFAULT FALSE,
    -- The epoch at which this broker became (or last confirmed itself as)
    -- leader for this partition.  NULL for non-leaders.
    leader_epoch        leader_epoch    NULL,
    -- Highest Raft log index known to be replicated on this replica.
    -- Corresponds to matchIndex[] in the ReplicationPipeline.
    match_index         BIGINT          NOT NULL DEFAULT 0
                            CHECK (match_index >= 0),
    -- Whether this replica is currently in-sync (has caught up to within
    -- replica.lag.max.messages of the leader).  Informational only in MVP;
    -- Raft majority is the commit gate, not ISR.
    in_sync             BOOLEAN         NOT NULL DEFAULT TRUE,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_partition_replicas PRIMARY KEY (partition_id, broker_id),
    CONSTRAINT fk_pr_partition
        FOREIGN KEY (partition_id) REFERENCES partitions (partition_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_pr_broker
        FOREIGN KEY (broker_id) REFERENCES brokers (broker_id)
        ON DELETE RESTRICT
);

-- At most one leader per partition at any point in time.
-- This partial unique index enforces INV-4 at the database layer as a
-- belt-and-suspenders check; the primary enforcement is Raft term fencing.
CREATE UNIQUE INDEX IF NOT EXISTS uq_partition_replicas_one_leader
    ON partition_replicas (partition_id)
    WHERE is_leader = TRUE;

COMMENT ON TABLE  partition_replicas       IS 'Junction table: which brokers hold each partition replica, and which is the current Raft leader.';
COMMENT ON COLUMN partition_replicas.leader_epoch IS 'Raft term in which this broker became leader; NULL for followers. Enforces INV-4.';
COMMENT ON COLUMN partition_replicas.match_index  IS 'Highest Raft log index replicated here; mirrors ReplicationPipeline.matchIndex.';
COMMENT ON INDEX  uq_partition_replicas_one_leader IS 'Partial unique index: at most one leader row per partition. Belt-and-suspenders for INV-4.';

-- =============================================================================
-- 2. RAFT DURABLE STATE
-- =============================================================================
-- The Raft paper requires that currentTerm and votedFor survive crashes.
-- In the broker implementation these are written to a per-partition file
-- (PersistentState.java); this table provides a queryable replica of that
-- state, useful for operational dashboards and post-mortem analysis.
-- The file-based store is authoritative on startup; this table is updated
-- asynchronously and must not be used as the primary durability mechanism.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raft_state (
    partition_id        INTEGER         NOT NULL,
    broker_id           broker_id       NOT NULL,
    -- Raft §5.1: latest term server has seen; updated on stable storage
    -- before responding to any RPC.
    current_term        raft_term       NOT NULL DEFAULT 1,
    -- Raft §5.2: candidateId that received vote in current term; NULL = none.
    voted_for           broker_id       NULL,
    -- Highest log index known to be committed on this node.
    commit_index        BIGINT          NOT NULL DEFAULT 0
                            CHECK (commit_index >= 0),
    -- Highest log index applied to the state machine on this node.
    last_applied        BIGINT          NOT NULL DEFAULT 0
                            CHECK (last_applied >= 0),
    -- Role as of the last persisted state change.
    role                VARCHAR(16)     NOT NULL DEFAULT 'FOLLOWER'
                            CHECK (role IN ('FOLLOWER', 'CANDIDATE', 'LEADER')),
    persisted_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_raft_state PRIMARY KEY (partition_id, broker_id),
    CONSTRAINT fk_raft_state_partition
        FOREIGN KEY (partition_id) REFERENCES partitions (partition_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_raft_state_broker
        FOREIGN KEY (broker_id) REFERENCES brokers (broker_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_raft_state_voted_for
        FOREIGN KEY (voted_for) REFERENCES brokers (broker_id)
        ON DELETE SET NULL,
    CONSTRAINT chk_raft_state_applied_lte_commit
        CHECK (last_applied <= commit_index)
);

COMMENT ON TABLE  raft_state              IS 'Queryable mirror of per-partition Raft durable state (currentTerm, votedFor). Authoritative copy is the on-disk raft-state.bin file.';
COMMENT ON COLUMN raft_state.current_term IS 'Monotonically increasing Raft term; persisted before any RPC response (Raft §5.1).';
COMMENT ON COLUMN raft_state.voted_for    IS 'Broker that received this node''s vote in current_term; NULL means no vote cast yet.';
COMMENT ON COLUMN raft_state.commit_index IS 'Highest log index known committed; never decreases.';
COMMENT ON COLUMN raft_state.last_applied IS 'Highest log index applied to the partition log state machine; last_applied <= commit_index always.';

-- =============================================================================
-- 3. CONSUMER GROUP OFFSETS
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 3.1  consumer_groups
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consumer_groups (
    group_id            VARCHAR(255)    NOT NULL,
    description         TEXT            NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_consumer_groups PRIMARY KEY (group_id)
);

COMMENT ON TABLE consumer_groups IS 'Registered consumer groups. group_id is the application-supplied string identifier.';

-- ---------------------------------------------------------------------------
-- 3.2  consumer_group_offsets
-- ---------------------------------------------------------------------------
-- Stores the last committed read offset for each (group, partition) pair.
-- This is the durable backing store for ConsumerGroupManager / OffsetStore.
-- The in-memory ConcurrentHashMap in the broker is rebuilt from this table
-- (or from the per-group append-only file) on restart.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consumer_group_offsets (
    group_id            VARCHAR(255)    NOT NULL,
    partition_id        INTEGER         NOT NULL,
    -- The next offset the consumer will fetch; i.e., the offset of the last
    -- successfully processed record + 1.  Matches the semantics of
    -- COMMIT_OFFSET_REQ.offset in the wire protocol.
    committed_offset    log_offset      NOT NULL,
    -- Wall-clock time of the commit; used for monitoring lag staleness.
    committed_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    -- Metadata blob the consumer may attach (e.g., JSON with processing
    -- checkpoint info).  Optional; NULL in the MVP.
    metadata            TEXT            NULL,

    CONSTRAINT pk_consumer_group_offsets PRIMARY KEY (group_id, partition_id),
    CONSTRAINT fk_cgo_group
        FOREIGN KEY (group_id) REFERENCES consumer_groups (group_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_cgo_partition
        FOREIGN KEY (partition_id) REFERENCES partitions (partition_id)
        ON DELETE CASCADE
);

COMMENT ON TABLE  consumer_group_offsets            IS 'Durable committed read offsets per (consumer group, partition). Rebuilt into broker memory on startup.';
COMMENT ON COLUMN consumer_group_offsets.committed_offset IS 'Next offset to fetch; equals last-processed-offset + 1. Consumers must not read past the partition high_watermark (INV-5).';

-- ---------------------------------------------------------------------------
-- 3.3  consumer_group_offset_history  (audit trail)
-- ---------------------------------------------------------------------------
-- Append-only history of every offset commit.  Allows replay and lag
-- analysis.  The primary table (consumer_group_offsets) holds only the
-- latest value; this table holds every historical commit.
-- Partitioned by committed_at (monthly) for manageability.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consumer_group_offset_history (
    history_id          BIGSERIAL       NOT NULL,
    group_id            VARCHAR(255)    NOT NULL,
    partition_id        INTEGER         NOT NULL,
    committed_offset    log_offset      NOT NULL,
    committed_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    metadata            TEXT            NULL,

    CONSTRAINT pk_cgo_history PRIMARY KEY (history_id, committed_at)
) PARTITION BY RANGE (committed_at);

-- Default partition catches anything not covered by explicit monthly slices.
-- The application (or a maintenance job) creates monthly partitions ahead of time:
--   CREATE TABLE broker.consumer_group_offset_history_y2024m01
--       PARTITION OF broker.consumer_group_offset_history
--       FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS consumer_group_offset_history_default
    PARTITION OF consumer_group_offset_history DEFAULT;

COMMENT ON TABLE consumer_group_offset_history IS 'Append-only audit trail of every consumer offset commit. Partitioned monthly by committed_at.';

-- =============================================================================
-- 4. IDEMPOTENT PRODUCER STATE
-- =============================================================================
-- Mirrors the in-memory IdempotencyStore (ConcurrentHashMap<producerId →
-- lastCommittedSeq> per partition).  Rebuilt from the partition log on
-- broker restart; this table provides a queryable snapshot and a faster
-- recovery path when the segment files are large.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS producer_state (
    producer_id         BIGINT          NOT NULL,   -- UUID-derived long (ProducerClient)
    partition_id        INTEGER         NOT NULL,
    -- Highest sequence number successfully committed for this producer on
    -- this partition.  Incoming seq must equal last_committed_seq + 1.
    last_committed_seq  BIGINT          NOT NULL
                            CHECK (last_committed_seq >= 0),
    -- Offset assigned to the record with last_committed_seq.
    -- Returned as the cached ack on duplicate detection (INV-3).
    last_committed_offset log_offset    NOT NULL,
    -- Epoch in which the last commit occurred; used to detect stale state
    -- after a leader change.
    leader_epoch        leader_epoch    NULL,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_producer_state PRIMARY KEY (producer_id, partition_id),
    CONSTRAINT fk_ps_partition
        FOREIGN KEY (partition_id) REFERENCES partitions (partition_id)
        ON DELETE CASCADE
);

COMMENT ON TABLE  producer_state                    IS 'Idempotent producer deduplication state: last committed sequence per (producer, partition). Enforces INV-3.';
COMMENT ON COLUMN producer_state.last_committed_seq IS 'Incoming seq == last_committed_seq + 1 → new write. Incoming seq <= last_committed_seq → duplicate; return cached ack. Incoming seq > last_committed_seq + 1 → SEQUENCE_GAP error.';
COMMENT ON COLUMN producer_state.last_committed_offset IS 'Offset of the record with last_committed_seq; echoed back on duplicate detection without a second append.';

-- =============================================================================
-- 5. RETENTION POLICY
-- =============================================================================
-- Per-topic retention overrides.  If no row exists for a topic, the broker
-- uses the cluster-wide defaults from BrokerConfig.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS retention_policies (
    topic_id                INTEGER         NOT NULL,
    -- Maximum total size of all segment files for this topic's partition,
    -- in bytes.  NULL = use cluster default.
    retention_bytes         BIGINT          NULL
                                CHECK (retention_bytes IS NULL OR retention_bytes > 0),
    -- Maximum age of a log segment before it is eligible for deletion,
    -- in milliseconds.  NULL = use cluster default.
    retention_ms            BIGINT          NULL
                                CHECK (retention_ms IS NULL OR retention_ms > 0),
    -- Fsync policy override for this topic.  NULL = use cluster default.
    fsync_policy            VARCHAR(16)     NULL
                                CHECK (fsync_policy IS NULL
                                    OR fsync_policy IN ('EVERY_WRITE', 'PERIODIC', 'OS_MANAGED')),
    -- Maximum segment file size in bytes before the SegmentManager rolls
    -- to a new segment.  NULL = use cluster default (1 GB).
    segment_bytes           BIGINT          NULL
                                CHECK (segment_bytes IS NULL OR segment_bytes > 0),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_retention_policies PRIMARY KEY (topic_id),
    CONSTRAINT fk_rp_topic
        FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
        ON DELETE CASCADE
);

COMMENT ON TABLE  retention_policies              IS 'Per-topic retention and segment configuration overrides. Absent row → cluster-wide BrokerConfig defaults apply.';
COMMENT ON COLUMN retention_policies.retention_bytes IS 'Total partition log size cap in bytes; oldest segments deleted first when exceeded.';
COMMENT ON COLUMN retention_policies.retention_ms    IS 'Maximum segment age in milliseconds; segments older than this are eligible for deletion.';
COMMENT ON COLUMN retention_policies.fsync_policy    IS 'Override for FsyncPolicy enum: EVERY_WRITE | PERIODIC | OS_MANAGED.';

-- =============================================================================
-- 6. AUDIT / EVENT LOG
-- =============================================================================
-- Append-only record of significant cluster events: leadership changes,
-- broker status transitions, topic creation/deletion.  Used by the chaos
-- harness (DivergenceChecker, LinearizabilityChecker) and for post-mortem
-- analysis.  Never updated; only inserted.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cluster_events (
    event_id            BIGSERIAL       NOT NULL,
    event_type          VARCHAR(64)     NOT NULL
                            CHECK (event_type IN (
                                'BROKER_REGISTERED',
                                'BROKER_ONLINE',
                                'BROKER_OFFLINE',
                                'BROKER_DECOMMISSIONED',
                                'TOPIC_CREATED',
                                'TOPIC_DELETED',
                                'PARTITION_LEADER_ELECTED',
                                'PARTITION_LEADER_STEPPED_DOWN',
                                'RAFT_TERM_ADVANCED',
                                'SEGMENT_ROLLED',
                                'SEGMENT_DELETED',
                                'RETENTION_ENFORCED',
                                'CONSUMER_GROUP_CREATED',
                                'CONSUMER_GROUP_DELETED',
                                'CHAOS_FAULT_INJECTED',
                                'CHAOS_FAULT_HEALED'
                            )),
    -- The broker that generated this event.
    source_broker_id    broker_id       NULL,
    -- Subject identifiers (all nullable; only relevant fields populated).
    topic_id            INTEGER         NULL,
    partition_id        INTEGER         NULL,
    group_id            VARCHAR(255)    NULL,
    -- For leadership events: the new leader's broker_id.
    new_leader_id       broker_id       NULL,
    -- For leadership events: the Raft term of the new leader.
    new_leader_epoch    leader_epoch    NULL,
    -- For Raft term events: the term that was advanced to.
    new_term            raft_term       NULL,
    -- Free-form JSON payload for event-specific details.
    payload             JSONB           NULL,
    occurred_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_cluster_events PRIMARY KEY (event_id)
);

COMMENT ON TABLE  cluster_events                IS 'Append-only audit log of significant cluster lifecycle events. Never updated after insert.';
COMMENT ON COLUMN cluster_events.event_type     IS 'Discriminator for the event; determines which subject columns are populated.';
COMMENT ON COLUMN cluster_events.new_leader_epoch IS 'Raft term of the newly elected leader; corresponds to leaderEpoch in PartitionReplica (INV-4).';
COMMENT ON COLUMN cluster_events.payload        IS 'JSONB bag for event-specific fields not covered by the typed columns.';

-- =============================================================================
-- 7. INDEXES
-- =============================================================================
-- Indexes are created separately from tables so that the migration that
-- creates them (V7__create_indexes.sql) can be re-run independently if an
-- index needs to be rebuilt.  All indexes use IF NOT EXISTS.
-- ---------------------------------------------------------------------------

-- brokers
CREATE INDEX IF NOT EXISTS idx_brokers_status
    ON brokers (status)
    WHERE status = 'ONLINE';

-- partitions
CREATE INDEX IF NOT EXISTS idx_partitions_topic_id
    ON partitions (topic_id);

-- partition_replicas
CREATE INDEX IF NOT EXISTS idx_pr_broker_id
    ON partition_replicas (broker_id);

CREATE INDEX IF NOT EXISTS idx_pr_leader
    ON partition_replicas (partition_id, broker_id)
    WHERE is_leader = TRUE;

-- raft_state
CREATE INDEX IF NOT EXISTS idx_raft_state_broker_id
    ON raft_state (broker_id);

CREATE INDEX IF NOT EXISTS idx_raft_state_role
    ON raft_state (role)
    WHERE role = 'LEADER';

-- consumer_group_offsets
CREATE INDEX IF NOT EXISTS idx_cgo_partition_id
    ON consumer_group_offsets (partition_id);

-- consumer_group_offset_history
CREATE INDEX IF NOT EXISTS idx_cgo_history_group_partition
    ON consumer_group_offset_history (group_id, partition_id, committed_at DESC);

-- producer_state
CREATE INDEX IF NOT EXISTS idx_ps_partition_id
    ON producer_state (partition_id);

CREATE INDEX IF NOT EXISTS idx_ps_updated_at
    ON producer_state (updated_at DESC);

-- cluster_events
CREATE INDEX IF NOT EXISTS idx_ce_occurred_at
    ON cluster_events (occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_ce_event_type
    ON cluster_events (event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_ce_partition_id
    ON cluster_events (partition_id, occurred_at DESC)
    WHERE partition_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ce_source_broker
    ON cluster_events (source_broker_id, occurred_at DESC)
    WHERE source_broker_id IS NOT NULL;

-- =============================================================================
-- 8. VIEWS
-- =============================================================================
-- Convenience views for operational queries and the MetadataService.
-- Views are defined in V8__create_views.sql; included here for completeness.
-- ---------------------------------------------------------------------------

-- Current partition leadership map (answers METADATA_REQ).
CREATE OR REPLACE VIEW v_partition_leaders AS
SELECT
    t.topic_name,
    p.partition_index,
    p.partition_id,
    pr.broker_id        AS leader_broker_id,
    b.host              AS leader_host,
    b.port              AS leader_port,
    pr.leader_epoch,
    p.high_watermark,
    p.hwm_leader_epoch
FROM partitions p
JOIN topics t
    ON t.topic_id = p.topic_id
JOIN partition_replicas pr
    ON pr.partition_id = p.partition_id
   AND pr.is_leader = TRUE
JOIN brokers b
    ON b.broker_id = pr.broker_id
WHERE t.deleted_at IS NULL;

COMMENT ON VIEW v_partition_leaders IS 'Current leader broker for every active partition; used by MetadataService to answer METADATA_REQ.';

-- Consumer lag per (group, partition).
CREATE OR REPLACE VIEW v_consumer_lag AS
SELECT
    cgo.group_id,
    t.topic_name,
    p.partition_index,
    p.partition_id,
    cgo.committed_offset,
    p.high_watermark,
    (p.high_watermark - cgo.committed_offset) AS lag_messages,
    cgo.committed_at
FROM consumer_group_offsets cgo
JOIN partitions p
    ON p.partition_id = cgo.partition_id
JOIN topics t
    ON t.topic_id = p.topic_id
WHERE t.deleted_at IS NULL;

COMMENT ON VIEW v_consumer_lag IS 'Consumer lag in messages per (group, partition); lag = high_watermark - committed_offset.';

-- Raft cluster health summary.
CREATE OR REPLACE VIEW v_raft_health AS
SELECT
    rs.partition_id,
    t.topic_name,
    p.partition_index,
    rs.broker_id,
    b.host,
    b.port,
    rs.role,
    rs.current_term,
    rs.commit_index,
    rs.last_applied,
    (rs.commit_index - rs.last_applied) AS apply_lag,
    rs.persisted_at
FROM raft_state rs
JOIN partitions p
    ON p.partition_id = rs.partition_id
JOIN topics t
    ON t.topic_id = p.topic_id
JOIN brokers b
    ON b.broker_id = rs.broker_id
WHERE t.deleted_at IS NULL;

COMMENT ON VIEW v_raft_health IS 'Per-node Raft state summary: role, term, commit/apply indices, and apply lag.';

-- Recent leadership changes (last 100 events).
CREATE OR REPLACE VIEW v_recent_leader_elections AS
SELECT
    ce.event_id,
    ce.occurred_at,
    t.topic_name,
    p.partition_index,
    ce.new_leader_id,
    b.host              AS new_leader_host,
    ce.new_leader_epoch,
    ce.source_broker_id AS reporting_broker_id,
    ce.payload
FROM cluster_events ce
LEFT JOIN partitions p
    ON p.partition_id = ce.partition_id
LEFT JOIN topics t
    ON t.topic_id = p.topic_id
LEFT JOIN brokers b
    ON b.broker_id = ce.new_leader_id
WHERE ce.event_type = 'PARTITION_LEADER_ELECTED'
ORDER BY ce.occurred_at DESC
LIMIT 100;

COMMENT ON VIEW v_recent_leader_elections IS 'Most recent 100 leader election events; useful for diagnosing election storms.';

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================