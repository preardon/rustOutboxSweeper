CREATE SCHEMA IF NOT EXISTS core
    AUTHORIZATION postgres;

CREATE TABLE core.outbox (
                             id BIGSERIAL PRIMARY KEY,
                             message_id VARCHAR(64) UNIQUE NOT NULL,
                             message_type VARCHAR(1024) NOT NULL,
                             channel_address VARCHAR(2048) NOT NULL,
                             dispatched TIMESTAMPTZ DEFAULT NULL,
                             "timestamp" TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
                             body TEXT NOT NULL,
                             trace_parent VARCHAR(55) DEFAULT NULL
);

COMMENT ON COLUMN core.outbox.message_id IS 'The id of the message';
COMMENT ON COLUMN core.outbox.message_type IS 'The Type of message';
COMMENT ON COLUMN core.outbox.channel_address IS 'The ARN of the SNS Channel';
COMMENT ON COLUMN core.outbox.dispatched IS 'The time that the message was dispatched from the outbox';
COMMENT ON COLUMN core.outbox."timestamp" IS 'The time that this message was placed in the outbox';
COMMENT ON COLUMN core.outbox.body IS 'The payload of the message';
COMMENT ON COLUMN core.outbox.trace_parent IS 'The Open Telemetry Parent Trace Id';

CREATE INDEX idx_outbox_dispatched ON core.outbox (dispatched);