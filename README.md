# Rust Outbox Sweeper

This is an attempt to learn rust by creating a small outbox sweeper services that connects to a postgres database, and polls a table for not yet dispatched messages.  When it finds messages that require dispatching it splits them by channel and does a batch dispatch for each channel.

This code currently supports SQS and SNS, at the minute SNS requires the channel address to start with `SNS::` otherwise it will assume SQS.

```BASH
docker-compose up
```