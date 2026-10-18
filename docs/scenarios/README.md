# Incident drills

Every drill follows the same loop an on-call engineer uses:

1. **Detect.** Something in a dashboard or the shop stats looks wrong.
2. **Inspect.** Inspector, Kafka UI, `kafka-consumer-groups --describe`.
3. **Hypothesise.** Is the problem in the producer, the broker, the consumer, or the data?
4. **Fix.** Change code, config, or topology.
5. **Verify.** Lag drops, throughput recovers, nothing lost, duplicates explained.

Start every drill with the stack running (`make up`, `make shop`, `make inspector`)
and press **Stop everything** on the On call tab first so you begin from a clean state.

| Drill | Trigger | Core concept |
|---|---|---|
| [01 Lag storm](01-lag-storm.md) | On call tab → Lag storm | lag, consumer parallelism, partition count as the ceiling |
| [02 Hot partition](02-hot-partition.md) | On call tab → Hot partition | keys, skew, ordering vs throughput |
| [03 Crash before commit](03-crash-before-commit.md) | On call tab → Crash before commit | committed offsets, at-least-once, idempotency |
| [04 Poison messages](04-poison-messages.md) | On call tab → Poison messages | transient vs permanent errors, retries, dead-letter topic |
| [05 Zombie worker](05-zombie-worker.md) | On call tab → Zombie worker, then **Kill** one | heartbeats, session timeout, rebalance cost |
| [06 Broker down](06-broker-down.md) | `make stop-broker N=2` | replication, leaders, in-sync replicas, min.insync.replicas |
