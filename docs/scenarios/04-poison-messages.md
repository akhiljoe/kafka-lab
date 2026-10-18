# Drill 04: Poison messages

## Setup

Lab (http://localhost:8000) → On call tab → Incident drills → **Poison messages**. Generator at 4/s, one worker
with a 60% failure rate. In the worker 70% of failures are transient
("inventory service timeout"), 30% permanent ("payment declined").

## Symptom

* On call tab stats: "Retries" climbs, "Dead-lettered" climbs, latency p95 jumps.
* Order rows show `retry 1 · inventory service timeout`, then either `ready`
  or `DLQ · payment declined`.
* Inspector: `orders.dlq` partition bar starts growing. Lag on `orders` rises
  because retries with backoff slow the worker down.

## Investigate

1. Inspector → topic tab `orders.dlq` → live messages. Each dead letter carries
   the original order plus `dlq_reason`, `source_partition`, `source_offset`.
   That is enough to replay it later.
2. Worker log: `transient 'inventory service timeout', retry 1 in 0.2s`,
   `retry 2 in 0.4s`, `retry 3 in 0.8s`. Exponential backoff.
3. Grafana "Order outcomes per minute": ready vs dead_lettered vs retries.

## What the worker does and why

| Error type | Behaviour | Reason |
|---|---|---|
| transient | retry in place, backoff, up to 3 times | the world will probably be fine in a second |
| transient, retries exhausted | dead-letter | do not block the partition forever on one message |
| permanent | dead-letter immediately | retrying cannot change the outcome |

Retrying **in place** keeps ordering but blocks every message behind the bad
one on that partition. A **retry topic** (`orders.retry`) unblocks the
partition but reorders. Pick based on whether ordering matters more than
latency.

## Fix

In this lab the "fix" is operational: lower `fail_rate` to 0.05 on a new
worker, then replay the DLQ when the downstream is healthy again:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.dlq --from-beginning --max-messages 5
```

A replay tool would read `orders.dlq`, drop `dlq_reason`, and produce back to
`orders` with the original key.

## Verify

* Dead-letter rate drops to the expected permanent-failure share.
* Lag on `orders` drains.
* Every dead letter is accounted for: `placed = ready + dead_lettered + in_flight`.

## Questions to answer

* Why is "unknown item" permanent but "timeout" transient?
* What does a poison message do to a partition if you have no DLQ?
* What would you need to store with a dead letter to replay it safely?
