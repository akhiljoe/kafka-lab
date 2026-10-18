# Drill 02: Hot partition

## Setup

Lab (http://localhost:8000) → On call tab → Incident drills → **Hot partition**. Three workers, generator at
10/s, key mode `customer`, and every order comes from one customer (`lara`).

## Symptom

* Lag grows even though there are three workers and only 10 orders/s.
* Inspector partition lanes: one partition's bar is growing fast, the other
  two barely move. Only one committed-offset marker is behind.
* `make describe-group`: lag is on exactly one partition.

## Investigate

1. Inspector "Live messages on orders": every message has key `lara` and the
   same partition number.
2. Kafka hashes the key (murmur2) and takes it modulo the partition count.
   Same key, same partition, always. That is the ordering guarantee and the
   skew problem in one mechanism.
3. The worker that owns that partition is maxed out at 5/s (0.2 s delay). The
   other two workers are idle. Adding workers cannot help: they would be
   assigned the idle partitions.

## Hypothesis

Key choice, not consumer count, is the bottleneck.

## Fix

Pick the key by what actually needs ordering:

* `order_id` (switch in the Lab): spreads load evenly, loses per-customer
  ordering. Fine if each order is independent.
* `none`: the producer picks partitions itself in sticky batches; also even,
  also no ordering.
* Keep `customer` but remove the skew at the source: in real systems a single
  hot key is often a bug (a default tenant id, a null key written as the
  string "null", a retry loop from one client).

Switch key mode to `order_id` and clear the hot customer in the generator
card. Watch all three lanes grow and all three markers keep up.

## Verify

* All three partitions show similar message counts and lag near 0.
* All three workers log steady throughput.

## Questions to answer

* Why does the same key always land on the same partition?
* What breaks if you add a 4th partition while keeping `customer` as the key?
* Name a real-world key you would choose for a payments topic, and what you give up.
