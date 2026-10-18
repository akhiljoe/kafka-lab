# Drill 01: Lag storm

## Setup

Lab (http://localhost:8000) → On call tab → Incident drills → **Lag storm**. This starts the generator at 25
orders/s and one worker that spends 0.3 s per order.

## Symptom

* Grafana "Total consumer lag" climbs steadily. "Records in" is around 25/s,
  "Records committed" around 3/s.
* Inspector: the committed-offset marker on each `orders` partition falls
  further behind the right edge.

## Investigate

1. Is lag on one partition or all of them?

   ```bash
   make describe-group GROUP=order-processor
   ```

   `LAG` grows on all three partitions and all three have the same
   `CONSUMER-ID`. One consumer owns everything.

2. Is the consumer slow or stuck? Open the worker log in the Lab. It
   processes steadily, about 3 per second. Not stuck, just slow.

3. Do the maths. One worker at 0.3 s per order handles 3.3 orders/s. Input is
   25/s. The gap is 22/s and it goes straight into lag.

## Hypothesis

Consumer throughput is the bottleneck. Options: make each order cheaper, or
process in parallel.

## Fix

Start two more workers in the same group (`order-processor`, delay 0.3).
Watch the inspector's group change log: `PREPARING_REBALANCING → STABLE`,
then each worker owns one partition.

Throughput becomes roughly 3 × 3.3 = 10/s. Still below 25/s.

Now start a **fourth** worker. Watch the assignment: it gets nothing. Three
partitions means at most three active consumers in a group. The partition
count is the parallelism ceiling.

Real fixes from here, in the order a team would try them:

1. Make the work cheaper (`--delay 0.05` in this lab; in real life, batch the
   database writes, drop a slow call, add a cache).
2. Add partitions (`kafka-topics --alter --partitions 6`) and workers. Note
   that adding partitions changes which partition a key maps to, so ordering
   per key is briefly broken across the boundary.
3. Reduce the input rate upstream (backpressure).

## Verify

* Inspector "Committed" rate ≥ produced rate.
* Lag chart turns downward and reaches zero.
* Shop latency p95 drops back under a second.
* `make describe-group` shows LAG 0 across partitions.

## Questions to answer

* If 3 partitions and 2 consumers, how can partitions be assigned?
* Why did the fourth worker sit idle?
* Which of the three real fixes is the only one that does not change the data layout?
