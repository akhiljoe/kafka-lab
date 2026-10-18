# Drill 06: Broker down

## Setup

Watch tab: pick a slow worker and send a few orders so every partition has
data. Open the Inspector next to it. Note which broker leads each partition
(the `B1`, `B2`, `B3` labels next to the partition bars, or `make topics`).

## Break it

```bash
make stop-broker N=2
```

## Symptom

* Inspector: within a few seconds the partitions that had broker 2 as leader
  show a new leader. In-sync replica count drops from 3/3 to 2/3 on every
  partition (broker 2 held a copy of all of them).
* Watch tab: the broker 2 box disappears from the cluster column. Orders
  keep flowing. Producers and workers reconnect to the new leaders on their
  own.
* Kafka UI → Brokers: two online.

## Investigate

```bash
make topics
```

`Leader` changed for the partitions broker 2 led, `Isr` lists two brokers,
`Replicas` still lists three: Kafka remembers where the copy should be.

Send more orders. They are acknowledged. With `min.insync.replicas=1` the
leader alone is enough. Set it to 2 and a second broker failure would make
writes fail with `NotEnoughReplicas`, which is the safer choice for payments.

## Fix

```bash
make start-broker N=2
```

Broker 2 catches up from the leaders (watch `Isr` go back to three). It does
not automatically take leadership back right away; Kafka rebalances leaders
on its own after a while (`leader.imbalance.check.interval.seconds`), or run:

```bash
docker exec kafka-1 kafka-leader-election --bootstrap-server localhost:9092 \
  --election-type preferred --all-topic-partitions
```

## Verify

* `make topics`: three brokers in every `Isr`.
* No order lost: count events in `orders` before and after
  (Inspector tiles, or `kafka-run-class kafka.tools.GetOffsetShell`).
* Lag back to 0.

## What replication does and does not promise

* Replication factor 3 means three copies, not three guarantees. A write is
  safe once it is on every **in-sync** replica the leader waits for.
* `acks=all` makes the producer wait for the in-sync set. `acks=1` waits for
  the leader only: a leader that dies before followers copy the record loses
  it. `acks=0` waits for nothing.
* `min.insync.replicas` is the floor. With 1 (this lab's default) the leader
  alone may acknowledge, so a leader crash right after an ack can lose the
  record. With 2, the cluster refuses writes (`NotEnoughReplicas`) rather
  than risk them when only one replica is left. Choose 2 for payments.
* A broker outage is not a quorum loss. The KRaft controller quorum here is
  the same three nodes; losing two of them stops metadata changes
  (leader elections included) even though one broker is still serving.

## Questions to answer

* Why did no data get lost when broker 2 died?
* What would happen with replication factor 1?
* What does `acks=all` mean when the in-sync set is only two brokers?
