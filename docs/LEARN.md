# Learn Kafka with this lab, one number at a time

The dashboards show a lot. You need three numbers. Everything else is built
from them.

| Number | Meaning | Where to see it |
|---|---|---|
| **offset** | position of a message inside one partition. Starts at 0, goes up by 1 per message, never reused. | right edge of a partition bar in the inspector |
| **committed offset** | the position a consumer group has said "I am done up to here". | vertical marker on the bar, and the `Committed` column |
| **lag** | log end offset minus committed offset. How many messages are waiting. | the gap between marker and right edge |

Each lesson below adds one idea. Do them in order. Each takes 5 to 10
minutes. Do not open Grafana or Kafka UI until lesson 8.

Setup for every lesson: Kafka running (`make up`), shop running
(`make shop`), inspector running (`make inspector`). Two UIs only:

* **Lab** at http://localhost:8000. Two tabs: Watch (see the event
  travel) and On call (debug incidents). The left column
  names every moving part with its live state. Read it once.
* **Inspector** at http://localhost:8080. What the broker knows. Starts in
  Beginner view.

First thing, every time the numbers look big: press **Start fresh** in the
Lab header. Offsets go back to 0.

---

## Lesson 0: Watch one event, slowly

Open the **Watch** tab. One worker starts on its own (about 3 s per order).
Pick a customer, click **Send one order**.

Watch the dots: producer → broker → one partition (blue), partition → worker
(blue, worker turns orange for 3 s), worker → order-status → shop-api
(green), then the violet commit. The right panel writes each hop out.
Below the graph, the journey lists each hop with the real time it took.

Send the same customer again. Same partition. Press **Send 3** and watch the
worker take them one at a time, in offset order; the journey of the last one
says it is waiting behind the others.

**Idea:** an event has four moments: produced, consumed, processed,
committed. They happen at different times, in different places. Kafka is
fast; the gap you can see is the consumer's work.

Do this lesson until you can predict what the next box will do before it
does it. Then move on.

---

## Lesson 1: One message, one offset

Use your own first script. In a terminal:

```bash
.venv/bin/python producer.py
```

It prints something like `Delivered to orders : partition 1 : at offset 42`.

Look at the inspector, topic `orders`. Find partition 1. Its right-hand
number went up by one. That number is the **log end offset**: where the
next message will land. Your message is at the offset the script printed,
one less than the log end.

Run the script three more times. Watch which partition each one lands on.
The script sends no key, so Kafka's producer picks partitions itself.

**Idea:** a topic is not one queue. It is several independent logs called
partitions. A message lives in exactly one of them, at one offset, forever
(until retention deletes it).

**Question:** why did the offset on partition 1 not change when a message
went to partition 0?

---

## Lesson 2: The same key always lands in the same place

Open the shop UI at http://localhost:8000. Do not start any worker yet.

In "Place an order", pick customer `lara`, key mode `customer`, press
**Place order** five times. Watch the "Partition @ offset" column.

All five land on the same partition, offsets going up by one each time.

Change customer to `tim`. Place five more. Different partition, probably.
(Maybe the same; with 3 partitions and 8 customers, collisions are normal.)

Now switch key mode to `order_id` and place five. They scatter.

**Idea:** Kafka hashes the key and uses the result to pick the partition.
Same key, same partition, always. That is how Kafka keeps order per
customer while still splitting the topic across machines.

**Question:** which key would you use for bank transfers? What would break
if you used `none`?

---

## Lesson 3: A consumer group remembers where it stopped

Still no worker. Run your own first consumer:

```bash
.venv/bin/python tracker.py
```

It prints every order you placed in lessons 1 and 2, then waits.

Look at the inspector, "Consumer groups". A group called `order-tracker`
appeared, with one member and all three partitions assigned to it. Each
partition row shows `Committed` equal to `Log end` and `Lag 0`.

Stop the consumer with Ctrl-C. In the shop, place three more orders. The
inspector shows `order-tracker` with **no members** but still a row per
partition, and lag 3. Kafka remembers the group's position even though
nobody is connected.

Start `tracker.py` again. It prints only the three new orders. Lag goes
back to 0.

**Idea:** a committed offset is a bookmark Kafka keeps per group per
partition. Restarting a consumer resumes from the bookmark, not from the
beginning. `auto.offset.reset=earliest` only matters the very first time,
when there is no bookmark yet.

**Question:** what would happen if you changed `group.id` in `tracker.py`
to something new and ran it?

---

## Lesson 4: Lag is just subtraction

Stop `tracker.py`. In the shop UI, start **one** worker (default settings:
group `order-processor`, delay 0.2 s). Watch the inspector: the new group
appears, state goes `PREPARING_REBALANCING`, then `STABLE`, and the worker
owns all three partitions.

Press **Burst 20**. Watch one partition bar in the inspector. The right edge
jumps (20 messages arrived, spread across the three bars). The marker
crawls to the right at about 5 per second (0.2 s each). The label under the
marker says `lag 7`, `lag 6`, `lag 5`... then `✓`.

On the Watch tab, the journey of the latest order fills in step by step.
The last step, **committed**, fills when the marker in the inspector passes
that order's offset. Same event, two views.

**Idea:** lag = log end offset − committed offset. Not a health score, not
a percentage. A count of messages between "arrived" and "done".

**Question:** when the worker is idle, is lag 0 or is lag undefined?

---

## Lesson 5: Partitions are the limit of parallelism

One worker running, generator off. Set the traffic generator to 10 orders/s
and start it. Lag climbs: the worker does 5/s.

Start a second worker. Watch the group change log: a rebalance, then
partitions split 2 + 1. Lag growth slows. Start a third: 1 + 1 + 1,
throughput 15/s, lag drains to 0.

Start a fourth. Watch its assignment: nothing. It sits idle. Three
partitions, three active consumers, no more.

**Idea:** within one group, a partition is owned by at most one consumer.
Partition count is the ceiling on how many consumers can work in parallel.

**Question:** why does Kafka not split one partition between two consumers?

---

## Lesson 6: Two groups, one topic, two bookmarks

Keep the three workers. Start a fourth worker but set its group to
`audit`. Watch the inspector: a second group, its own committed offsets,
its own lag, starting from the beginning of the topic (so a large lag at
first, draining fast).

Both groups read every message. Neither affects the other.

**Idea:** a consumer group is one application's view of a topic. Payments
and analytics can both read `orders` without stepping on each other. This
is the difference from a queue, where a message is gone once consumed.

**Question:** how many times is each `orders` message now consumed? How
many times is it stored?

---

## Lesson 7: Stop versus kill

Keep three `order-processor` workers and the generator at 5/s. Stop the
`audit` worker and remove it.

Press **Stop** on one worker. Watch the change log: `member left`,
`PREPARING_REBALANCING`, partitions move, `STABLE`. All within a second or
two.

Press **Kill** on another. Watch the inspector. Nothing happens. The dead
worker still owns its partition. Lag on that partition climbs. Count the
seconds. Around 45 later: `member left`, rebalance.

**Idea:** a consumer proves it is alive by sending heartbeats. Kafka
declares it dead only after `session.timeout.ms` (45 s by default) of
silence. A clean shutdown says goodbye and skips the wait.

**Question:** what happened to the messages the killed worker had
consumed but not yet committed?

---

## Lesson 7b: Three brokers, one dies

Watch tab. Note which broker leads each `orders` partition (the small
"leader B2" text). Then:

```bash
make stop-broker N=2
```

The broker 2 box vanishes. Partitions it led get a new leader within
seconds. "in-sync 2/3" on every partition. Send orders: they still work.
Then `make start-broker N=2` and watch "in-sync 3/3" come back.

**Idea:** a partition is copied to several brokers. One copy is the leader
and takes all reads and writes; the others follow. Losing a broker loses
nothing as long as one in-sync copy survives.

**Question:** what does replication factor 3 cost you, and what does it buy?

---

## Lesson 8: Now look at everything

Turn off Beginner view. Every panel you now see is one of the three
numbers over time or per partition:

* "Messages per second" is the log end offset's rate of change (produced)
  and the committed offset's rate of change (committed).
* "Consumer lag over time" is the subtraction, plotted.
* Grafana's "Kafka Lab Overview" is the same three numbers scraped by
  kafka-exporter, kept for hours instead of minutes.
* Kafka UI shows the same partitions and groups in a generic tool used at
  many companies.
* `make describe-group` prints the same table from the command line:
  `CURRENT-OFFSET` is committed, `LOG-END-OFFSET` is log end, `LAG` is lag.

Open all four, run **Burst 1000**, and find the same event in each.

**Idea:** there is no secret fourth number. Production dashboards are these
three, sliced and plotted.

---

## Then: the incident drills

Only now go to `docs/scenarios/`. Each drill breaks one of the ideas above
on purpose. You already know what normal looks like, so you will see what
is wrong.

Keep a notes file. For each lesson write one sentence in your own words
and the answer to its question. If you cannot write the sentence, redo the
lesson.
