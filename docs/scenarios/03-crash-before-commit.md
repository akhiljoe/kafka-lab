# Drill 03: Crash before commit

## Setup

Lab (http://localhost:8000) → On call tab → Incident drills → **Crash before commit**. Generator at 3/s, one
worker that processes 8 orders and then exits without committing.

## Symptom

* Worker pill turns red: `crashed`. Its last log line says
  `simulated crash before commit after 8 orders`.
* Watch tab: the order shows processed but **not** committed. The
  application believes the work is done. Kafka does not.
* Inspector: the group's committed offset stops moving; lag grows.

## Investigate

1. Compare the shop's view and Kafka's view for one of the processed-but-
   uncommitted orders. Note its partition and offset.
2. `make describe-group`: `CURRENT-OFFSET` is below that order's offset.
3. Read the worker code. Normally it commits after every message. In crash
   mode it processes 8 orders, commits none of them, and exits. All 8 sit in
   the window between "work done" and "offset committed". A real worker that
   commits once per batch has exactly this window, the size of the batch.

## Hypothesis

Restarting the worker will re-read every message after the last committed
offset. Some orders will be processed twice.

## Fix and observe

Start a new worker in the same group (delay 0.1, crash after 0). Watch:

* The worker's log shows the same partition@offset pairs the crashed one
  already handled.
* `order-status` gets a second `consumed` and `ready` event for those orders.
  On call tab stats: `ready` can exceed `placed` for a while.

This is **at-least-once** delivery. Kafka's promise is "I will not lose a
message you did not commit". Duplicates are your problem.

Ways to make duplicates harmless:

* **Idempotent processing.** Key the side effect by `order_id` and make the
  second write a no-op (upsert, unique constraint, "already processed" set).
* **Commit before processing** gives at-most-once: no duplicates, but the
  in-flight message is lost on a crash. Right for metrics, wrong for payments.
* **Transactions** (`transactional.id`, `isolation.level=read_committed`)
  make consume-transform-produce exactly-once within Kafka, not into an
  external database.

## Verify

* Lag returns to 0.
* Count `ready` events per `order_id` in the `order-status` topic (Kafka UI
  → topic → messages, filter on the id). Two is expected for the replayed
  orders.

## Questions to answer

* What does a committed offset represent exactly?
* Which messages are replayed after a crash: everything, or only after the last commit?
* Where would you put the idempotency check in the worker?
