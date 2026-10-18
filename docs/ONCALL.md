# The On call tab

A write-up of what the On call tab is, how it is laid out, what every control
does, and what is real versus modelled. Served by the shop service at
`http://localhost:8000/ops`, embedded as the third tab of the Lab at
`http://localhost:8000`. Source: `services/shop/static/index.html` (page) and
the incident, runbook and facts sections of `services/shop/app.py` (server).

## Purpose

The tab is a debugging game built on a real Kafka cluster. The server breaks
something for real, without saying what. You get an alert, investigate with
the same kinds of evidence an on-call engineer has, declare a root cause, apply
a fix, and the incident is only resolved when live measurements prove the
system recovered and stayed recovered.

Everything measured comes from the brokers (admin API, watermarks, committed
offsets, group descriptions) and from the worker processes the lab manages.
Nothing on the page is a simulation of Kafka. What is modelled is the world
around Kafka: customer traffic, a flaky dependency, a producer deploy, another
team fixing their service. Those are listed under "What is modelled" below.

## Layout, top to bottom

```
┌ Alert bar ───────────────────────────────────────────────────────────────┐
│ 🚨 New incident │ mode ▾ │ practice a cause ▾ │ QUIET │ · · · · · · │ … │
├ Mission panel (only while an incident is open) ──────────────────────────┤
│ ALERT text · SEV · started · topic · group · impact · evidence viewed    │
│ investigation hints (mode dependent)                                     │
│ Declare root cause: six buttons                                          │
│ On-call actions: four buttons (progress shown on the button)            │
│ Recovery checks and hold timer                                           │
├ Scene ───────────────────────────────────────────────────────────────────┤
│ PRODUCERS      TOPIC orders (3 lanes, one per partition)   CONSUMER GROUP │
│ web            partition 0  owner  lag   ▪▪▪▪▪▪            consumer card  │
│ mobile-app     partition 1  owner  lag   ▪▪                consumer card  │
│ partner-api    partition 2  owner  lag                     consumer card  │
├ Charts (left) ──────────────────────┬ System state (right) ──────────────┤
│ Throughput: produced/s, committed/s │ read-only facts, colored by health │
│ Consumer lag                        ├ Runbook ───────────────────────────┤
│                                     │ consumers −/+   seconds/message −/+│
│                                     │ Rolling restart  Add partitions → 6 │
│                                     │ Replay dead letters  Reset offsets  │
├ Sandbox (collapsed drawer) ──────────────────────────────────────────────┤
│ produced/s · failure % · key · burst · break a consumer · topology      │
└──────────────────────────────────────────────────────────────────────────┘
```

The page is designed to fit one screen at a typical laptop width so nothing
important is below the fold except the sandbox, which is intentionally tucked
away.

## 1. Alert bar

| Control | What it does | Server |
|---|---|---|
| 🚨 New incident | Starts an incident with a random cause, different from the previous one. Brings all three brokers up first. Locks the sandbox. | `POST /api/incidents/start {mode}` |
| mode | `guided`: cause and CLI hints shown. `standard`: alert plus investigation hints. `expert`: alert only. | sent with start |
| practice a cause | Starts an incident with a chosen cause, cause revealed in the panel. For a guided first run of each cause. | `POST /api/incidents/start {cause, mode}` |
| badge | `QUIET` (no incident), `INCIDENT BREWING` (fault injected, symptom not yet visible), `🔴 ALERT` (symptom visible), `✓ RESOLVED`. Red siren vignette around the page while in ALERT. | derived from `GET /api/incidents/current` |
| six dots | One per cause. Tick when you have resolved an incident of that cause (stored in the browser). | localStorage |
| Abandon incident | Appears while an incident is open. Drops it and stops everything. | `POST /api/incidents/abandon` then `/api/reset` |
| Stop everything | Stops the producers and every consumer. Keeps topics and offsets. | `POST /api/reset` |
| Stop + clear backlog | Same, then moves the group's committed offsets to the log end so no backlog remains. | `POST /api/reset?skip_backlog=true` |

## 2. Mission panel

Shown only while an incident is open. It changes with the incident's stage.

**Brewing.** "Something is going wrong in production. No page yet." The fault is
already injected; the alarm fires when the symptom crosses a threshold (lag,
retries, a crash, a missing broker, depending on the cause).

**Alert.** The alert line is always the same, on purpose: the order pipeline
health check is failing on `orders`. It watches lag, errors, group membership
and replication and does not say which one tripped, so the symptom has to come
from the evidence. Below it:

- Severity: `SEV1` if lag is above 200 or a broker is missing, `SEV2` above 50,
  else `SEV3`. Recomputed every second.
- Started time, topic, consumer group.
- Impact in plain words: how many orders are waiting and how long they would
  take to clear at the current commit rate.
- Evidence viewed: how many pieces of evidence you opened (consumer logs, the
  Inspector). Counted, never penalised.
- Investigation hints in guided and standard mode. Expert mode shows none.
- In practice runs: the cause and its explanation.

**Declare root cause.** Six buttons, one per cause. A wrong choice flashes the
page red, strikes the button through and is counted. A correct choice turns the
button green, reveals the explanation, and highlights the relevant on-call
action. Diagnosis is a stage, not a gate: you may use any fix before you are
sure, but the incident does not count as resolved until the diagnosis is
correct.

**On-call actions.** All four actions are visible from the start so that the
list itself does not reveal the cause. Each shows its progress on the button
after you click it ("in progress, ~22 s", then "done 3 s ago"). After a correct
diagnosis the relevant action is blue and the others dim.

| Action | What really happens | Models |
|---|---|---|
| Deploy producer fix: key by order_id (~6 s) | After 6 s the shop's producers switch to keying by order id and the hot customer is cleared. | A producer code change going through a deploy. |
| Page the downstream team (~25 s) | After 25 s every running consumer is restarted with failure rate 0. | Another team fixing their service. |
| Restart the group with session.timeout.ms = 10 s | Live members are restarted with `--session-timeout-ms 10000`. | A consumer config change. |
| Start broker 2 | `docker start kafka-2`. | Ops bringing a broker back. |

**Recovery checks.** Before diagnosis only generic conditions are shown
(lag under a threshold, no unexpected duplicates, lag trend flat or
decreasing). After diagnosis the cause-specific checks appear. All checks,
shown or not, must be true for 10 consecutive seconds after the alarm has
fired at least once. A single one-second blip does not restart the hold; two
seconds of a failing check does. The server evaluates this every second on its
own; it does not depend on the browser polling.

**Debrief.** On resolution: an overlay with what happened, the evidence that
confirmed it, misleading clues, alternative fixes, what could have gone wrong,
and your own timeline (every action, wrong guess and piece of evidence with a
timestamp). "Next incident" starts another random one.

## 3. Scene

A live picture of the pipeline, redrawn every second from broker and process
state. Order tokens travel left to right at the real measured rates, capped at
a readable pace.

- **Producers**: the three producer clients (`web`, `mobile-app`,
  `partner-api`) with their recent order counts.
- **Brokers**: three chips above the lanes showing what each broker leads
  according to the cluster metadata. A broker missing from the metadata is
  drawn dim, leading nothing. No "down" label on purpose: the chip is
  evidence, not a verdict. Hidden in expert mode.
- **Topic orders**: one horizontal lane per partition. Each lane shows its
  owner in the consumer group, the leader broker, the in-sync replica count,
  its lag, and one small box per message not yet committed (up to 22 drawn).
  A lane turns red and glows past 20 lag.
- **Consumer group**: one card per consumer process the lab manages.
  Sleeping icon when idle, gear when its partitions have lag, skull when the
  process has crashed. Colored squares show which partitions it owns, with a
  connector from each lane. The seconds-per-message setting is printed in the
  corner. **Click a card to open its log** (last 40 lines). Opening a log is
  recorded as evidence.

## 4. Charts

Both cover the last three minutes and carry a flag for every change you or the
incident made (blue: producer side, orange: consumer side, violet: incident
events). Hover for values and the flags near that moment.

- **Throughput**: produced per second (blue) versus committed per second
  (orange). Orange under blue means lag grows.
- **Consumer lag**: messages produced but not yet committed by
  `order-processor`, summed over partitions.

Rates are deltas of log-end offsets and committed offsets sampled twice a
second and smoothed over two seconds.

## 5. System state

Read-only. The dashboard an on-call engineer reads. Values turn orange or red
when they look wrong, which is a hint, not a verdict.

| Row | Meaning | Red when |
|---|---|---|
| produced / committed | the two rates | committed below 90 % of produced |
| lag total | sum over partitions | above 20 |
| lag per partition | per partition with its owner, "(no owner)" if unassigned | any above 20 |
| key pattern | distinct keys and the share of the most common key in recent messages | top key above 60 % |
| errors | retries and dead letters since the incident started, duplicates if any | any retries or dead letters |
| group order-processor | state and member list as the coordinator reports it | never |
| live processes | consumer processes the lab manages, crashed ones, and any group member it does not manage | crashed, or members ≠ processes |
| session.timeout.ms | of the running consumers | never |
| brokers | brokers reachable and in-sync replicas per partition | fewer than 3, or any partition not fully in sync |
| partitions | count for `orders` | never |

Reading a diagnosis off this panel:

| Cause | Tell-tale |
|---|---|
| Not enough consumer throughput | committed below produced, lag on all partitions, all members busy, no errors |
| Hot partition | top key near 100 %, lag on one partition, one member busy |
| Consumer crashes before committing | a crashed process, partitions with no owner |
| Downstream dependency failing | retries and dead letters climbing, consumers alive |
| Dead consumer still owns a partition | group members more than live processes, one partition lagging |
| Broker down | brokers 2 up, in-sync 2/3 |

## 6. Runbook

Operations an on-call engineer can perform. Always available, during an
incident and in free play. Each is a real operation on the cluster or the
consumer processes.

| Control | What really happens | Used by which incident |
|---|---|---|
| consumers − / + | Scales the group `order-processor` to N processes. New members inherit the failure rate and session timeout of the existing ones (they live in the same world). At most one consumer per partition gets work. | throughput, crash before commit, zombie (as mitigation) |
| seconds per message − / + | Restarts every running consumer with the chosen processing time: 0.05, 0.2, 0.3, 1 or 3 s. Keeps the member count. | throughput, hot partition (brute force) |
| Replay dead letters | Reads `orders.dlq` from where the last replay stopped and produces each order back to `orders`. Replayed messages are not counted as duplicates. | downstream failing, after recovery |
| Reset offsets to latest | Stops the consumers and moves the group's committed offsets to the log end. The backlog is skipped and never processed. Confirmation dialog. | the dangerous shortcut in every incident |

### The four buttons, decided

Two stayed in the runbook, two moved to the sandbox under "Topology".

- **Reset offsets to latest.** Keep. It is the trap. It makes lag zero in a
  second and resolves most incidents on paper, at the cost of never
  processing those orders. The debrief calls it out under "what could have
  gone wrong". The lesson is that recovery and correctness are different
  things, and this button is the clearest way to feel it.
- **Replay dead letters.** Keep. It is the second half of the downstream
  incident. Dead-lettering is only acceptable if someone replays or reviews
  the dead letters later. Without the button the DLQ is a black hole.
- **Add partitions → 6.** Not needed by any current incident. It is a real
  and important operation (the only way to raise the parallelism ceiling)
  with a real trap (one-way, reshuffles keys). It belongs in a future
  "capacity ceiling" incident where three fast consumers are still not
  enough. Moved to the sandbox until that incident exists.
- **Rolling restart.** Not needed by any current incident and nothing in the
  lab misbehaves in a way a restart fixes. It belongs in a future "rebalance
  storm" or "stuck consumer" incident. Moved to the sandbox for now.

## 7. Sandbox

A collapsed drawer. For building a situation yourself in free play. Every
control here changes the world, not the remedy, so it is locked while an
incident is open: the UI dims it, and the server answers HTTP 409 to the
corresponding endpoints.

| Control | What it does |
|---|---|
| produced / s − / + | Traffic generator rate in steps of 5. 0 is off. Orders come from `mobile-app` and `partner-api` at random. |
| failure % − / + | Restarts the consumers with that downstream failure rate. 70 % of failures are transient (retried with backoff), 30 % permanent (dead-lettered). |
| Message key | `customer` (same customer, same partition), `one hot key (lara)`, `order id` (even spread). |
| Burst | +20, +200, +1000 orders at once. |
| Break a consumer | `Stop one` (SIGTERM, clean leave), `Kill one` (SIGKILL, partition stuck until the session timeout), `Add one that crashes` (processes 8 messages, commits none, exits). |
| Topology | `Rolling restart` (SIGTERM and restart each live member in turn), `Add partitions → 6` (`create_partitions` on `orders`, one-way, reshuffles keys; confirmation dialog). Here until an incident needs them. |

## The six causes

Each cause is a scenario on the server (`SCENARIOS` in `app.py`) plus an
incident definition (`CAUSES`) with alarm condition, checks, fixes and debrief
text. Full investigations in `docs/scenarios/`.

| Cause | Fault injected | Alarm fires when | Resolved when |
|---|---|---|---|
| Not enough consumer throughput | 25 msg/s in, one consumer at 0.3 s | lag above 30 | producers still at 20/s or more, commit rate at least 90 % of produce rate, lag under 20 |
| Hot partition | 10 msg/s, all keyed `lara`, three consumers | lag above 20 | producers still at 8/s or more, key spreads, lag under 20 |
| Consumer crashes before committing | 3 msg/s, one consumer that exits after 8 messages without committing | a crash noted and lag above 0 | a managed consumer owns every partition, lag under 5, duplicates observed |
| Downstream dependency failing | 4 msg/s, consumer with 60 % failures | more than 3 retries since start | producers on, at least one dead letter, all consumers at 5 % or less, lag under 20 |
| Dead consumer still owns a partition | 5 msg/s, three consumers, one SIGKILLed after 8 s | a kill noted and group members more than live processes | members equal live processes, lag under 5 |
| Broker down | 5 msg/s, three consumers, `docker stop kafka-2` after 6 s | brokers fewer than 3 or any partition not fully in sync | 3 brokers, every partition in sync 3/3, lag under 20 |

Plus, for every cause except the crash: "no unexpected duplicates". And for
every cause once enough history exists: "lag trend flat or decreasing".

## What is real and what is modelled

Real: topics, partitions, replicas, leaders, in-sync sets, offsets, lag,
consumer group membership and assignment, rebalances, session timeouts,
crashes, SIGKILL, broker stop and start, dead letters, replays, partition
counts, every chart and every number in System state.

Modelled: the customers (a traffic generator), the message key choice (a
switch in the shop's producers), the failing dependency (a failure rate inside
the worker's `do_work`), the producer deploy (a 6 s timer then the switch),
the downstream team (a 25 s timer then a consumer restart at 0 % failure).

## Known limitations

- One consumer process is one sequential lane: roughly `1 / seconds per
  message` throughput. Real consumers can batch or process concurrently.
- Incidents live in the shop's memory. Restarting the shop service drops an
  open incident. Resolved-cause ticks live in the browser.
- The worker's crash leaves the group cleanly in the crash incident (it
  exits through `finally`), so partitions become unowned at once rather than
  after a session timeout. The zombie incident covers the other case.
- Severity thresholds are fixed numbers chosen for the lab's traffic levels.

