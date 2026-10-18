# Drill 05: Zombie worker

## Setup

Lab (http://localhost:8000) → On call tab → Incident drills → **Zombie worker**. Generator at 5/s, three
healthy workers, one partition each. Wait until the inspector shows the group
`STABLE`.

Now press **Kill** (not Stop) on one worker. Kill is `SIGKILL`: the process
dies instantly, no chance to leave the group.

## Symptom

* The killed worker's pill goes red immediately.
* Inspector: for up to 45 seconds the group stays `STABLE` with **three**
  members, and the dead worker still owns its partition. Lag on that one
  partition grows. The other two partitions are fine.
* Then: `STABLE → PREPARING_REBALANCING`, `member worker-x left`,
  `orders[n] worker-x → worker-y`, back to `STABLE`.

## Investigate

1. `make describe-group` during the first 45 s: the dead consumer id still
   appears with lag growing. Kafka does not know it is dead.
2. Why 45 s? `session.timeout.ms` defaults to 45000. The consumer sends a
   heartbeat every `heartbeat.interval.ms` (3000). The group coordinator only
   declares a member dead after a full session timeout without heartbeats.
3. Compare with **Stop** (`SIGTERM`): the worker calls `consumer.close()`,
   sends a LeaveGroup, and the rebalance happens within a second.

## Hypothesis

The delay is the coordinator's failure detector, not a bug. Shortening it
trades faster recovery for more false positives under GC pauses or network
blips.

## Fix

* Lower `session.timeout.ms`. The worker takes a flag for it:

  ```bash
  make worker NAME=fast-fail DELAY=0.1 ARGS="--session-timeout-ms 10000"
  ```

  Re-run the drill with this worker and time the failover.
* Always shut down cleanly: handle `SIGTERM`, call `close()`. Kubernetes sends
  `SIGTERM` then waits `terminationGracePeriodSeconds` before `SIGKILL`.
* Watch out for the other direction too: a worker that takes longer than
  `max.poll.interval.ms` (300000) between polls is also kicked out, with a
  rebalance, even though the process is alive. Long processing per message
  needs a higher value or smaller batches.

## Verify

* After the rebalance, lag on the orphaned partition drains.
* Group change log shows the reassignment.
* With the shorter session timeout, the gap is around 10 s instead of 45 s.

## Questions to answer

* What is the difference between a consumer leaving and a consumer timing out?
* What happens to messages the dead worker consumed but had not committed?
* Why not set `session.timeout.ms` to 1 second?
