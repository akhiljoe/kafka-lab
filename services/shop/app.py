"""Shop service: the product that uses Kafka.

* Places orders (manual or generated traffic) on the `orders` topic.
* Listens to `order-status` so the UI can show what the workers did.
* Checks committed offsets so "committed" is a fact from Kafka, not a guess.
* Spawns and stops processor workers so you can experiment from the browser.

    uvicorn services.shop.app:app --port 8000 --reload
"""
import json
import os
import shlex
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel

from services.common import BOOTSTRAP, CUSTOMERS, DLQ_TOPIC, MENU, ORDERS_TOPIC, PARTITIONS, PRODUCERS, REPLICATION, RESTAURANTS, STATUS_TOPIC

STATIC = Path(__file__).parent / "static"
MAX_ORDERS = 400
WORKER_LOG_LINES = 60

lock = threading.Lock()

# Every knob change lands here so the Experiment tab can draw it on the chart.
timeline = deque(maxlen=120)


def note(kind, text):
    timeline.appendleft({"at": time.time(), "kind": kind, "text": text})


# --------------------------------------------------------------------------- orders
class OrderStore:
    """In-memory view of orders and their lifecycle. Newest first when listed."""

    def __init__(self):
        self.orders = {}
        self.order_ids = deque(maxlen=MAX_ORDERS)
        self.counters = {"placed": 0, "produced": 0, "consumed": 0, "ready": 0, "dead_lettered": 0, "retries": 0}
        self.latencies = deque(maxlen=200)

    def add(self, order):
        with lock:
            if len(self.order_ids) == self.order_ids.maxlen:
                self.orders.pop(self.order_ids[0], None)
            self.order_ids.append(order["order_id"])
            self.orders[order["order_id"]] = order
            self.counters["placed"] += 1

    def mark_produced(self, order_id, partition, offset):
        with lock:
            o = self.orders.get(order_id)
            if o:
                o["produced"] = {"partition": partition, "offset": offset, "at": time.time()}
                self.counters["produced"] += 1

    def apply_event(self, ev, status_partition=None):
        with lock:
            o = self.orders.get(ev["order_id"])
            if not o:
                return
            status = ev["status"]
            if ev.get("group") and ev["group"] != "order-processor":
                # Another consumer group read the same event. Record it, but it does not
                # drive the order's lifecycle: only order-processor does the real work.
                o.setdefault("readers", {})[ev["group"]] = {"worker": ev.get("worker"), "status": status, "at": ev["at"]}
                return
            o["worker"] = ev.get("worker")
            o["group"] = ev.get("group")
            o["status_partition"] = status_partition
            if status == "consumed":
                o["consumed"] = {"at": ev["at"]}
                self.counters["consumed"] += 1
            elif status == "retrying":
                o["retries"] = ev.get("attempt", 0)
                o["last_error"] = ev.get("reason")
                self.counters["retries"] += 1
            elif status in ("ready", "dead_lettered"):
                o["processed"] = {"status": status, "reason": ev.get("reason"), "at": ev["at"]}
                self.counters[status] += 1
                self.latencies.append(ev["at"] - o["created_at"])

    def mark_committed(self, group, partition, committed_offset):
        with lock:
            for o in self.orders.values():
                p = o.get("produced")
                if p and o.get("group") == group and p["partition"] == partition and p["offset"] < committed_offset:
                    o.setdefault("committed", {"at": time.time()})

    def clear(self):
        with lock:
            self.orders.clear()
            self.order_ids.clear()
            self.latencies.clear()
            for k in self.counters:
                self.counters[k] = 0

    def snapshot(self, limit):
        with lock:
            ids = list(self.order_ids)[-limit:]
            orders = [self.orders[i] for i in reversed(ids)]
            lat = sorted(self.latencies)
            p50 = lat[len(lat) // 2] if lat else None
            p95 = lat[int(len(lat) * 0.95)] if lat else None
            in_flight = sum(1 for o in self.orders.values() if "processed" not in o)
            return orders, dict(self.counters, in_flight=in_flight, latency_p50=p50, latency_p95=p95)


store = OrderStore()


# --------------------------------------------------------------------------- kafka
def make_producer(client_id):
    return Producer({
        "bootstrap.servers": BOOTSTRAP, "client.id": client_id,
        "partitioner": "murmur2_random",  # same key -> partition mapping as the Java clients
        "linger.ms": 5,              # wait up to 5 ms to fill a batch: far fewer requests at high rates
        "compression.type": "lz4",
        "queue.buffering.max.messages": 200000,
    })


# One producer client per order source, so the broker (and the graph) see three distinct producers.
producers = {source: make_producer(source) for source in PRODUCERS}
producer = producers["web"]   # the Watch tab's Send button and manual API calls
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})


def on_delivery(err, msg):
    order_id = msg.headers() and dict(msg.headers()).get("order_id")
    order_id = order_id.decode() if order_id else json.loads(msg.value())["order_id"]
    if err:
        print(f"delivery failed for {order_id}: {err}", flush=True)
        return
    store.mark_produced(order_id, msg.partition(), msg.offset())


def place_order(customer, item, quantity, key_mode, source="web"):
    order = {
        "order_id": str(uuid.uuid4()),
        "customer": customer,
        "item": item,
        "quantity": quantity,
        "total": round(MENU[item] * quantity, 2),
        "created_at": time.time(),
        "key_mode": key_mode,
        "source": source,
    }
    key = {"order_id": order["order_id"], "customer": customer, "none": None}[key_mode]
    order["key"] = key
    store.add(order)
    prod = producers.get(source, producer)
    prod.produce(
        ORDERS_TOPIC,
        key=key,
        value=json.dumps(order).encode(),
        headers={"order_id": order["order_id"]},
        callback=on_delivery,
    )
    prod.poll(0)
    return order


SIMULATED_SOURCES = [src for src in PRODUCERS if src != "web"]


def random_order(key_mode, source=None):
    return place_order(random.choice(CUSTOMERS), random.choice(list(MENU)), random.randint(1, 5), key_mode,
                       source or random.choice(SIMULATED_SOURCES))


def poll_all(timeout=0):
    for prod in producers.values():
        prod.poll(timeout)


class Generator(threading.Thread):
    """Produces random orders at a configurable rate."""

    def __init__(self):
        super().__init__(daemon=True)
        self.running = False
        self.rate = 2.0
        self.key_mode = "customer"
        self.hot_customer = None   # when set, every generated order comes from this one customer

    TICK = 0.05  # seconds; orders are produced in batches per tick so high rates do not depend on sleep precision

    def run(self):
        carry = 0.0
        while True:
            if not self.running or self.rate <= 0:
                poll_all(0)
                time.sleep(0.2)
                carry = 0.0
                continue
            started = time.time()
            carry += self.rate * self.TICK
            n, carry = int(carry), carry - int(carry)
            for _ in range(n):
                if self.hot_customer:
                    place_order(self.hot_customer, random.choice(list(MENU)), random.randint(1, 5), self.key_mode,
                                random.choice(SIMULATED_SOURCES))
                else:
                    random_order(self.key_mode)
            poll_all(0)
            time.sleep(max(0.0, self.TICK - (time.time() - started)))


generator = Generator()


class StatusListener(threading.Thread):
    """Tails `order-status` and feeds lifecycle events into the store."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop = threading.Event()

    def run(self):
        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "shop-ui",
            "client.id": "shop-api",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            # A UI should recover fast after a restart, so a short session timeout.
            # The workers keep Kafka's 45 s default on purpose (see drill 05).
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 2000,
        })
        consumer.subscribe([STATUS_TOPIC])
        try:
            while not self.stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print("status consumer error:", msg.error(), flush=True)
                    continue
                try:
                    store.apply_event(json.loads(msg.value()), status_partition=msg.partition())
                except Exception as e:  # malformed event should not kill the listener
                    print("bad status event:", e, flush=True)
        finally:
            consumer.close()  # leave the group so a restart does not wait for the session timeout


class KafkaWatcher(threading.Thread):
    """Twice a second asks Kafka what it knows: committed offsets per group,
    log end offsets per partition, and who owns which partition. Marks orders
    as committed from that, and feeds the Flow page."""

    def __init__(self):
        super().__init__(daemon=True)
        self.view = {"partitions": [], "groups": [], "at": 0}
        self.history = deque(maxlen=400)   # (t, orders log end total, order-processor committed total): ~3 min at 2 Hz
        self.phistory = {}                 # partition id -> deque of (t, high, committed or None)
        self.group_events = deque(maxlen=300)
        self.prev_group = None             # (state, members tuple, owner map) of order-processor
        self.wm_consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"shop-wm-{uuid.uuid4().hex[:6]}",
                                     "enable.auto.commit": False})

    def diff_group(self, group_views, op, now):
        """Membership and assignment changes of order-processor, as the coordinator reports them."""
        g = next((x for x in group_views if x["id"] == "order-processor"), None)
        state = g["state"] if g else "NONE"
        members = tuple(sorted(m["client_id"] for m in g["members"])) if g else ()
        owners = {pid: p["owner"].get("order-processor") for pid, p in op.items()}
        if self.prev_group is not None:
            ps, pm, po = self.prev_group
            if state != ps:
                self.group_events.append({"at": now, "kind": "state", "text": f"group {ps.lower()} → {state.lower()}"})
            for m in sorted(set(members) - set(pm)):
                self.group_events.append({"at": now, "kind": "join", "text": f"{m} joined"})
            for m in sorted(set(pm) - set(members)):
                self.group_events.append({"at": now, "kind": "leave", "text": f"{m} left the group"})
            for pid, o in owners.items():
                if po.get(pid) != o:
                    self.group_events.append({"at": now, "kind": "assign", "text": f"partition {pid}: {po.get(pid) or 'nobody'} → {o or 'nobody'}"})
        self.prev_group = (state, members, owners)

    def partition_series(self):
        out = {}
        for pid, h in self.phistory.items():
            pts = list(h)
            out[pid] = [{"t": t, "lag": max(0, hi - c) if c is not None else None, "high": hi} for t, hi, c in pts]
        return out

    def series(self):
        """Per-second rates and lag, smoothed over a 2 s window, for the charts."""
        pts = list(self.history)
        out = []
        for i in range(4, len(pts)):
            t, h, c = pts[i]
            t0, h0, c0 = pts[i - 4]
            dt = t - t0
            if dt <= 0:
                continue
            out.append({"t": t, "in": max(0.0, (h - h0) / dt), "processed": max(0.0, (c - c0) / dt), "lag": max(0, h - c)})
        return out

    def reset_admin(self):
        global admin
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        self.wm_consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"shop-wm-{uuid.uuid4().hex[:6]}",
                                     "enable.auto.commit": False})

    def watermarks(self, partition):
        return self.wm_consumer.get_watermark_offsets(TopicPartition(ORDERS_TOPIC, partition), timeout=5, cached=False)

    def run(self):
        while True:
            time.sleep(0.5)
            try:
                self.collect()
            except Exception as e:
                # Stale metadata after a topic was recreated or a broker moved: start over
                # with fresh clients rather than reporting zeros forever.
                print("kafka watcher:", e, "-> recreating clients", flush=True)
                self.reset_admin()
                time.sleep(1.0)

    def collect(self):
        with lock:
            groups = {o.get("group") for o in store.orders.values() if o.get("group")}
        groups |= {w.group for w in workers.values()} | {"order-processor", "shop-ui"}
        md = admin.list_topics(timeout=5)
        brokers = [{"id": b.id, "host": b.host, "port": b.port} for b in sorted(md.brokers.values(), key=lambda b: b.id)]
        topics = {}
        for name in (ORDERS_TOPIC, STATUS_TOPIC):
            tm = md.topics.get(name)
            if tm is None:
                continue
            parts = {}
            for pid, pm in sorted(tm.partitions.items()):
                low, high = self.wm_consumer.get_watermark_offsets(TopicPartition(name, pid), timeout=5, cached=False)
                parts[pid] = {"id": pid, "topic": name, "leader": pm.leader, "replicas": list(pm.replicas), "isr": list(pm.isrs),
                              "low": low, "high": high, "committed": {}, "owner": {}}
            topics[name] = parts

        descs = admin.describe_consumer_groups(sorted(groups), request_timeout=5)
        group_views = []
        failures = []
        for g in sorted(groups):
            try:
                d = descs[g].result(timeout=5)
            except Exception as e:
                err = getattr(e.args[0], "code", lambda: None)() if e.args else None
                if err != KafkaError.GROUP_ID_NOT_FOUND:   # "not found" is normal before the first worker
                    failures.append(f"{g}: {e}")
                continue
            members = []
            for m in d.members:
                assigned_all = sorted((tp.topic, tp.partition) for tp in (m.assignment.topic_partitions if m.assignment else []))
                for topic, pid in assigned_all:
                    if topic in topics and pid in topics[topic]:
                        topics[topic][pid]["owner"][g] = m.client_id
                members.append({"client_id": m.client_id,
                                "assigned": [pid for t, pid in assigned_all if t == ORDERS_TOPIC],
                                "assigned_all": [{"topic": t, "partition": pid} for t, pid in assigned_all]})
            group_views.append({"id": g, "state": str(d.state).split(".")[-1], "members": members})
            try:
                res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g)], require_stable=False)[g].result(timeout=5)
            except Exception:
                continue
            for tp in res.topic_partitions:
                if tp.offset >= 0 and tp.topic in topics and tp.partition in topics[tp.topic]:
                    topics[tp.topic][tp.partition]["committed"][g] = tp.offset
                    if tp.topic == ORDERS_TOPIC:
                        store.mark_committed(g, tp.partition, tp.offset)
        if failures and len(failures) == len(groups):
            # Every describe failed although the groups exist: the admin client's cached
            # coordinator is stale (happens after brokers restart). Replace the client.
            print("kafka watcher: all group lookups failed, recreating admin client:", failures[0], flush=True)
            self.reset_admin()
        op = topics.get(ORDERS_TOPIC, {})
        now = time.time()
        for pid, p in op.items():
            self.phistory.setdefault(pid, deque(maxlen=400)).append((now, p["high"], p["committed"].get("order-processor")))
        self.diff_group(group_views, op, now)
        high_total = sum(p["high"] for p in op.values())
        committed_total = sum(p["committed"].get("order-processor", 0) for p in op.values())
        self.history.append((time.time(), high_total, committed_total))
        self.view = {
            "partitions": list(topics.get(ORDERS_TOPIC, {}).values()),   # kept for the Flow page
            "topics": {name: list(parts.values()) for name, parts in topics.items()},
            "brokers": brokers, "groups": group_views, "at": time.time(),
        }


# --------------------------------------------------------------------------- workers
class WorkerProc:
    def __init__(self, name, group, delay, fail_rate, crash_after, session_timeout_ms=45000):
        self.id = uuid.uuid4().hex[:8]
        self.name, self.group, self.delay, self.fail_rate, self.crash_after = name, group, delay, fail_rate, crash_after
        self.session_timeout_ms = session_timeout_ms
        self.started_at = time.time()
        self.log = deque(maxlen=WORKER_LOG_LINES)
        cmd = [
            sys.executable, "-u", "-m", "services.processor.worker",
            "--name", name, "--group", group, "--delay", str(delay), "--fail-rate", str(fail_rate),
            "--crash-after", str(crash_after), "--parent-pid", str(os.getpid()),
            "--session-timeout-ms", str(session_timeout_ms),
        ]
        self.proc = subprocess.Popen(
            cmd, cwd=Path(__file__).resolve().parents[2],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            env={**os.environ, "KAFKA_BOOTSTRAP": BOOTSTRAP},
        )
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.proc.stdout:
            self.log.append(line.rstrip())

    def state(self):
        rc = self.proc.poll()
        if rc is None:
            return "running"
        st = "crashed" if rc else "stopped"
        if st == "crashed" and not getattr(self, "_noted", False):
            self._noted = True
            note("workers", f"CRASH {self.name} exited with code {rc} ({'after ' + str(self.crash_after) + ' uncommitted messages' if self.crash_after else 'killed'})")
        return st

    def to_dict(self):
        return {
            "id": self.id, "name": self.name, "group": self.group, "delay": self.delay,
            "fail_rate": self.fail_rate, "crash_after": self.crash_after, "pid": self.proc.pid, "session_timeout_ms": self.session_timeout_ms,
            "state": self.state(), "started_at": self.started_at, "log": list(self.log),
        }


workers = {}


# --------------------------------------------------------------------------- brokers (docker)
def docker_broker(broker_id: int, action: str):
    """Stop or start kafka-<n> with the docker CLI. Local lab only."""
    name = f"kafka-{broker_id}"
    r = subprocess.run(["docker", action, name], capture_output=True, text=True, timeout=60)
    ok = r.returncode == 0
    note("drill", f"{action} {name}" + ("" if ok else f" FAILED: {r.stderr.strip()[:80]}"))
    return {"ok": ok, "broker": name, "action": action, "error": r.stderr.strip() if not ok else None}


# --------------------------------------------------------------------------- scenarios
def _untracked_worker_pids():
    """Worker processes that are children of this shop but missing from `workers`
    (a race between a wipe and a UI auto-start can leave one behind)."""
    r = subprocess.run(["pgrep", "-P", str(os.getpid()), "-f", "services.processor.worker"], capture_output=True, text=True)
    known = {w.proc.pid for w in workers.values()}
    return [int(x) for x in r.stdout.split() if x.isdigit() and int(x) not in known]


def _stop_all_workers():
    for w in list(workers.values()):
        if w.state() == "running":
            w.proc.send_signal(signal.SIGTERM)
    for pid in _untracked_worker_pids():
        try:
            os.kill(pid, signal.SIGTERM)
            note("workers", f"stopped an untracked consumer process (pid {pid})")
        except ProcessLookupError:
            pass


def _spawn(name, **kw):
    w = WorkerProc(name, kw.get("group", "order-processor"), kw.get("delay", 0.2), kw.get("fail_rate", 0.0), kw.get("crash_after", 0))
    workers[w.id] = w
    return w


def scenario_lag_storm():
    _stop_all_workers()
    generator.hot_customer = None
    generator.key_mode = "customer"
    generator.rate = 25
    generator.running = True
    _spawn("slow-worker", delay=0.3)


def scenario_hot_partition():
    _stop_all_workers()
    generator.key_mode = "customer"
    generator.hot_customer = "lara"
    generator.rate = 10
    generator.running = True
    for n in ("worker-a", "worker-b", "worker-c"):
        _spawn(n, delay=0.2)


def scenario_crash_loop():
    _stop_all_workers()
    generator.hot_customer = None
    generator.rate = 3
    generator.running = True
    _spawn("fragile-worker", delay=0.1, crash_after=8)


def scenario_poison_pill():
    _stop_all_workers()
    generator.hot_customer = None
    generator.rate = 4
    generator.running = True
    _spawn("worker-a", delay=0.1, fail_rate=0.6)


def scenario_broker_down():
    _stop_all_workers()
    generator.hot_customer = None
    generator.key_mode = "customer"
    generator.rate = 5
    generator.running = True
    for n in ("worker-a", "worker-b", "worker-c"):
        _spawn(n, delay=0.1)
    threading.Thread(target=lambda: (time.sleep(6), docker_broker(2, "stop")), daemon=True).start()


def scenario_zombie():
    _stop_all_workers()
    generator.hot_customer = None
    generator.rate = 5
    generator.running = True
    _spawn("worker-a", delay=0.1)
    _spawn("worker-b", delay=0.1)
    victim = _spawn("worker-c", delay=0.1)

    def kill_later():
        time.sleep(8)
        if victim.state() == "running":
            victim.proc.kill()
            note("workers", f"KILL {victim.name} (process died, no LeaveGroup)")
    threading.Thread(target=kill_later, daemon=True).start()


SCENARIOS = {
    "lag-storm": {
        "title": "Lag storm", "doc": "docs/scenarios/01-lag-storm.md", "apply": scenario_lag_storm,
        "summary": "Traffic at 25/s, one worker that needs 0.3 s per order. Lag climbs.",
        "look": "Inspector: lag growing on all three partitions, one owner for all of them. Here: In flight and latency climbing.",
        "fix": "Workers card: start two more workers in group order-processor and watch lag slow down. Start a fourth: it gets no partition. "
               "Then start workers with delay 0.05 instead of 0.3 and watch lag drain. Real fix: cheaper processing, more partitions and consumers, or less input.",
    },
    "hot-partition": {
        "title": "Hot partition", "doc": "docs/scenarios/02-hot-partition.md", "apply": scenario_hot_partition,
        "summary": "Three workers, but every order comes from one customer. Two workers sit idle.",
        "look": "Inspector: one partition grows and lags, the other two are flat. Live messages all have the same key.",
        "fix": "Send orders card: set the key to order_id and clear the hot customer. All three partitions fill, all three workers work. "
               "Real fix: choose a key that spreads load while keeping the ordering you actually need.",
    },
    "crash-loop": {
        "title": "Crash before commit", "doc": "docs/scenarios/03-crash-before-commit.md", "apply": scenario_crash_loop,
        "summary": "A worker dies after 8 orders without committing any of them.",
        "look": "Orders show processed but never committed. Worker pill turns red. Inspector lag stops moving.",
        "fix": "Workers card: start a new worker in the same group. It re-reads from the last committed offset and processes the same 8 orders again. "
               "That is at-least-once delivery. Real fix is in code: make processing idempotent, keyed on order_id, so the second time is a no-op.",
    },
    "poison-pill": {
        "title": "Poison messages", "doc": "docs/scenarios/04-poison-messages.md", "apply": scenario_poison_pill,
        "summary": "60% of orders fail. Retries, backoff, and the dead-letter topic fill up.",
        "look": "Retries and Dead-lettered tiles climb. Inspector topic orders.dlq grows. Worker log shows retry 1, 2, 3 with backoff.",
        "fix": "Workers card: stop this worker, start one with fail rate 0.05. Then look at orders.dlq in the Inspector: every dead letter carries the reason and source offset. "
               "Real fix: repair the downstream, then replay the dead-letter topic.",
    },
    "broker-down": {
        "title": "Broker down", "doc": "docs/scenarios/06-broker-down.md", "apply": scenario_broker_down,
        "summary": "Traffic at 5/s, three workers. Six seconds in, broker 2 dies.",
        "look": "Inspector: partitions led by broker 2 get a new leader within seconds; in-sync drops to 2/3 everywhere. Orders keep flowing.",
        "fix": "Bring broker 2 back (button below, or `make start-broker N=2`). Watch in-sync return to 3/3. "
               "Real fix: replication factor 3 and min.insync.replicas=2 so one dead broker never loses data.",
    },
    "zombie": {
        "title": "Zombie worker", "doc": "docs/scenarios/05-zombie-worker.md", "apply": scenario_zombie,
        "summary": "Three healthy workers. Now press Kill (not Stop) on one of them.",
        "look": "Inspector: the dead worker still owns its partition for about 45 s and lag grows there. Then a rebalance moves it.",
        "fix": "Nothing to click: wait for session.timeout.ms (45 s). To make Kafka notice faster, start a worker from a terminal with "
               "`make worker ARGS=\"--session-timeout-ms 10000\"` and kill that one. Real fix: shorter session timeout plus clean shutdown on SIGTERM.",
    },
}


# --------------------------------------------------------------------------- api
app = FastAPI(title="Kafka Lab - Shop")


@app.middleware("http")
async def no_cache(request, call_next):
    """The UI files change often while learning. Never let the browser keep an old copy."""
    if request.method == "OPTIONS":
        response = PlainTextResponse("")
    else:
        response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    # the Inspector (port 8080) reports "evidence viewed" to the incident journal
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:8080"
    response.headers["Access-Control-Allow-Headers"] = "content-type"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return response


status_listener = StatusListener()
kafka_watcher = KafkaWatcher()


@app.on_event("startup")
def startup():
    generator.start()
    status_listener.start()
    kafka_watcher.start()


@app.on_event("shutdown")
def shutdown():
    for w in workers.values():
        if w.state() == "running":
            w.proc.send_signal(signal.SIGTERM)
    status_listener.stop.set()
    status_listener.join(timeout=5)
    for prod in producers.values():
        prod.flush(3)


@app.get("/")
def lab():
    """One page, three tabs: Order, Watch, Experiment. The primer lives in its left column."""
    return FileResponse(STATIC / "lab.html")


@app.get("/ops")
def control_panel():
    return FileResponse(STATIC / "index.html")


@app.get("/graph")
def graph_page():
    """Live topology: producer, broker, partitions, consumer groups. Events animate along the edges."""
    return FileResponse(STATIC / "graph.html")


@app.get("/api/flow")
def flow_state(limit: int = 40):
    orders, stats = store.snapshot(limit)
    return {"now": time.time(), "orders": orders, "kafka": kafka_watcher.view, "stats": stats,
            "workers": [{k: v for k, v in w.to_dict().items() if k != "log"} for w in workers.values()],
            "customers": CUSTOMERS, "menu": MENU, "producers": PRODUCERS}


class GroupIn(BaseModel):
    group: str
    members: int = 1
    delay: float | None = None


@app.post("/api/flow/group")
def flow_group(body: GroupIn):
    """Scale a consumer group to N members for the Watch tab.

    Members are named <group>-1, <group>-2, ... and are plain workers. For
    order-processor the flow-worker (speed buttons) counts as member one; extra
    members copy its delay so they process at the same speed.
    """
    group = body.group
    want = max(0, min(body.members, 6))
    mine = sorted((w for w in workers.values() if w.group == group and w.state() == "running"), key=lambda w: w.started_at)
    delay = body.delay
    if delay is None:
        delay = mine[0].delay if mine else (0.05 if group != "order-processor" else 3.0)
    # New members live in the same world as the existing ones: same dependency failure
    # rate, same session timeout. Scaling out never conjures healthier consumers.
    fail = mine[0].fail_rate if mine else 0.0
    session = mine[0].session_timeout_ms if mine else 45000
    while len(mine) > want:
        w = mine.pop()  # newest first; the flow-worker is the oldest so it goes last
        w.proc.send_signal(signal.SIGTERM)
    if len(mine) != want:
        note("workers", f"{group}: {len(mine)} → {want} members")
    taken = {w.name for w in mine}
    n = 1
    while len(mine) < want:
        name = f"{group}-{n}"
        n += 1
        if name in taken:
            continue
        w = WorkerProc(name, group, delay, fail, 0, session_timeout_ms=session)
        workers[w.id] = w
        mine.append(w)
    # drop cleanly stopped entries so the list does not grow forever; crashed ones are evidence, keep them
    for wid, w in list(workers.items()):
        if w.group == group and w.state() == "stopped":
            workers.pop(wid, None)
    return {"group": group, "members": [w.name for w in mine]}


class FlowWorkerIn(BaseModel):
    delay: float = 3.0
    fail_rate: float = 0.0


@app.post("/api/flow/worker")
def flow_worker(body: FlowWorkerIn):
    """Set the speed of the order-processor group: every running member is restarted
    with the new delay, keeping the member count. With no members, one is started."""
    running = sorted((w for w in workers.values() if w.group == "order-processor" and w.state() == "running"),
                     key=lambda w: w.started_at)
    names = [w.name for w in running] or ["flow-worker"]
    for w in running:
        w.proc.send_signal(signal.SIGTERM)
    for w in running:
        try:
            w.proc.wait(timeout=10)
        except Exception:
            w.proc.kill()
        workers.pop(w.id, None)
    started = []
    for name in names:
        w = WorkerProc(name, "order-processor", body.delay, body.fail_rate, 0)
        workers[w.id] = w
        started.append(w.name)
    note("workers", f"order-processor restarted: {body.delay:g} s per order, fail {body.fail_rate:g}")
    return {"delay": body.delay, "members": started}


@app.post("/api/flow/worker/stop")
def flow_worker_stop():
    """'no worker' in the dropdown: stop every order-processor member."""
    for w in list(workers.values()):
        if w.group == "order-processor" and w.state() == "running":
            w.proc.send_signal(signal.SIGTERM)
    return {"ok": True}


@app.get("/api/customer/{customer}")
def customer_orders(customer: str):
    with lock:
        mine = [store.orders[i] for i in reversed(store.order_ids) if store.orders[i]["customer"] == customer]
    return {"now": time.time(), "customer": customer, "orders": mine[:30], "menu": MENU,
            "restaurants": RESTAURANTS, "customers": CUSTOMERS}


@app.get("/api/state")
def state(limit: int = 60):
    orders, stats = store.snapshot(limit)
    return {
        "now": time.time(),
        "orders": orders,
        "stats": stats,
        "generator": {"running": generator.running, "rate": generator.rate, "key_mode": generator.key_mode,
                      "hot_customer": generator.hot_customer},
        "scenarios": [{"id": k, **{kk: vv for kk, vv in v.items() if kk != "apply"}} for k, v in SCENARIOS.items()],
        "workers": [w.to_dict() for w in workers.values()],
        "series": kafka_watcher.series(),
        "timeline": list(timeline),
        "lag": sum(max(0, p["high"] - p["committed"]["order-processor"]) for p in kafka_watcher.view.get("partitions", []) if "order-processor" in p["committed"]),
        "kafka": {
            "brokers": kafka_watcher.view.get("brokers", []),
            "partitions": [{k: v for k, v in p.items() if k in ("id", "leader", "replicas", "isr", "high", "owner", "committed")} for p in kafka_watcher.view.get("partitions", [])],
            "groups": [{"id": g["id"], "state": g["state"], "members": [m["client_id"] for m in g["members"]]} for g in kafka_watcher.view.get("groups", [])],
        },
        "menu": MENU,
        "customers": CUSTOMERS,
        "producers": PRODUCERS,
    }


class OrderIn(BaseModel):
    customer: str
    item: str
    quantity: int = 1
    key_mode: str = "customer"


@app.post("/api/orders")
def create_order(body: OrderIn):
    if body.item not in MENU:
        raise HTTPException(400, "unknown item")
    if body.key_mode not in ("order_id", "customer", "none"):
        raise HTTPException(400, "key_mode must be order_id, customer or none")
    return place_order(body.customer, body.item, max(1, body.quantity), body.key_mode)


@app.post("/api/orders/burst")
def burst(count: int = 20, key_mode: str = "customer"):
    _fault_lock()
    ids = [random_order(key_mode)["order_id"] for _ in range(min(count, 20000))]
    poll_all(0)
    return {"count": len(ids)}


class GeneratorIn(BaseModel):
    running: bool | None = None
    rate: float | None = None
    key_mode: str | None = None
    hot_customer: str | None = None
    clear_hot_customer: bool = False


def _fault_lock():
    """Fault injection is the incident's job while one is open. Remediation endpoints stay open."""
    if incident["id"] and not incident["resolved_at"]:
        raise HTTPException(409, "locked during an incident: this changes the situation, not the remedy")


@app.post("/api/generator")
def set_generator(body: GeneratorIn):
    _fault_lock()
    if body.running is not None and body.running != generator.running:
        generator.running = body.running
        note("input", f"traffic {'started' if body.running else 'stopped'} at {generator.rate:g}/s")
    if body.rate is not None and body.rate != generator.rate:
        generator.rate = max(0.0, min(body.rate, 5000.0))
        note("input", f"rate → {generator.rate:g} orders/s")
    if body.key_mode in ("order_id", "customer", "none") and body.key_mode != generator.key_mode:
        generator.key_mode = body.key_mode
        note("input", f"key → {body.key_mode}")
    if body.hot_customer in CUSTOMERS and body.hot_customer != generator.hot_customer:
        generator.hot_customer = body.hot_customer
        note("input", f"all orders from {body.hot_customer}")
    if body.clear_hot_customer and generator.hot_customer:
        generator.hot_customer = None
        note("input", "customers spread again")
    return {"running": generator.running, "rate": generator.rate, "key_mode": generator.key_mode}


class WorkerIn(BaseModel):
    name: str | None = None
    group: str = "order-processor"
    delay: float = 0.2
    fail_rate: float = 0.05
    crash_after: int = 0


@app.post("/api/workers")
def start_worker(body: WorkerIn):
    if body.crash_after or body.fail_rate > 0.05:
        _fault_lock()
    name = body.name or f"worker-{len(workers) + 1}"
    w = WorkerProc(name, body.group, body.delay, body.fail_rate, body.crash_after)
    workers[w.id] = w
    note("workers", f"+ {name} ({body.group}, {body.delay:g} s, fail {body.fail_rate:g}{', crash after ' + str(body.crash_after) if body.crash_after else ''})")
    return w.to_dict()


@app.post("/api/workers/{worker_id}/stop")
def stop_worker(worker_id: str):
    w = workers.get(worker_id)
    if not w:
        raise HTTPException(404)
    if w.state() == "running":
        w.proc.send_signal(signal.SIGTERM)  # graceful: leaves the group, commits nothing extra
        note("workers", f"stop {w.name} (clean leave)")
    return w.to_dict()


@app.post("/api/workers/{worker_id}/kill")
def kill_worker(worker_id: str):
    _fault_lock()
    w = workers.get(worker_id)
    if not w:
        raise HTTPException(404)
    if w.state() == "running":
        w.proc.kill()  # abrupt: the group only notices after session.timeout.ms
        note("workers", f"KILL {w.name} (no leave, Kafka notices after 45 s)")
    return w.to_dict()


@app.delete("/api/workers/{worker_id}")
def remove_worker(worker_id: str):
    w = workers.pop(worker_id, None)
    if w and w.state() == "running":
        w.proc.kill()
    return {"ok": True}


@app.get("/metrics")
def metrics():
    """Prometheus text format, hand-rolled to avoid another dependency."""
    from fastapi.responses import PlainTextResponse
    _, st = store.snapshot(1)
    lines = [
        "# TYPE shop_orders_total counter",
        f'shop_orders_total{{outcome="placed"}} {st["placed"]}',
        f'shop_orders_total{{outcome="ready"}} {st["ready"]}',
        f'shop_orders_total{{outcome="dead_lettered"}} {st["dead_lettered"]}',
        "# TYPE shop_retries_total counter",
        f"shop_retries_total {st['retries']}",
        "# TYPE shop_orders_in_flight gauge",
        f"shop_orders_in_flight {st['in_flight']}",
        "# TYPE shop_order_latency_seconds gauge",
        f'shop_order_latency_seconds{{quantile="0.5"}} {st["latency_p50"] or 0}',
        f'shop_order_latency_seconds{{quantile="0.95"}} {st["latency_p95"] or 0}',
        "# TYPE shop_generator_rate gauge",
        f"shop_generator_rate {generator.rate if generator.running else 0}",
        "# TYPE shop_workers gauge",
    ]
    for state_name in ("running", "stopped", "crashed"):
        lines.append(f'shop_workers{{state="{state_name}"}} {sum(1 for w in workers.values() if w.state() == state_name)}')
    return PlainTextResponse("\n".join(lines) + "\n")


@app.post("/api/scenarios/{scenario_id}")
def run_scenario(scenario_id: str):
    sc = SCENARIOS.get(scenario_id)
    if not sc:
        raise HTTPException(404, "unknown scenario")
    _fault_lock()
    note("drill", f"drill: {sc['title']}")
    sc["apply"]()
    return {"ok": True, "scenario": scenario_id}


@app.post("/api/reset")
def reset(skip_backlog: bool = False):
    """Stop traffic and every worker. Kafka keeps the data and the committed offsets.

    With skip_backlog=true the worker groups' committed offsets on `orders` are
    moved to the log end, so the next worker starts fresh instead of chewing
    through a load test's leftovers. Same thing as:
      kafka-consumer-groups --reset-offsets --to-latest --execute
    Kafka only allows it while the group has no live members.
    """
    generator.running = False
    generator.hot_customer = None
    _stop_all_workers()
    note("drill", "stop everything" + (" + skip backlog" if skip_backlog else ""))
    if not skip_backlog:
        return {"ok": True}
    groups = {w.group for w in workers.values()} | {"order-processor"}
    # Kafka refuses to move offsets while the group has members, so wait for
    # the workers to leave (a SIGTERMed worker leaves within a second; a killed
    # one only after session.timeout.ms).
    deadline = time.time() + 15
    while time.time() < deadline:
        descs = admin.describe_consumer_groups(list(groups), request_timeout=5)
        live = {g for g, f in descs.items() if f.result().members}
        if not live:
            break
        time.sleep(0.5)
    skipped = {}
    md = admin.list_topics(ORDERS_TOPIC, timeout=5).topics[ORDERS_TOPIC]
    for g in groups:
        tps = []
        for pid in md.partitions:
            _low, high = kafka_watcher.watermarks(pid)
            tps.append(TopicPartition(ORDERS_TOPIC, pid, high))
        try:
            res = admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(g, tps)])[g].result(timeout=5)
            errors = [str(tp.error) for tp in res.topic_partitions if tp.error]
            skipped[g] = errors[0] if errors else {tp.partition: tp.offset for tp in tps}
        except Exception as e:  # group never existed, or still has members
            skipped[g] = f"not reset: {e}"
    return {"ok": True, "skipped_to": skipped}




@app.post("/api/wipe")
def wipe():
    """Start fresh: stop everything, delete and recreate the topics so offsets begin at 0.

    Same as `kafka-topics --delete` followed by `--create`. Consumer group
    offsets for the deleted topics are dropped by the broker as well.
    """
    generator.running = False
    generator.hot_customer = None
    _stop_all_workers()
    incident["id"] = None
    for b in (1, 2, 3):   # topics must be recreated with all replicas, so every broker has to be up
        subprocess.run(["docker", "start", f"kafka-{b}"], capture_output=True, timeout=30)
    deadline = time.time() + 15
    while time.time() < deadline and any(w.state() == "running" for w in workers.values()):
        time.sleep(0.2)
    for wid, w in list(workers.items()):
        if w.state() == "running":
            w.proc.kill()          # did not leave in time: force it, keep nothing behind
        workers.pop(wid, None)
    for pid in _untracked_worker_pids():
        os.kill(pid, signal.SIGKILL)
    deadline = time.time() + 30
    while time.time() < deadline and len(admin.list_topics(timeout=5).brokers) < 3:
        time.sleep(1)

    specs = {ORDERS_TOPIC: PARTITIONS, STATUS_TOPIC: PARTITIONS, DLQ_TOPIC: 1}
    # Delete, then confirm the topics are really gone. A delete issued while the
    # brokers are still finishing a previous create can be accepted and then lost,
    # so repeat until the metadata agrees.
    for attempt in range(6):
        existing = admin.list_topics(timeout=5).topics
        to_delete = [t for t in specs if t in existing]
        if not to_delete:
            break
        for t, fut in admin.delete_topics(to_delete, operation_timeout=15).items():
            try:
                fut.result()
            except KafkaException as e:
                if e.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise
        deadline = time.time() + 5
        while time.time() < deadline and any(t in admin.list_topics(timeout=5).topics for t in specs):
            time.sleep(0.3)
    print("wipe: topics gone from metadata", flush=True)
    rf = min(REPLICATION, len(admin.list_topics(timeout=5).brokers))
    pending = dict(specs)
    deadline = time.time() + 30
    while pending and time.time() < deadline:
        # A deleted topic can still answer "already exists" for a moment while the
        # brokers finish removing it, so retry until every create goes through.
        for t, fut in admin.create_topics([NewTopic(t, num_partitions=n, replication_factor=rf) for t, n in pending.items()]).items():
            try:
                fut.result()
                pending.pop(t, None)
            except KafkaException as e:
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        if pending:
            time.sleep(0.5)
    store.clear()
    kafka_watcher.reset_admin()      # its metadata points at the deleted topics
    # The status listener subscribed to the old topic; give it a fresh subscription.
    global status_listener
    status_listener.stop.set()
    status_listener.join(timeout=5)
    status_listener = StatusListener()
    status_listener.start()
    return {"ok": not pending, "recreated": specs, "still_pending": list(pending)}


@app.post("/api/broker/{broker_id}/{action}")
def broker_action(broker_id: int, action: str):
    if action not in ("stop", "start") or broker_id not in (1, 2, 3):
        raise HTTPException(400, "action must be stop or start, broker 1-3")
    key = f"broker-{broker_id}-{action}"
    pending_actions[key] = {"at": time.time(), "eta": 8, "done_at": None}
    try:
        return docker_broker(broker_id, action)
    finally:
        pending_actions[key]["done_at"] = time.time()


# --------------------------------------------------------------------------- on-call actions
# Things an on-call engineer can trigger but that are not knobs: a producer deploy,
# or another team fixing their dependency. Each takes a realistic moment.
def _deploy_key_fix():
    time.sleep(6)
    generator.key_mode = "order_id"
    generator.hot_customer = None
    note("drill", "deployed: producers now key by order_id")


def _downstream_recovers():
    time.sleep(25)
    running = [w for w in workers.values() if w.group == "order-processor" and w.state() == "running"]
    delay = running[0].delay if running else 0.1
    names = [w.name for w in running] or ["worker-a"]
    for w in running:
        w.proc.send_signal(signal.SIGTERM)
        w.proc.wait(timeout=10)
        workers.pop(w.id, None)
    for name in names:
        w = WorkerProc(name, "order-processor", delay, 0.0, 0)
        workers[w.id] = w
    note("drill", "downstream team: dependency fixed, consumers restarted at 0 % failure")


ACTIONS = {
    "deploy-key-fix": ("deploy in progress: producers will key by order_id in ~6 s", _deploy_key_fix),
    "page-downstream": ("paged the downstream team: they say ~25 s", _downstream_recovers),
}


ACTION_ETA = {"deploy-key-fix": 6, "page-downstream": 25, "short-session": 4, "rolling-restart": 8, "add-partitions": 2, "replay-dlq": 4}
pending_actions = {}   # name -> {"at", "eta", "done_at"}


@app.post("/api/actions/{name}")
def run_action(name: str):
    if name not in ACTIONS:
        raise HTTPException(404, "unknown action")
    if name in pending_actions and not pending_actions[name].get("done_at"):
        return {"ok": True, "action": name, "text": "already in progress"}
    text, fn = ACTIONS[name]
    note("drill", text)
    pending_actions[name] = {"at": time.time(), "eta": ACTION_ETA.get(name, 3), "done_at": None}

    def run():
        try:
            fn()
        finally:
            pending_actions[name]["done_at"] = time.time()
    threading.Thread(target=run, daemon=True).start()
    return {"ok": True, "action": name, "text": text}


@app.get("/api/actions")
def actions_status():
    now = time.time()
    return {n: {"running": not a["done_at"], "left": max(0, int(a["at"] + a["eta"] - now)), "done_ago": int(now - a["done_at"]) if a["done_at"] else None}
            for n, a in pending_actions.items()}


# --------------------------------------------------------------------------- incidents (the debugging game)
# An incident sets up a real failure without telling the player which. The player
# investigates, declares a root cause, and only then gets the matching fixes. Every
# check below reads live broker and worker state.
HOLD_SECONDS = 10
GENERIC_CHECKS = {"lag under 20", "lag under 5", "lag back to 0", "no unexpected duplicates", "lag trend flat or decreasing"}

CAUSES = {
    "capacity": {
        "confirm": ["commit rate stays below produce rate with every member busy", "lag grows on all partitions at a similar pace"],
        "refute": ["a single partition carrying all the lag (key skew)", "retries or dead letters (dependency)", "members ≠ live processes (zombie or crash)"],
        "evidence": ["committed rate well below produced rate", "lag growing on all partitions at a similar pace", "every member busy, none crashed, retries near zero"],
        "misleading": ["a single partition slightly ahead of the others looks like a hot key; check the key pattern, it is spread"],
        "alternatives": ["add consumers up to the partition count", "deploy faster processing (batching, fewer round trips downstream)", "add partitions, then consumers", "throttle the producers upstream (backpressure)"],
        "risky": ["reset offsets to latest drops the backlog: fast, but those orders are never processed"],
        "title": "Not enough consumer throughput",
        "explain": "Messages arrive faster than the group can commit them. Lag grows on every partition evenly, "
                   "every consumer is busy. Fix: more consumers (up to the partition count) or faster processing.",
        "scenario": "lag-storm",
        "fixes": [],
    },
    "hot-key": {
        "confirm": ["top key share near 100 % in recent messages", "lag on exactly one partition, its owner busy, the others idle"],
        "refute": ["lag spread evenly over partitions", "distinct keys in the dozens"],
        "evidence": ["top key share near 100 % of recent messages", "lag on exactly one partition, the others at 0", "one member busy, the others idle"],
        "misleading": ["adding consumers looks like the obvious move; the idle ones get the empty partitions and nothing changes"],
        "alternatives": ["change the producer key to something with higher cardinality", "add a random suffix to the hot key if per-key ordering is not needed", "brute force: one very fast consumer on the hot partition (works until traffic grows)"],
        "risky": ["adding partitions moves keys around: per-key ordering breaks once, and the hot key still lands on one partition"],
        "title": "Hot partition: one key dominates",
        "explain": "All messages carry the same key, so they hash to one partition. One consumer does all the work "
                   "while the others idle. Adding consumers cannot help. Fix: change the key in the producer.",
        "scenario": "hot-partition",
        "fixes": [{"label": "Deploy producer fix: key by order_id (~6 s)", "call": "/api/actions/deploy-key-fix"}],
    },
    "crash-loop": {
        "confirm": ["a crashed process in the consumer list", "partitions with no owner while lag is frozen"],
        "refute": ["every partition has a live owner", "commit rate still moving"],
        "evidence": ["a crashed process in the consumer list", "partitions with no owner while the group is empty", "lag frozen, not growing fast: nobody consumes", "after a restart: duplicates appear"],
        "misleading": ["lag that does not move can look like a broker problem; brokers and in-sync replicas are fine"],
        "alternatives": ["start a replacement consumer: it replays from the last committed offset", "make processing idempotent (dedupe on order_id) so the replay is harmless", "commit more often to shrink the replay window"],
        "risky": ["reset offsets to latest skips the uncommitted messages: no duplicates, but those orders are lost"],
        "title": "A consumer crashes before committing",
        "explain": "A consumer processes messages but dies before committing offsets. Lag stops moving on its partitions; "
                   "the group has no live member for them. A replacement replays from the last commit: duplicates.",
        "scenario": "crash-loop",
        "fixes": [],
    },
    "downstream": {
        "confirm": ["retries and dead letters climbing", "consumer logs showing timeouts or declines from the dependency"],
        "refute": ["zero retries and zero dead letters"],
        "evidence": ["retries climbing", "dead letters in orders.dlq", "consumer logs: transient and permanent errors from the dependency", "committed rate low although consumers are alive and busy"],
        "misleading": ["low commit rate looks like a capacity problem; scaling consumers only multiplies the failed calls"],
        "alternatives": ["page the owning team; keep the group alive", "circuit-break: pause consumption instead of hammering the dependency", "after recovery: replay orders.dlq"],
        "risky": ["raising the failure tolerance or disabling retries pushes everything to the DLQ"],
        "title": "Downstream dependency failing",
        "explain": "Most messages fail with retries and backoff; some are dead-lettered. The consumers are fine, "
                   "the thing they call is not. Fix: page that team; keep the group alive; drain lag after.",
        "scenario": "poison-pill",
        "fixes": [{"label": "Page the downstream team (~25 s)", "call": "/api/actions/page-downstream"}],
    },
    "zombie": {
        "confirm": ["group members more than live processes", "one partition lagging while its owner's process is gone"],
        "refute": ["members equal live processes", "lag spread across partitions"],
        "evidence": ["group members > live processes", "one partition with growing lag while the others drain", "group state stable: the coordinator has not noticed"],
        "misleading": ["the group says stable, which looks healthy; compare members with live processes"],
        "alternatives": ["wait for session.timeout.ms (45 s) and let the coordinator evict it", "restart the group with a shorter session timeout", "make shutdown clean: handle SIGTERM and close the consumer"],
        "risky": ["a very short session timeout evicts healthy members on every GC pause: rebalance storm"],
        "title": "Dead consumer still owns a partition",
        "explain": "A consumer process died without LeaveGroup. The coordinator keeps its partition assigned until "
                   "session.timeout.ms (45 s). Lag grows on that one partition while the others are fine.",
        "scenario": "zombie",
        "fixes": [{"label": "Restart the group with session.timeout.ms = 10 s", "call": "/api/actions/short-session"}],
    },
    "broker-down": {
        "confirm": ["fewer than 3 brokers in metadata", "in-sync replicas below replicas on every partition", "leaders moved to other brokers"],
        "refute": ["3 brokers up and every partition fully in sync"],
        "evidence": ["brokers 2 up", "in-sync 2/3 on every partition", "leaders moved; produce and consume continue"],
        "misleading": ["nothing is lost and lag may stay low, so it is easy to dismiss; one more failure and writes stop or data is at risk"],
        "alternatives": ["bring the broker back and let it catch up", "with min.insync.replicas=2 and acks=all, a second failure would block writes instead of risking data"],
        "risky": ["running long with 2/3 replicas: the next broker loss is a quorum or data problem"],
        "title": "A broker is down",
        "explain": "One broker died. Partitions it led got new leaders; every partition shows fewer in-sync replicas. "
                   "Producing still works with min.insync.replicas=1. Fix: bring the broker back.",
        "scenario": "broker-down",
        "fixes": [{"label": "Start broker 2", "call": "/api/broker/2/start"}],
    },
}

incident = {"id": None, "cause": None, "started_at": None, "alarmed_at": None, "diagnosed_at": None,
            "resolved_at": None, "wrong": [], "ok_since": None, "stats0": None, "practice": False,
            "mode": "standard", "evidence": [], "lag_at_diagnosis": None, "replayed": 0, "fail_since": None}


def _running():
    return [w for w in workers.values() if w.group == "order-processor" and w.state() == "running"]


def _kafka_group(gid="order-processor"):
    return next((g for g in kafka_watcher.view.get("groups", []) if g["id"] == gid), {"members": []})


def _since(kind, prefix=None):
    t0 = incident["started_at"] or 0
    return [e for e in timeline if e["at"] >= t0 and e["kind"] == kind and (prefix is None or e["text"].startswith(prefix))]


def _rate():
    pts = kafka_watcher.series()
    return pts[-1] if pts else {"in": 0.0, "processed": 0.0, "lag": 0}


def _plag(p):
    """Lag of one partition for order-processor, never negative: a commit can be read a
    moment before the next watermark refresh."""
    return max(0, p["high"] - p["committed"]["order-processor"]) if "order-processor" in p["committed"] else None


def _lag():
    return sum(_plag(p) or 0 for p in kafka_watcher.view.get("partitions", []))


def _dup():
    """Messages processed more than once since the incident began. Dead letters that the
    operator replayed on purpose are expected, so they do not count."""
    s0 = incident["stats0"] or store.counters
    c = store.counters
    return max(0, (c["ready"] - s0["ready"]) + (c["dead_lettered"] - s0["dead_lettered"]) - (c["placed"] - s0["placed"]) - incident.get("replayed", 0))


def incident_checks(cause):
    r = _rate()
    parts = kafka_watcher.view.get("partitions", [])
    brokers = kafka_watcher.view.get("brokers", [])
    run = _running()
    common_lag = ("lag under 20", _lag() < 20)
    if cause == "capacity":
        return [("producers still at 20 msg/s or more", generator.running and generator.rate >= 20),
                ("commit rate at least 90 % of produce rate", r["processed"] >= r["in"] * 0.9), common_lag]
    if cause == "hot-key":
        return [("producers still at 8 msg/s or more", generator.running and generator.rate >= 8),
                ("key spreads over partitions", not generator.hot_customer or generator.key_mode != "customer"), common_lag]
    if cause == "crash-loop":
        live = {w.name for w in run}
        return [("a live consumer owns every partition", bool(parts) and all(p["owner"].get("order-processor") in live for p in parts)),
                ("lag under 5", _lag() < 5), ("duplicates observed (replayed messages)", _dup() > 0)]
    if cause == "downstream":
        return [("producers still on", generator.running),
                ("at least one message dead-lettered", store.counters["dead_lettered"] - (incident["stats0"] or store.counters)["dead_lettered"] > 0),
                ("all running consumers at 5 % failure or less", bool(run) and all(w.fail_rate <= 0.05 for w in run)), common_lag]
    if cause == "zombie":
        members = _kafka_group()["members"]
        return [("coordinator evicted the dead member (group members = live processes)", bool(run) and len(members) == len(run)),
                ("lag under 5", _lag() < 5)]
    if cause == "broker-down":
        return [("all 3 brokers back", len(brokers) == 3),
                ("every partition in sync 3/3", bool(parts) and all(len(p["isr"]) == len(p["replicas"]) for p in parts)), common_lag]
    return []


def incident_alarm(cause):
    parts = kafka_watcher.view.get("partitions", [])
    if cause == "broker-down":
        return len(kafka_watcher.view.get("brokers", [])) < 3 or any(len(p["isr"]) < len(p["replicas"]) for p in parts)
    if cause == "downstream":
        return store.counters["retries"] - (incident["stats0"] or store.counters)["retries"] > 3
    if cause == "zombie":
        return bool(_since("workers", "KILL")) and len(_kafka_group()["members"]) > len(_running())
    if cause == "crash-loop":
        return bool(_since("workers", "CRASH")) and _lag() > 0
    return _lag() > (30 if cause == "capacity" else 20)


class IncidentIn(BaseModel):
    cause: str | None = None     # None = random, hidden. Given = practice run.
    mode: str = "standard"       # guided: cause shown + hints. standard: alert + hints. expert: alert only.


@app.post("/api/incidents/start")
def incident_start(body: IncidentIn):
    cause = body.cause if body.cause in CAUSES else random.choice([c for c in CAUSES if c != incident["cause"]] or list(CAUSES))
    mode = body.mode if body.mode in ("guided", "standard", "expert") else "standard"
    incident.update({"id": uuid.uuid4().hex[:6], "cause": cause, "started_at": time.time(), "alarmed_at": None,
                     "diagnosed_at": None, "resolved_at": None, "wrong": [], "ok_since": None,
                     "stats0": dict(store.counters), "practice": body.cause is not None or mode == "guided",
                     "mode": mode, "evidence": [], "lag_at_diagnosis": None, "replayed": 0, "fail_since": None, "journal": [], "cli": []})
    pending_actions.clear()
    for wid, w in list(workers.items()):   # evidence from an earlier incident is not evidence for this one
        if w.state() != "running":
            workers.pop(wid, None)
    for b in (1, 2, 3):   # a previous broker-down run may have left one stopped
        subprocess.run(["docker", "start", f"kafka-{b}"], capture_output=True, timeout=30)
    note("drill", "incident started" + (f" (practice: {CAUSES[cause]['title']})" if incident["practice"] else ""))
    SCENARIOS[CAUSES[cause]["scenario"]]["apply"]()
    return incident_status()


class DiagnoseIn(BaseModel):
    cause: str


@app.post("/api/incidents/diagnose")
def incident_diagnose(body: DiagnoseIn):
    if not incident["id"] or incident["resolved_at"]:
        raise HTTPException(400, "no open incident")
    if body.cause not in CAUSES:
        raise HTTPException(400, "unknown cause")
    if body.cause == incident["cause"]:
        if not incident["diagnosed_at"]:
            incident["diagnosed_at"] = time.time()
            incident["lag_at_diagnosis"] = _lag()
            note("drill", f"root cause declared: {CAUSES[body.cause]['title']}")
        return {"correct": True, **incident_status()}
    if body.cause not in incident["wrong"]:
        incident["wrong"].append(body.cause)
    note("drill", f"wrong diagnosis: {CAUSES[body.cause]['title']}")
    return {"correct": False, **incident_status()}


class JournalIn(BaseModel):
    text: str


@app.post("/api/incidents/journal")
def incident_journal(body: JournalIn):
    if incident["id"]:
        incident.setdefault("journal", []).append({"at": time.time(), "text": body.text[:500]})
        note("drill", f"note: {body.text[:80]}")
    return {"ok": True}


@app.get("/api/incidents/hypothesis/{cause}")
def hypothesis(cause: str):
    """What would confirm or rule out a suspected cause. Hidden in expert mode."""
    if cause not in CAUSES:
        raise HTTPException(404)
    if incident["id"] and incident["mode"] == "expert" and not incident["resolved_at"]:
        return {"cause": cause, "title": CAUSES[cause]["title"], "confirm": [], "refute": [], "hidden": True}
    if incident["id"]:
        incident["evidence"].append({"at": time.time(), "what": f"hypothesis: {CAUSES[cause]['title']}"})
    return {"cause": cause, "title": CAUSES[cause]["title"], "confirm": CAUSES[cause]["confirm"], "refute": CAUSES[cause]["refute"], "hidden": False}


class EvidenceIn(BaseModel):
    what: str


@app.post("/api/incidents/evidence")
def incident_evidence(body: EvidenceIn):
    """The UI reports what the learner looked at: a consumer log, the Inspector, a CLI run."""
    if incident["id"] and not incident["resolved_at"]:
        incident["evidence"].append({"at": time.time(), "what": body.what[:120]})
    return {"ok": True}


@app.get("/api/incidents/current")
def incident_status():
    if not incident["id"]:
        return {"open": False, "causes": {k: v["title"] for k, v in CAUSES.items()}}
    cause = incident["cause"]
    now = time.time()
    alarm = incident_alarm(cause)
    if alarm and not incident["alarmed_at"]:
        incident["alarmed_at"] = now
    checks = [{"label": l, "ok": bool(ok)} for l, ok in incident_checks(cause)]
    # scenario-specific correctness on top of recovery: duplicates are only expected after a crash
    if cause != "crash-loop":
        checks.append({"label": "no unexpected duplicates", "ok": _dup() == 0})
    pts = kafka_watcher.series()
    if len(pts) >= 6:
        checks.append({"label": "lag trend flat or decreasing", "ok": pts[-1]["lag"] <= pts[-6]["lag"] or pts[-1]["lag"] < 5})
    all_ok = bool(checks) and all(c["ok"] for c in checks)
    if all_ok and incident["alarmed_at"] and incident["diagnosed_at"]:
        incident["ok_since"] = incident["ok_since"] or now
        incident["fail_since"] = None
    elif incident.get("ok_since"):
        # A single-sample blip (a message in flight, a watermark read a beat early) does
        # not restart the hold. Two seconds of red does.
        incident["fail_since"] = incident.get("fail_since") or now
        if now - incident["fail_since"] >= 2:
            incident["ok_since"] = None
            incident["fail_since"] = None
    else:
        incident["ok_since"] = None
        incident["fail_since"] = None
    if incident["ok_since"] and now - incident["ok_since"] >= HOLD_SECONDS and not incident["resolved_at"]:
        incident["resolved_at"] = now
        note("drill", f"incident resolved in {int(now - incident['started_at'])} s")
    revealed = bool(incident["diagnosed_at"] or incident["resolved_at"] or incident["practice"])
    status = "resolved" if incident["resolved_at"] else "alarm" if alarm and not all_ok else "investigating" if incident["alarmed_at"] else "building"
    lag = _lag()
    r = _rate()
    eta = (lag / r["processed"]) if r["processed"] > 0 else None
    severity = "SEV1" if (lag > 200 or len(kafka_watcher.view.get("brokers", [])) < 3) else "SEV2" if lag > 50 else "SEV3"
    impact = (f"about {lag} orders waiting; at the current commit rate they clear in ~{int(eta)} s" if eta is not None and lag
              else f"{lag} orders waiting and nobody is committing" if lag else "no orders waiting right now")
    c = CAUSES[cause]
    return {
        "open": True, "id": incident["id"], "status": status, "started_at": incident["started_at"],
        "alarmed_at": incident["alarmed_at"], "diagnosed_at": incident["diagnosed_at"], "resolved_at": incident["resolved_at"],
        "wrong": [CAUSES[c]["title"] for c in incident["wrong"]], "practice": incident["practice"],
        "cause": cause if revealed else None,
        "title": CAUSES[cause]["title"] if revealed else None,
        "explain": CAUSES[cause]["explain"] if revealed else None,
        "mode": incident["mode"], "severity": severity, "impact": impact, "lag": lag,
        "topic": ORDERS_TOPIC, "group": "order-processor",
        # Every on-call action is always on the table (most of them do nothing for the real
        # cause). Showing only the matching one would give the diagnosis away.
        "fixes": [dict(f, relevant=bool(incident["diagnosed_at"]) and f in c["fixes"])
                  for f in [f for cc in CAUSES.values() for f in cc["fixes"]]],
        # Cause-specific checks would name the cause. Before a diagnosis only the generic
        # recovery conditions are shown; resolution still evaluates the full list.
        "checks": checks if incident["diagnosed_at"] else [ch for ch in checks if ch["label"] in GENERIC_CHECKS],
        "needs_diagnosis": not incident["diagnosed_at"],
        "evidence": incident["evidence"],
        "debrief": ({"evidence": c["evidence"], "misleading": c["misleading"], "alternatives": c["alternatives"], "risky": c["risky"],
                     "actions": [e for e in reversed(list(timeline)) if e["at"] >= incident["started_at"]],
                     "journal": incident.get("journal", []), "cli": incident.get("cli", []),
                     "lag_at_diagnosis": incident["lag_at_diagnosis"]} if incident["resolved_at"] else None),
        "hold": int(now - incident["ok_since"]) if incident["ok_since"] else 0, "hold_needed": HOLD_SECONDS,
        "causes": {k: v["title"] for k, v in CAUSES.items()},
    }


@app.post("/api/incidents/abandon")
def incident_abandon():
    incident["id"] = None
    return {"ok": True}


def _short_session():
    """Zombie fix: restart the live members with a 10 s session timeout. The dead one is
    evicted when its old 45 s timeout runs out; the new members would be noticed in 10 s."""
    live = _running()
    names = [w.name for w in live] or ["worker-a"]
    delay = live[0].delay if live else 0.1
    for w in live:
        w.proc.send_signal(signal.SIGTERM)
        w.proc.wait(timeout=10)
        workers.pop(w.id, None)
    for name in names:
        w = WorkerProc(name, "order-processor", delay, 0.0, 0, session_timeout_ms=10000)
        workers[w.id] = w
    note("drill", "consumers restarted with session.timeout.ms = 10 s")


ACTIONS["short-session"] = ("restarting the group with a 10 s session timeout", _short_session)


def _incident_ticker():
    """Evaluate the open incident every second even when no browser is polling,
    so the hold timer and the resolution do not depend on the UI."""
    while True:
        time.sleep(1.0)
        try:
            if incident["id"]:
                incident_status()
        except Exception as e:
            print("incident ticker:", e, flush=True)


threading.Thread(target=_incident_ticker, daemon=True).start()


# --------------------------------------------------------------------------- runbook actions
def _rolling_restart():
    """Restart every live member of order-processor one at a time, same settings."""
    for w in list(_running()):
        name, delay, fail, st = w.name, w.delay, w.fail_rate, w.session_timeout_ms
        w.proc.send_signal(signal.SIGTERM)
        w.proc.wait(timeout=10)
        workers.pop(w.id, None)
        nw = WorkerProc(name, "order-processor", delay, fail, 0, session_timeout_ms=st)
        workers[nw.id] = nw
        time.sleep(2)
    note("drill", "rolling restart of order-processor done")


def _add_partitions():
    """kafka-topics --alter --partitions 6 on orders. Raises the parallelism ceiling.
    Existing keys may now hash to a different partition: per-key ordering breaks once."""
    md = admin.list_topics(ORDERS_TOPIC, timeout=5).topics[ORDERS_TOPIC]
    target = 6
    if len(md.partitions) >= target:
        note("drill", f"orders already has {len(md.partitions)} partitions")
        return
    admin.create_partitions([NewPartitions(ORDERS_TOPIC, target)])[ORDERS_TOPIC].result()
    note("drill", f"orders: {len(md.partitions)} → {target} partitions (key → partition mapping changed)")


replayed = {"offset": {}}   # dlq partition -> next offset to replay


def _replay_dlq():
    """Read orders.dlq from where the last replay stopped and produce each order back to orders."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-replay-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False})
    md = c.list_topics(DLQ_TOPIC, timeout=5).topics[DLQ_TOPIC]
    tps = [TopicPartition(DLQ_TOPIC, pid, replayed["offset"].get(pid, 0)) for pid in md.partitions]
    c.assign(tps)
    n, idle = 0, 0
    while idle < 4:
        msg = c.poll(0.5)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            continue
        order = json.loads(msg.value())
        for k in ("dlq_reason", "source_partition", "source_offset"):
            order.pop(k, None)
        order["replayed"] = True
        producer.produce(ORDERS_TOPIC, key=msg.key(), value=json.dumps(order).encode())
        replayed["offset"][msg.partition()] = msg.offset() + 1
        n += 1
        idle = 0
    producer.flush(5)
    c.close()
    if incident["id"]:
        incident["replayed"] = incident.get("replayed", 0) + n
    note("drill", f"replayed {n} dead letter(s) from orders.dlq back to orders")


ACTIONS["rolling-restart"] = ("rolling restart of order-processor", _rolling_restart)
ACTIONS["add-partitions"] = ("adding partitions to orders", _add_partitions)
ACTIONS["replay-dlq"] = ("replaying dead letters", _replay_dlq)


def system_facts():
    """What an on-call engineer can observe. Read-only. Feeds the System state panel."""
    r = _rate()
    parts = kafka_watcher.view.get("partitions", [])
    brokers = kafka_watcher.view.get("brokers", [])
    g = _kafka_group()
    with lock:
        recent = [store.orders[i] for i in list(store.order_ids)[-60:]]
    keys = [o.get("key") for o in recent if o.get("produced")]
    top_share = 0.0
    if keys:
        top = max(set(keys), key=keys.count)
        top_share = keys.count(top) / len(keys)
    run = _running()
    s0 = incident["stats0"] if incident["id"] else {k: 0 for k in store.counters}
    return {
        "produce_rate": round(r["in"], 1), "commit_rate": round(r["processed"], 1), "lag": _lag(),
        "lag_per_partition": [{"id": p["id"], "lag": _plag(p), "owner": p["owner"].get("order-processor")} for p in parts],
        "key_top_share": round(top_share * 100), "key_distinct": len(set(keys)),
        "retries": max(0, store.counters["retries"] - s0["retries"]), "dead_lettered": max(0, store.counters["dead_lettered"] - s0["dead_lettered"]),
        "duplicates": _dup() if incident["id"] else 0,
        "group_state": g.get("state", "none"), "group_members": [m if isinstance(m, str) else m.get("client_id") for m in g.get("members", [])],
        "unknown_members": [m for m in [m if isinstance(m, str) else m.get("client_id") for m in g.get("members", [])] if m not in {w.name for w in run}],
        "live_processes": [w.name for w in run], "crashed": [w.name for w in workers.values() if w.state() == "crashed"],
        "session_timeout_ms": run[0].session_timeout_ms if run else None,
        "brokers_up": len(brokers), "isr": [f"{len(p['isr'])}/{len(p['replicas'])}" for p in parts],
        "partitions": len(parts),
    }


@app.get("/api/facts")
def facts():
    return system_facts()


@app.get("/api/history")
def history():
    """Per-partition lag history and the consumer-group event stream, for the Investigate page."""
    return {"now": time.time(), "partitions": kafka_watcher.partition_series(),
            "group_events": list(kafka_watcher.group_events), "timeline": list(timeline)}


# --------------------------------------------------------------------------- CLI (real commands, read-only)
CLI_TOOLS = {"kafka-topics", "kafka-consumer-groups", "kafka-console-consumer", "kafka-configs", "kafka-log-dirs",
             "kafka-broker-api-versions", "kafka-get-offsets", "kafka-metadata-quorum", "kafka-cluster"}
CLI_FORBIDDEN = {"--delete", "--create", "--alter", "--execute", "--reset-offsets", "--delete-offsets", "--add-config",
                 "--delete-config", "--reassignment-json-file", "--elect", "--unregister", "--format"}
CLI_ALLOW_WITH_DRY_RUN = {"--reset-offsets"}
cli_history = deque(maxlen=100)


class CliIn(BaseModel):
    cmd: str


@app.post("/api/cli")
def run_cli(body: CliIn):
    """Run a read-only Kafka CLI command inside the kafka-1 container. Real output, real exit code.
    Mutating flags are refused so the terminal cannot change the thing being investigated."""
    try:
        argv = shlex.split(body.cmd.strip())
    except ValueError as e:
        raise HTTPException(400, f"cannot parse: {e}")
    if not argv:
        raise HTTPException(400, "empty command")
    tool = argv[0].replace(".sh", "")
    if tool not in CLI_TOOLS:
        raise HTTPException(400, f"only these tools are allowed: {', '.join(sorted(CLI_TOOLS))}")
    flags = {a for a in argv[1:] if a.startswith("--")}
    bad = (flags & CLI_FORBIDDEN) - (CLI_ALLOW_WITH_DRY_RUN if "--dry-run" in flags else set())
    if bad:
        raise HTTPException(400, f"refused, this terminal is read-only: {' '.join(sorted(bad))}")
    # Inside the container the brokers know each other as kafka-N:29092; the localhost
    # addresses only work from the Mac. Rewrite transparently so every tool reaches all brokers.
    if "--bootstrap-server" in flags:
        i = argv.index("--bootstrap-server")
        if i + 1 < len(argv) and "localhost" in argv[i + 1]:
            argv[i + 1] = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
    else:
        argv += ["--bootstrap-server", "kafka-1:29092,kafka-2:29092,kafka-3:29092"]
    if tool == "kafka-console-consumer":
        if "--max-messages" not in flags:
            argv += ["--max-messages", "10"]
        if "--timeout-ms" not in flags:
            argv += ["--timeout-ms", "5000"]
    started = time.time()
    try:
        r = subprocess.run(["docker", "exec", "kafka-1"] + [tool] + argv[1:], capture_output=True, text=True, timeout=30)
        out, err, code = r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        out, err, code = "", "timed out after 30 s", 124
    # trim the JVM noise the console tools print
    err = "\n".join(l for l in err.splitlines() if "SLF4J" not in l and "log4j" not in l.lower())
    rec = {"at": started, "cmd": body.cmd.strip(), "stdout": out[-20000:], "stderr": err[-4000:], "code": code, "ms": int((time.time() - started) * 1000)}
    cli_history.appendleft(rec)
    if incident["id"] and not incident["resolved_at"]:
        incident.setdefault("cli", []).append({"at": started, "cmd": rec["cmd"], "code": code})
        incident["evidence"].append({"at": started, "what": f"cli: {rec['cmd'][:80]}"})
    return rec


@app.get("/api/cli/history")
def cli_hist():
    return list(cli_history)


CLI_SUGGESTIONS = [
    {"cmd": "kafka-consumer-groups --describe --group order-processor", "why": "committed offset, log end offset and lag per partition, plus which member owns each partition"},
    {"cmd": "kafka-consumer-groups --describe --group order-processor --members --verbose", "why": "members, their hosts and assignments"},
    {"cmd": "kafka-consumer-groups --describe --group order-processor --state", "why": "group state, coordinator, assignor"},
    {"cmd": "kafka-topics --describe --topic orders", "why": "leader, replicas and in-sync replicas per partition"},
    {"cmd": "kafka-topics --describe --under-replicated-partitions", "why": "partitions with a replica out of sync, across all topics"},
    {"cmd": "kafka-get-offsets --topic orders", "why": "log end offset per partition"},
    {"cmd": "kafka-console-consumer --topic orders.dlq --from-beginning --max-messages 5 --property print.key=true", "why": "what is in the dead-letter topic"},
    {"cmd": "kafka-console-consumer --topic orders --max-messages 5 --property print.key=true --property print.partition=true", "why": "a few live messages with their keys and partitions"},
    {"cmd": "kafka-metadata-quorum describe --status", "why": "KRaft controller quorum: which nodes vote and who leads"},
    {"cmd": "kafka-broker-api-versions", "why": "which brokers answer at all"},
]


@app.get("/api/cli/suggestions")
def cli_suggestions():
    if incident["id"] and incident["mode"] == "expert" and not incident["resolved_at"]:
        return []
    return CLI_SUGGESTIONS


