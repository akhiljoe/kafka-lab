"""Kafka inspector: shows what the broker actually holds, straight from the admin API.

Everything here is read-only. Once a second it collects:
  * brokers, topics, partitions, leaders and in-sync replicas
  * low / high watermark per partition (log start and end offsets)
  * consumer groups, their members, assignments and committed offsets -> lag
  * a rolling history so the UI can draw produced/consumed rates
  * a change log of rebalances (members joining, leaving, partitions moving)
A separate tailer streams the last messages of each topic for the live feed.

    uvicorn services.inspector.app:app --port 8080 --reload
"""
import json
import threading
import time
import uuid
from collections import deque
from pathlib import Path

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaError, TopicPartition, OFFSET_END
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse

from services.common import BOOTSTRAP

STATIC = Path(__file__).parent / "static"
HISTORY_POINTS = 180        # 3 minutes at 1 Hz
TAIL_PER_TOPIC = 150
INTERNAL_PREFIX = "__"

lock = threading.Lock()
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
watermark_consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"inspector-wm-{uuid.uuid4().hex[:6]}",
                               "enable.auto.commit": False})


class Collector(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.snapshot = {"ok": False, "error": "collecting"}
        self.topic_history = {}     # topic -> deque of (t, end_total)
        self.group_history = {}     # group -> deque of (t, committed_total, lag_total)
        self.events = deque(maxlen=200)
        self.prev_groups = {}

    def log_event(self, kind, text, group=None):
        self.events.appendleft({"at": time.time(), "kind": kind, "group": group, "text": text})

    # -- pieces -----------------------------------------------------------
    def collect_cluster(self):
        md = admin.list_topics(timeout=5)
        dc = admin.describe_cluster(request_timeout=5).result()
        brokers = [{"id": n.id, "host": n.host, "port": n.port, "controller": dc.controller and n.id == dc.controller.id,
                    "leader_of": 0} for n in dc.nodes]
        by_id = {b["id"]: b for b in brokers}
        topics = []
        for name, t in sorted(md.topics.items()):
            if name.startswith(INTERNAL_PREFIX):
                continue
            parts = []
            for pid, p in sorted(t.partitions.items()):
                low, high = watermark_consumer.get_watermark_offsets(TopicPartition(name, pid), timeout=5, cached=False)
                if p.leader in by_id:
                    by_id[p.leader]["leader_of"] += 1
                parts.append({"id": pid, "leader": p.leader, "replicas": list(p.replicas), "isr": list(p.isrs),
                              "low": low, "high": high, "messages": high - low})
            topics.append({"name": name, "partitions": parts, "messages": sum(p["messages"] for p in parts)})
        return dc.cluster_id, brokers, topics

    def collect_groups(self, topics):
        listing = admin.list_consumer_groups(request_timeout=5).result()
        ids = [g.group_id for g in listing.valid]
        if not ids:
            return []
        descs = admin.describe_consumer_groups(ids, request_timeout=5)
        # The client accepts one group per call, so fan out and collect the futures.
        offsets = {g: admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g)], require_stable=False)[g] for g in ids}
        high = {(t["name"], p["id"]): p["high"] for t in topics for p in t["partitions"]}
        groups = []
        for gid in ids:
            d = descs[gid].result()
            committed = {}
            for tp in offsets[gid].result().topic_partitions:
                if tp.offset >= 0:
                    committed[(tp.topic, tp.partition)] = tp.offset
            members = []
            owner = {}
            for m in d.members:
                assigned = [(tp.topic, tp.partition) for tp in (m.assignment.topic_partitions if m.assignment else [])]
                for a in assigned:
                    owner[a] = m.client_id
                members.append({"member_id": m.member_id, "client_id": m.client_id, "host": m.host,
                                "assigned": [{"topic": t, "partition": p} for t, p in sorted(assigned)]})
            partitions = []
            keys = sorted(set(committed) | set(owner))
            for (t, p) in keys:
                if t.startswith(INTERNAL_PREFIX):
                    continue
                c = committed.get((t, p))
                h = high.get((t, p))
                partitions.append({"topic": t, "partition": p, "committed": c, "high": h,
                                   "lag": (h - c) if (c is not None and h is not None) else None,
                                   "owner": owner.get((t, p))})
            total_lag = sum(p["lag"] or 0 for p in partitions)
            groups.append({"id": gid, "state": str(d.state).split(".")[-1], "assignor": d.partition_assignor,
                           "coordinator": d.coordinator.id if d.coordinator else None, "members": members,
                           "partitions": partitions, "lag": total_lag,
                           "committed_total": sum(c for (t, _), c in committed.items() if not t.startswith(INTERNAL_PREFIX))})
        return groups

    def diff_groups(self, groups):
        now = {g["id"]: g for g in groups}
        for gid, g in now.items():
            old = self.prev_groups.get(gid)
            if old is None:
                self.log_event("group", f"group appeared ({g['state']}, {len(g['members'])} members)", gid)
                continue
            if old["state"] != g["state"]:
                self.log_event("state", f"{old['state']} → {g['state']}", gid)
            old_m = {m["client_id"] for m in old["members"]}
            new_m = {m["client_id"] for m in g["members"]}
            for m in sorted(new_m - old_m):
                self.log_event("join", f"member {m} joined", gid)
            for m in sorted(old_m - new_m):
                self.log_event("leave", f"member {m} left", gid)
            old_owner = {(p["topic"], p["partition"]): p["owner"] for p in old["partitions"]}
            for p in g["partitions"]:
                k = (p["topic"], p["partition"])
                if k in old_owner and old_owner[k] != p["owner"] and p["owner"]:
                    self.log_event("assign", f"{p['topic']}[{p['partition']}] {old_owner[k] or 'nobody'} → {p['owner']}", gid)
        for gid in set(self.prev_groups) - set(now):
            self.log_event("group", "group gone", gid)
        self.prev_groups = now

    def record_history(self, t, topics, groups):
        for topic in topics:
            self.topic_history.setdefault(topic["name"], deque(maxlen=HISTORY_POINTS)).append(
                (t, sum(p["high"] for p in topic["partitions"])))
        for g in groups:
            self.group_history.setdefault(g["id"], deque(maxlen=HISTORY_POINTS)).append((t, g["committed_total"], g["lag"]))

    @staticmethod
    def rates(series, idx=1):
        pts = list(series)
        out = []
        for a, b in zip(pts, pts[1:]):
            dt = b[0] - a[0]
            out.append({"t": b[0], "v": (b[idx] - a[idx]) / dt if dt > 0 else 0})
        return out

    # -- loop -------------------------------------------------------------
    def run(self):
        while True:
            started = time.time()
            try:
                cluster_id, brokers, topics = self.collect_cluster()
                groups = self.collect_groups(topics)
                self.diff_groups(groups)
                self.record_history(started, topics, groups)
                snap = {
                    "ok": True, "at": started, "cluster_id": cluster_id, "bootstrap": BOOTSTRAP,
                    "brokers": brokers, "topics": topics, "groups": groups,
                    "produced_rate": {t: self.rates(h) for t, h in self.topic_history.items()},
                    "consumed_rate": {g: self.rates(h) for g, h in self.group_history.items()},
                    "lag_history": {g: [{"t": p[0], "v": p[2]} for p in h] for g, h in self.group_history.items()},
                    "events": list(self.events),
                    "collect_ms": round((time.time() - started) * 1000),
                }
            except Exception as e:  # broker down, timeouts: keep serving the last good snapshot with the error
                snap = {**self.snapshot, "ok": False, "error": f"{type(e).__name__}: {e}", "at": started}
            with lock:
                self.snapshot = snap
            time.sleep(max(0.0, 1.0 - (time.time() - started)))


class Tailer(threading.Thread):
    """Reads the newest messages from every user topic for the live feed. Never commits."""

    def __init__(self):
        super().__init__(daemon=True)
        self.buffers = {}
        self.assigned = set()

    def run(self):
        consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"inspector-tail-{uuid.uuid4().hex[:6]}",
                             "enable.auto.commit": False, "enable.partition.eof": False})
        last_check = 0
        while True:
            if time.time() - last_check > 3:
                last_check = time.time()
                try:
                    md = consumer.list_topics(timeout=5)
                    wanted = {(t, p) for t, tm in md.topics.items() if not t.startswith(INTERNAL_PREFIX) for p in tm.partitions}
                    if wanted != self.assigned:
                        consumer.assign([TopicPartition(t, p, OFFSET_END) for t, p in sorted(wanted)])
                        self.assigned = wanted
                except Exception as e:
                    print("tailer:", e, flush=True)
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            raw = msg.value()
            try:
                value = json.loads(raw)
            except Exception:
                value = raw.decode("utf-8", "replace") if raw else None
            rec = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                   "key": msg.key().decode("utf-8", "replace") if msg.key() else None,
                   "timestamp": msg.timestamp()[1] / 1000, "value": value}
            with lock:
                self.buffers.setdefault(msg.topic(), deque(maxlen=TAIL_PER_TOPIC)).appendleft(rec)

    def tail(self, topic, limit):
        with lock:
            return list(self.buffers.get(topic, []))[:limit]


collector = Collector()
tailer = Tailer()

app = FastAPI(title="Kafka Lab - Inspector")


@app.middleware("http")
async def no_cache(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.on_event("startup")
def startup():
    collector.start()
    tailer.start()


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/snapshot")
def snapshot():
    with lock:
        return collector.snapshot


@app.get("/api/messages")
def messages(topic: str, limit: int = 50):
    return tailer.tail(topic, min(limit, TAIL_PER_TOPIC))


@app.get("/metrics")
def metrics():
    """Prometheus text format: lag per group/partition plus a rebalance counter."""
    from fastapi.responses import PlainTextResponse
    with lock:
        s = collector.snapshot
        events = list(collector.events)
    lines = ["# TYPE inspector_group_lag gauge", "# TYPE inspector_group_members gauge"]
    for g in s.get("groups", []):
        lines.append(f'inspector_group_members{{group="{g["id"]}"}} {len(g["members"])}')
        for p in g["partitions"]:
            if p["lag"] is not None:
                lines.append(f'inspector_group_lag{{group="{g["id"]}",topic="{p["topic"]}",partition="{p["partition"]}"}} {p["lag"]}')
    lines.append("# TYPE inspector_rebalance_events_total counter")
    counts = {}
    for e in events:
        if e["kind"] in ("join", "leave", "assign", "state"):
            counts[e["group"]] = counts.get(e["group"], 0) + 1
    for g, n in counts.items():
        lines.append(f'inspector_rebalance_events_total{{group="{g}"}} {n}')
    return PlainTextResponse("\n".join(lines) + "\n")
