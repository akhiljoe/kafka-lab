"""Standalone load generator for thousands of events per second.

The shop UI's generator is convenient but runs inside the web process. For
real load tests use this: one tight producer loop per process, tuned for
throughput, with a report of what the broker actually acknowledged.

    python scripts/loadgen.py --rate 5000 --seconds 30
    python scripts/loadgen.py --total 100000 --key order_id
    python scripts/loadgen.py --rate 20000 --processes 4        # 4 producers in parallel

Watch the inspector (produced msg/s), Grafana (records in) and
`kafka-consumer-groups --describe` while it runs.
"""
import argparse
import json
import multiprocessing
import random
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Producer

from services.common import BOOTSTRAP, CUSTOMERS, MENU, ORDERS_TOPIC


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rate", type=float, default=1000, help="target events per second per process (0 = as fast as possible)")
    p.add_argument("--seconds", type=float, default=20, help="how long to run (ignored when --total is set)")
    p.add_argument("--total", type=int, default=0, help="stop after this many events per process")
    p.add_argument("--key", choices=["customer", "order_id", "none"], default="customer")
    p.add_argument("--processes", type=int, default=1)
    p.add_argument("--acks", default="all", help="0, 1 or all. Fewer acks = faster, less safe")
    p.add_argument("--linger-ms", type=int, default=20)
    p.add_argument("--batch-size", type=int, default=256 * 1024)
    p.add_argument("--compression", default="lz4")
    return p.parse_args()


def make_order():
    item = random.choice(list(MENU))
    qty = random.randint(1, 5)
    customer = random.choice(CUSTOMERS)
    return {"order_id": str(uuid.uuid4()), "customer": customer, "item": item, "quantity": qty,
            "total": round(MENU[item] * qty, 2), "created_at": time.time(), "key_mode": None}


def run(worker_idx, args):
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": f"loadgen-{worker_idx}",
        "acks": args.acks,
        "linger.ms": args.linger_ms,
        "batch.size": args.batch_size,
        "compression.type": args.compression,
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 1_048_576,
    })
    stats = {"acked": 0, "failed": 0}

    def on_delivery(err, _msg):
        stats["failed" if err else "acked"] += 1

    sent = 0
    started = time.time()
    deadline = started + args.seconds
    next_report = started + 1
    while True:
        if args.total and sent >= args.total:
            break
        if not args.total and time.time() >= deadline:
            break
        order = make_order()
        order["key_mode"] = args.key
        key = {"customer": order["customer"], "order_id": order["order_id"], "none": None}[args.key]
        while True:
            try:
                producer.produce(ORDERS_TOPIC, key=key, value=json.dumps(order).encode(), callback=on_delivery)
                break
            except BufferError:          # local queue full: drain callbacks, then retry
                producer.poll(0.01)
        sent += 1
        if sent % 100 == 0:
            producer.poll(0)
        if args.rate > 0:
            expected = started + sent / args.rate
            lag = expected - time.time()
            if lag > 0:
                producer.poll(lag)
        if time.time() >= next_report:
            next_report += 1
            elapsed = time.time() - started
            print(f"[loadgen-{worker_idx}] {elapsed:5.1f}s sent={sent} acked={stats['acked']} failed={stats['failed']} "
                  f"avg={sent / elapsed:,.0f}/s", flush=True)
    producer.flush()
    elapsed = time.time() - started
    print(f"[loadgen-{worker_idx}] done: sent={sent} acked={stats['acked']} failed={stats['failed']} "
          f"in {elapsed:.1f}s = {sent / elapsed:,.0f} events/s", flush=True)


def main():
    args = parse_args()
    if args.processes <= 1:
        run(0, args)
        return
    procs = [multiprocessing.Process(target=run, args=(i, args)) for i in range(args.processes)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == "__main__":
    main()
