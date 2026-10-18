"""Order processor: consumes `orders`, does the work, publishes lifecycle events to `order-status`.

Run several copies with the same --group to watch Kafka split partitions
between them. Run one with a different --group to see a second, independent
reader of the same topic.

    python -m services.processor.worker --name worker-a
    python -m services.processor.worker --name worker-b --delay 0.5

Failure handling:
  * transient failure  -> retried in place with backoff (--retries)
  * permanent failure  -> sent to the dead-letter topic `orders.dlq`
  * --crash-after N    -> process exits without committing, so a restart
                          re-reads the uncommitted messages (at-least-once)
"""
import argparse
import json
import os
import random
import signal
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer

from services.common import BOOTSTRAP, DLQ_TOPIC, ORDERS_TOPIC, STATUS_TOPIC


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--name", default=f"worker-{uuid.uuid4().hex[:4]}", help="instance name (shows up as client.id)")
    p.add_argument("--group", default="order-processor", help="consumer group id")
    p.add_argument("--delay", type=float, default=0.2, help="seconds of simulated work per order")
    p.add_argument("--fail-rate", type=float, default=0.05, help="fraction of orders that hit an error")
    p.add_argument("--retries", type=int, default=3, help="retries for transient errors before giving up")
    p.add_argument("--crash-after", type=int, default=0, help="process N orders without committing any, then exit")
    p.add_argument("--parent-pid", type=int, default=0, help="exit when this process is gone (used by the shop UI)")
    p.add_argument("--session-timeout-ms", type=int, default=45000,
                   help="how long the broker waits without heartbeats before declaring this worker dead")
    return p.parse_args()


def do_work(order, delay, fail_rate):
    """Simulated business logic. Raises transient or permanent errors at random."""
    time.sleep(delay)
    if random.random() < fail_rate:
        if random.random() < 0.7:
            raise TransientError("inventory service timeout")
        raise PermanentError(random.choice(["payment declined", "unknown item"]))


class Worker:
    def __init__(self, args):
        self.args = args
        self.running = True
        self.consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": args.group,
            "client.id": args.name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after the work is done
            "session.timeout.ms": args.session_timeout_ms,
            "heartbeat.interval.ms": max(1000, args.session_timeout_ms // 3),
        })
        self.producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": args.name})

    def log(self, text):
        try:
            print(f"[{self.args.name}] {text}", flush=True)
        except OSError:
            # stdout was a pipe to the shop UI and the shop is gone. Logging
            # must never stop us from leaving the group cleanly.
            pass

    def emit(self, msg, order, status, **extra):
        """Publish one lifecycle event for an order to the status topic."""
        event = {
            "order_id": order["order_id"],
            "customer": order["customer"],
            "status": status,
            "worker": self.args.name,
            "group": self.args.group,
            "partition": msg.partition(),
            "offset": msg.offset(),
            "at": time.time(),
            **extra,
        }
        self.producer.produce(STATUS_TOPIC, key=msg.key(), value=json.dumps(event).encode())
        self.producer.poll(0)

    def on_assign(self, _consumer, partitions):
        self.log(f"assigned partitions {[p.partition for p in partitions]}")

    def on_revoke(self, _consumer, partitions):
        self.log(f"revoked partitions {[p.partition for p in partitions]} (rebalance)")

    def handle(self, msg):
        order = json.loads(msg.value())
        self.emit(msg, order, "consumed")

        attempt = 0
        while True:
            try:
                do_work(order, self.args.delay, self.args.fail_rate)
                self.emit(msg, order, "ready", attempts=attempt + 1)
                return "ready"
            except TransientError as e:
                attempt += 1
                if attempt > self.args.retries:
                    self.dead_letter(msg, order, f"gave up after {attempt} attempts: {e}")
                    return "dlq"
                backoff = 0.2 * (2 ** (attempt - 1))
                self.emit(msg, order, "retrying", attempt=attempt, reason=str(e), backoff=backoff)
                self.log(f"p{msg.partition()}@{msg.offset()} transient '{e}', retry {attempt} in {backoff:.1f}s")
                time.sleep(backoff)
            except PermanentError as e:
                self.dead_letter(msg, order, str(e))
                return "dlq"

    def dead_letter(self, msg, order, reason):
        payload = {**order, "dlq_reason": reason, "source_partition": msg.partition(), "source_offset": msg.offset()}
        self.producer.produce(DLQ_TOPIC, key=msg.key(), value=json.dumps(payload).encode())
        self.emit(msg, order, "dead_lettered", reason=reason)

    def run(self):
        args = self.args
        self.consumer.subscribe([ORDERS_TOPIC], on_assign=self.on_assign, on_revoke=self.on_revoke)
        self.log(f"group={args.group} delay={args.delay}s fail_rate={args.fail_rate} waiting on '{ORDERS_TOPIC}'")

        processed = 0
        try:
            while self.running:
                if args.parent_pid and os.getppid() != args.parent_pid:
                    self.log("parent process gone, leaving the group")
                    break
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        self.log(f"error: {msg.error()}")
                    continue

                outcome = self.handle(msg)
                processed += 1
                key = msg.key().decode() if msg.key() else None
                self.log(f"p{msg.partition()}@{msg.offset()} key={key} -> {outcome}")

                if args.crash_after and processed >= args.crash_after:
                    self.log(f"simulated crash before commit after {processed} orders")
                    sys.exit(1)

                # Stage 4 of the lifecycle. Visible to the inspector as the
                # group's committed offset moving to offset + 1. In crash mode
                # we skip it on purpose so the restart replays everything.
                if not args.crash_after:
                    self.consumer.commit(message=msg, asynchronous=False)
        finally:
            self.producer.flush(5)
            self.consumer.close()  # sends LeaveGroup: the rebalance happens now, not after session.timeout.ms
            self.log("left the group, bye")


def main():
    args = parse_args()
    worker = Worker(args)

    def stop(*_):
        worker.running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    worker.run()


if __name__ == "__main__":
    main()
