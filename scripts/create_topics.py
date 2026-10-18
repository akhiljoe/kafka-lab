"""Create (or widen) the topics the demo needs. Safe to run repeatedly."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

from services.common import BOOTSTRAP, DLQ_TOPIC, ORDERS_TOPIC, PARTITIONS, REPLICATION, STATUS_TOPIC

admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
md = admin.list_topics(timeout=10)
existing = md.topics
rf = min(REPLICATION, len(md.brokers))
print(f"{len(md.brokers)} broker(s) up, replication factor {rf}")

for name, parts in ((ORDERS_TOPIC, PARTITIONS), (STATUS_TOPIC, PARTITIONS), (DLQ_TOPIC, 1)):
    if name not in existing:
        fut = admin.create_topics([NewTopic(name, num_partitions=parts, replication_factor=rf)])
        fut[name].result()
        print(f"created {name} with {parts} partitions, {rf} replicas each")
        continue

    current = len(existing[name].partitions)
    if current < parts:
        fut = admin.create_partitions([NewPartitions(name, parts)])
        fut[name].result()
        print(f"widened {name}: {current} -> {parts} partitions")
    else:
        print(f"{name} already has {current} partitions")
