"""Pub/Sub topics and subscriptions for the changefeed sink (one topic + one subscription per table)."""

from __future__ import annotations

from ..config import CdcConfig


def names(cdc: CdcConfig, table: str) -> tuple[str, str]:
    topic = f"projects/{cdc.pubsub_project}/topics/{cdc.topic_prefix}{table}"
    sub = f"projects/{cdc.pubsub_project}/subscriptions/{cdc.topic_prefix}{table}{cdc.subscription_suffix}"
    return topic, sub


def create(cdc: CdcConfig, tables: list[str], ack_deadline_s: int = 60, retention_days: int = 7, log=print) -> int:
    """Create missing topics/subscriptions. Existing ones are reused. Returns number of failures."""
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import pubsub_v1
    from google.protobuf import duration_pb2

    pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
    failures = 0
    for t in tables:
        topic, subscription = names(cdc, t)
        try:
            try:
                pub.create_topic(name=topic)
                log(f"{t}: created topic {topic}")
            except AlreadyExists:
                log(f"{t}: topic exists {topic}")
            try:
                sub.create_subscription(request={
                    "name": subscription, "topic": topic, "ack_deadline_seconds": ack_deadline_s,
                    "message_retention_duration": duration_pb2.Duration(seconds=retention_days * 86400),
                })
                log(f"{t}: created subscription {subscription}")
            except AlreadyExists:
                log(f"{t}: subscription exists {subscription}")
        except Exception as e:  # noqa: BLE001 - report and continue with the next table
            failures += 1
            log(f"{t}: FAILED {e}")
    return failures


def delete(cdc: CdcConfig, tables: list[str], log=print) -> int:
    from google.api_core.exceptions import NotFound
    from google.cloud import pubsub_v1

    pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
    failures = 0
    for t in tables:
        topic, subscription = names(cdc, t)
        for kind, fn, name in (("subscription", sub.delete_subscription, subscription),
                               ("topic", pub.delete_topic, topic)):
            try:
                fn(request={kind: name})
                log(f"{t}: deleted {kind} {name}")
            except NotFound:
                log(f"{t}: {kind} not found {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                log(f"{t}: FAILED deleting {kind} {name}: {e}")
    return failures


def list_all(cdc: CdcConfig, log=print) -> None:
    from google.cloud import pubsub_v1

    pub = pubsub_v1.PublisherClient()
    for topic in pub.list_topics(request={"project": f"projects/{cdc.pubsub_project}"}):
        if cdc.topic_prefix and f"/topics/{cdc.topic_prefix}" not in topic.name:
            continue
        log(topic.name)
        for s in pub.list_topic_subscriptions(request={"topic": topic.name}):
            log(f"  {s}")
