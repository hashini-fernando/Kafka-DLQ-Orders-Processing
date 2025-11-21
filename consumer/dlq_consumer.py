from confluent_kafka import Consumer
import json

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-dlq-consumer",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe(["orders-dlq"])

print("DLQ Consumer Started... Press Ctrl + C to stop")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("[Consumer Error]:", msg.error())
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))

            print("\n[DLQ Message]")
            print("  Reason:", payload.get("reason"))
            print("  Failed Record:", payload.get("failedRecord"))
            print("  Timestamp:", payload.get("timestamp"))
            print("  Partition:", msg.partition())
            print("  Offset:", msg.offset())
            print("-" * 60)

        except Exception as e:
            print("[Parsing Error] Failed to decode DLQ message:", e)

except KeyboardInterrupt:
    print("\nStopping DLQ Consumer...")

finally:
    consumer.close()
