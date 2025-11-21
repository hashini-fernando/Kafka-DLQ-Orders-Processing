import json
import io
import time
import random

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader


class TemporaryError(Exception):
    pass


class PermanentError(Exception):
    pass


# Load schema
with open("C:/Semester8/sem-8/big_data/schemas/order.avsc", 'r') as f:
    schema = json.load(f)

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-consumer-group",
    "auto.offset.reset": "earliest",
}

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}

consumer = Consumer(consumer_conf)
dlq_producer = Producer(producer_conf)

MAX_RETRIES = 3
count = 0
sum_prices = 0.0


def avro_deserialize(value_bytes: bytes) -> dict:
    buf = io.BytesIO(value_bytes)
    return schemaless_reader(buf, schema)


def send_to_dlq(record: dict, reason: str):
    payload = {
        "failedRecord": record,
        "reason": reason,
        "timestamp": int(time.time())
    }

    dlq_producer.produce(
        topic="orders-dlq",
        value=json.dumps(payload).encode("utf-8")
    )
    dlq_producer.flush()

    print("\n[DLQ] -- Record sent to Dead Letter Queue")
    print("  Reason:", reason)
    print("  Failed Record:", record)
    print("-" * 60)


def process_message(record: dict, metadata: dict):
    global count, sum_prices

    price = record.get("price")
    if price is None or price < 0:
        raise PermanentError(f"Invalid price: {price}")

    # Simulate 10% temporary failure
    if random.random() < 0.1:
        raise TemporaryError("Simulated temporary error")

    count += 1
    sum_prices += price
    avg = sum_prices / count

    print("\n[Message Processed]")
    print("  Record:", record)
    print("  Average Price:", f"{avg:.2f}")
    print("  Partition:", metadata["partition"])
    print("  Offset:", metadata["offset"])
    print("-" * 60)


def handle_message_with_retry(record: dict, metadata: dict):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_message(record, metadata)
            return
        except TemporaryError as e:
            last_err = e
            print(f"[Retry] Attempt {attempt}/{MAX_RETRIES} due to temporary issue -> {e}")
            time.sleep(1)

        except PermanentError as e:
            print("[Error] Permanent issue ->", e)
            send_to_dlq(record, str(e))
            return

    print(f"[Error] Retries exhausted: {last_err}")
    send_to_dlq(record, f"Retries exhausted: {last_err}")


if __name__ == "__main__":
    consumer.subscribe(["orders"])
    print("Consumer started... Press Ctrl + C to stop")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print("[Consumer Error]:", msg.error())
                continue

            metadata = {
                "partition": msg.partition(),
                "offset": msg.offset()
            }

            try:
                record = avro_deserialize(msg.value())
                handle_message_with_retry(record, metadata)

            except Exception as e:
                print("[Unexpected Error] -> Sending raw message to DLQ:", e)
                try:
                    send_to_dlq({"raw_data": str(msg.value())}, str(e))
                except Exception as ee:
                    print("[DLQ Failure] Unable to send failed record:", ee)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        dlq_producer.flush()
