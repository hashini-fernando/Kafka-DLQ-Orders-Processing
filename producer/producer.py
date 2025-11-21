import json
import io
import time
import random
from confluent_kafka import Producer
from fastavro import schemaless_writer

# Load Avro schema
with open("C:/Semester8/sem-8/big_data/schemas/order.avsc", 'r') as f:
    schema = json.load(f)

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_conf)

products = ["Cinnamon Sticks Alba", "Cinnamon C5", "Cinnamon Powder"]
order_id = 1000


def avro_serialize(record: dict) -> bytes:
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record)
    return buffer.getvalue()


def delivery_report(err, msg):
    print("\n[ Delivery Report ]")
    print("-" * 60)
    if err is not None:
        print(f"Status     : FAILED")
        print(f"Error      : {err}")
    else:
        print(f"Status     : SUCCESS")
        print(f"Topic      : {msg.topic()}")
        print(f"Partition  : {msg.partition()}")
        print(f"Offset     : {msg.offset()}")  
    print("-" * 60)


if __name__ == "__main__":
    print("Kafka Avro Producer Running (Press CTRL+C to stop)\n")

    try:
        while True:
            order_id += 1
            record = {
                "orderId": str(order_id),
                "product": random.choice(products),
                "price": round(random.uniform(10, 500), 2)
            }

            encoded_value = avro_serialize(record)

            producer.produce(
                topic="orders",
                value=encoded_value,
                callback=delivery_report
            )

            print("[ Produced New Record ]")
            print("-" * 60)
            print(f"Order ID   : {record['orderId']}")
            print(f"Product    : {record['product']}")
            print(f"Price ($)  : {record['price']}")
            print("-" * 60)

            producer.poll(0)  
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down producer ...")

    finally:
        producer.flush()
        print("All messages flushed. Producer stopped.")
