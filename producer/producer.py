from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import random
import time
import json

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'

# Load Avro schema
with open('C:/Semester8/sem-8/big_data/schemas/order.avsc', 'r') as f:
    schema_str = f.read()

# Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro Serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda order, ctx: order
)

# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'order-producer'
}

producer = Producer(producer_conf)

# Product catalog
PRODUCTS = [
    "Laptop", "Mouse", "Keyboard", "Monitor", 
    "Headphones", "Webcam", "Desk", "Chair"
]

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f' Message delivery failed: {err}')
    else:
        print(f' Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_order(order_id):
    """Generate a random order"""
    return {
        'orderId': str(order_id),
        'product': random.choice(PRODUCTS),
        'price': round(random.uniform(10.0, 1000.0), 2)
    }

def produce_orders(num_orders=20, delay=1):
    """Produce order messages to Kafka"""
    print(f" Starting producer - will send {num_orders} orders\n")
    
    for i in range(1, num_orders + 1):
        try:
            order = generate_order(i)
            
            # Serialize and produce
            producer.produce(
                topic=TOPIC,
                value=avro_serializer(
                    order,
                    SerializationContext(TOPIC, MessageField.VALUE)
                ),
                callback=delivery_report
            )
            
            print(f" Produced Order #{order['orderId']}: {order['product']} - ${order['price']}")
            
            # Poll to handle delivery reports
            producer.poll(0)
            
            time.sleep(delay)
            
        except Exception as e:
            print(f" Error producing message: {e}")
    
    # Wait for all messages to be delivered
    print("\n Flushing remaining messages...")
    producer.flush()
    print("All messages sent successfully!")

if __name__ == "__main__":
    try:
        produce_orders(num_orders=20, delay=2)
    except KeyboardInterrupt:
        print("\n  Producer interrupted by user")
    finally:
        producer.flush()