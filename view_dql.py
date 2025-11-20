from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import os
from datetime import datetime
import time

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
DLQ_TOPIC = 'orders-dlq'

# Load Avro schema
schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'order.avsc')
if not os.path.exists(schema_path):
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schemas', 'order.avsc')

with open(schema_path, 'r') as f:
    schema_str = f.read()

# Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro Deserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str,
    lambda order, ctx: order
)

# Consumer configuration - start from latest for monitoring
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'dlq-monitor-' + str(int(time.time())),  # Unique group
    'auto.offset.reset': 'latest',  # Only new messages
    'enable.auto.commit': True
}

consumer = Consumer(consumer_conf)

def monitor_dlq():
    """Monitor DLQ for new messages in real-time"""
    consumer.subscribe([DLQ_TOPIC])
    
    print("=" * 80)
    print(f"🔍 REAL-TIME DLQ MONITOR - Watching topic: {DLQ_TOPIC}")
    print("=" * 80)
    print("Waiting for failed messages... (Press Ctrl+C to stop)\n")
    
    message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Error: {msg.error()}")
                continue
            
            # New message arrived in DLQ!
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(DLQ_TOPIC, MessageField.VALUE)
                )
                
                message_count += 1
                timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000.0)
                current_time = datetime.now()
                
                print("\n" + "🚨" * 40)
                print(f"⚠️  NEW FAILURE DETECTED! (#{message_count})")
                print("🚨" * 40)
                print(f"  🕐 Detected at:   {current_time.strftime('%H:%M:%S')}")
                print(f"  📅 Failed at:     {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  🆔 Order ID:      {order['orderId']}")
                print(f"  📦 Product:       {order['product']}")
                print(f"  💰 Price:         ${order['price']}")
                print(f"  ❌ Status:        PERMANENTLY FAILED (Max retries exceeded)")
                print(f"  📍 Location:      Partition {msg.partition()}, Offset {msg.offset()}")
                print("🚨" * 40 + "\n")
                
            except Exception as e:
                print(f"❌ Failed to deserialize DLQ message: {e}\n")
    
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Monitor stopped. Total failures detected: {message_count}")
    
    finally:
        consumer.close()

if __name__ == "__main__":
    print("Starting DLQ monitor...")
    print("This will show new messages as they arrive in the DLQ.\n")
    monitor_dlq()