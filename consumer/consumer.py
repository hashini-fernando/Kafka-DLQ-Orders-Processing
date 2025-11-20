from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
import time
import random

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'
DLQ_TOPIC = 'orders-dlq'
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Load Avro schema
with open('C:/Semester8/sem-8/big_data/schemas/order.avsc', 'r') as f:
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

# Avro Serializer for DLQ
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda order, ctx: order
)

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Manual commit for better control
}

consumer = Consumer(consumer_conf)

# Producer for DLQ
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'dlq-producer'
}
dlq_producer = Producer(producer_conf)

# State for running average
class PriceAggregator:
    def __init__(self):
        self.total_price = 0.0
        self.count = 0
        self.running_avg = 0.0
    
    def add_price(self, price):
        self.total_price += price
        self.count += 1
        self.running_avg = self.total_price / self.count
        return self.running_avg
    
    def get_stats(self):
        return {
            'count': self.count,
            'total': round(self.total_price, 2),
            'average': round(self.running_avg, 2)
        }

aggregator = PriceAggregator()

def simulate_processing_failure():
    """Simulate random processing failures (20% chance)"""
    return random.random() < 0.2

def process_order(order, retry_count=0):
    """Process an order with retry logic"""
    
    # Simulate temporary failure
    if simulate_processing_failure() and retry_count < MAX_RETRIES:
        raise Exception("Temporary processing failure (simulated)")
    
    # Simulate permanent failure after max retries
    if retry_count >= MAX_RETRIES:
        raise Exception("Permanent failure - max retries exceeded")
    
    # Successfully process order
    avg_price = aggregator.add_price(order['price'])
    stats = aggregator.get_stats()
    
    print(f"✅ Processed Order #{order['orderId']}: {order['product']} - ${order['price']}")
    print(f"   📊 Running Average: ${stats['average']} (Total: {stats['count']} orders, Sum: ${stats['total']})\n")
    
    return True

def send_to_dlq(order, error_msg):
    """Send failed message to Dead Letter Queue"""
    try:
        dlq_producer.produce(
            topic=DLQ_TOPIC,
            value=avro_serializer(
                order,
                SerializationContext(DLQ_TOPIC, MessageField.VALUE)
            )
        )
        dlq_producer.flush()
        print(f" Sent to DLQ - Order #{order['orderId']}: {error_msg}\n")
    except Exception as e:
        print(f" Failed to send to DLQ: {e}\n")

def consume_orders():
    """Consume and process orders with retry logic"""
    consumer.subscribe([TOPIC])
    print(f"🎧 Consumer started - listening to '{TOPIC}' topic\n")
    print("=" * 70)
    
    retry_queue = {}  # {offset: (order, retry_count)}
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition")
                else:
                    print(f" Consumer error: {msg.error()}")
                continue
            
            # Deserialize message
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC, MessageField.VALUE)
                )
                
                offset = msg.offset()
                retry_count = retry_queue.get(offset, (None, 0))[1]
                
                # Process order
                try:
                    process_order(order, retry_count)
                    
                    # Success - commit offset and remove from retry queue
                    consumer.commit(msg)
                    if offset in retry_queue:
                        del retry_queue[offset]
                
                except Exception as e:
                    retry_count += 1
                    
                    if retry_count < MAX_RETRIES:
                        # Retry logic
                        print(f"⚠️  Order #{order['orderId']} failed (Attempt {retry_count}/{MAX_RETRIES}): {str(e)}")
                        print(f"   🔄 Retrying in {RETRY_DELAY} seconds...\n")
                        
                        retry_queue[offset] = (order, retry_count)
                        time.sleep(RETRY_DELAY)
                        
                        # Re-process
                        try:
                            process_order(order, retry_count)
                            consumer.commit(msg)
                            del retry_queue[offset]
                        except Exception as retry_error:
                            if retry_count >= MAX_RETRIES - 1:
                                # Send to DLQ after max retries
                                send_to_dlq(order, str(retry_error))
                                consumer.commit(msg)
                                del retry_queue[offset]
                    else:
                        # Max retries exceeded - send to DLQ
                        send_to_dlq(order, str(e))
                        consumer.commit(msg)
                        if offset in retry_queue:
                            del retry_queue[offset]
            
            except Exception as e:
                print(f" Deserialization error: {e}\n")
                consumer.commit(msg)
    
    except KeyboardInterrupt:
        print("\n Consumer interrupted by user")
    
    finally:
        print("\n Final Statistics:")
        stats = aggregator.get_stats()
        print(f"   Total Orders Processed: {stats['count']}")
        print(f"   Total Revenue: ${stats['total']}")
        print(f"   Average Order Price: ${stats['average']}")
        consumer.close()
        dlq_producer.flush()

if __name__ == "__main__":
    consume_orders()