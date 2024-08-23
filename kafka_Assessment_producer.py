from confluent_kafka import Producer
import json

# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Initialize the Kafka producer
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

# Produce 10 messages to the kafkaLab topic
for i in range(10):
    producer.produce('kafkaLab', value=json.dumps({'number': i}).encode('utf-8'), callback=delivery_report)
    producer.poll(0)  # Serve delivery reports from previous produce requests

# Ensure all messages are sent
producer.flush()

