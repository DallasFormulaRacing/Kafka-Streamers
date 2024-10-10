import json
import time
import random
from confluent_kafka import Producer
from datetime import datetime

kafka_config = {
    'bootstrap.servers': '',
    'client.id': '',
    'acks': 'all',
}

producer = Producer(kafka_config)

topic = 'linpot-data'

def generate_message():
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    linpot_data = {
        "data": {
            "name": "linpots",
            "fields": {
                "Front Left": round(random.uniform(1.5, 2.0), 6),
                "Front Right": round(random.uniform(1.5, 2.0), 6),
                "Rear Left": round(random.uniform(1.5, 2.0), 6),
                "Rear Right": round(random.uniform(1.5, 2.0), 6),
            },
        },
        "type": "data",
        "tags": {
            "car": "IC-24",
            "host": "raspberrypi",
            "session_id": "92",
            "source": "linpot",
        },
        "timestamp": int(time.time()),
    }
    
    return json.dumps(linpot_data)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_messages(batch_size=10, interval=5):
    messages_sent = 0
    try:
        while messages_sent < batch_size:
            message = generate_message()
            producer.produce(topic, value=message, callback=delivery_report)
            producer.poll(0)  # Trigger delivery reports
            
            messages_sent += 1
            print(f"Sent message {messages_sent}/{batch_size}")
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print('Interrupted by user')
    finally:
        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
        print('Flushing producer...')
        producer.flush()

if __name__ == '__main__':
    print("Starting Kafka Producer...")
    send_messages(batch_size=10, interval=2)  # 10 messages, 2 seconds apart
    print("Producer finished.")