import json
import time
import random
import os
from confluent_kafka import Producer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

#Azure Events Hub Config
event_hub_namespace = os.getenv("EVENT_HUB_NAMESPACE")
event_hub_name = os.getenv("EVENT_HUB_NAME")
shared_access_key_name = os.getenv("SHARED_ACCESS_KEY_NAME")
shared_access_key = os.getenv("SHARED_ACCESS_KEY")

#Kafka Config
kafka_config = {
    'bootstrap.servers': f"{event_hub_namespace}:9093",
    'client.id': 'linpot-producer',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': f'$ConnectionString',
    'sasl.password': f'Endpoint=sb://{event_hub_namespace}/;SharedAccessKeyName={shared_access_key_name};SharedAccessKey={shared_access_key};EntityPath={event_hub_name}',
    'acks': 'all',
}

producer = Producer(kafka_config)
topic = event_hub_name

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
        print('Flushing producer...')
        producer.flush()

if __name__ == '__main__':
    print("Starting Kafka Producer for Azure Event Hubs...")
    send_messages(batch_size=10, interval=2)  # 10 messages, 2 seconds apart
    print("Producer finished.")