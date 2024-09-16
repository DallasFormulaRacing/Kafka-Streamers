import json
import time
import random
from confluent_kafka import Producer
from datetime import datetime

# kafka config: adjust based on our setup
kafka_config = {
    'bootstrap.servers': '',
    'client.id': '',
}

producer = Producer(kafka_config)

# kafka topic
topic = 'linpot-data'

# generate message with mock values (for testing)
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
    
    return f'[{current_time}] Data: {json.dumps(linpot_data, indent=4)}'

# delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# send message chunks
def send_messages(batch_size=10, interval=5):
    try:
        for _ in range(batch_size):
            message = generate_message()
            producer.produce(topic, value=message, callback=delivery_report)
            producer.poll(0)
            time.sleep(interval)  # wait for specified interval between messages
    except KeyboardInterrupt:
        print('Interrupted by user')
    finally:
        producer.flush()

if __name__ == '__main__':
    send_messages(batch_size=10, interval=2)  # 10 messages, 2 seconds apart
