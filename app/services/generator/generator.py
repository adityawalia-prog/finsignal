from faker import Faker
from confluent_kafka import Producer
import json, time, random

fake = Faker()
producer = Producer({'bootstrap.servers': 'finsignal-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092'})

def generate_transaction():
    return {
        "txn_id": fake.uuid4(),
        "account_id": fake.bban(),
        "amount": round(random.uniform(1, 50000), 2),
        "country": fake.country_code(),
        "timestamp": fake.iso8601(),
        "is_high_risk_country": random.random() < 0.1,
        "velocity": random.randint(1, 30)  # txns in last hour
    }

while True:
    txn = generate_transaction()
    producer.produce('txn-events', json.dumps(txn).encode())
    producer.flush()
    print(f"Produced: {txn['txn_id']}")
    time.sleep(0.5)
