from confluent_kafka import Consumer
import psycopg2, json, threading, os
from fastapi import FastAPI, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI()

FRAUD_COUNTER = Counter('fraud_alerts_total', 'Total fraud alerts triggered')
SCORE_LATENCY = Histogram('scoring_latency_seconds', 'Time to score a transaction')

def rule_based_score(txn: dict) -> float:
    score = 0.0
    if txn['amount'] > 10000:
        score += 0.4
    if txn['is_high_risk_country']:
        score += 0.35
    if txn['velocity'] > 20:
        score += 0.25
    return min(score, 1.0)

def consume_and_score():
    consumer = Consumer({
        'bootstrap.servers': os.getenv(
            'KAFKA_BOOTSTRAP',
            'finsignal-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092'
        ),
        'group.id': 'scorer-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['txn-events'])
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres-postgresql.finsignal.svc.cluster.local'),
        database=os.getenv('POSTGRES_DB', 'alerts'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cur = conn.cursor()

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        txn = json.loads(msg.value())
        with SCORE_LATENCY.time():
            score = rule_based_score(txn)
        if score >= 0.6:
            FRAUD_COUNTER.inc()
            cur.execute(
                "INSERT INTO alerts (txn_id, score, timestamp) VALUES (%s, %s, NOW())",
                (txn['txn_id'], score)
            )
            conn.commit()

threading.Thread(target=consume_and_score, daemon=True).start()

@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

@app.get("/health")
def health():
    return {"status": "ok"}
