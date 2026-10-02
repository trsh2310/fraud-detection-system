import json
import logging
import math
import os
import time

import psycopg2
from confluent_kafka import Consumer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")

UPSERT_SCORE = """
    INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO UPDATE SET
        score = EXCLUDED.score,
        fraud_flag = EXCLUDED.fraud_flag,
        scored_at = NOW()
"""


def parse_score(raw):
    data = json.loads(raw)
    transaction_id = data["transaction_id"]
    score = data["score"]
    fraud_flag = data["fraud_flag"]
    if not isinstance(transaction_id, str) or not transaction_id:
        raise ValueError("transaction_id must be a nonempty string")
    if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError("score must be a finite number between 0 and 1")
    if type(fraud_flag) is not int or fraud_flag not in (0, 1):
        raise ValueError("fraud_flag must be 0 or 1")
    return transaction_id, score, fraud_flag


def main():
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": "scores-postgres-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    connection = None
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue
            try:
                score = parse_score(message.value())
            except (ValueError, KeyError, TypeError) as exc:
                logger.error("Invalid score at offset %s: %s", message.offset(), exc)
                consumer.commit(message=message, asynchronous=False)
                continue

            while True:
                try:
                    if connection is None or connection.closed:
                        connection = psycopg2.connect(DATABASE_URL)
                    with connection:
                        with connection.cursor() as cursor:
                            cursor.execute(UPSERT_SCORE, score)
                    # Commit Kafka only after the PostgreSQL transaction succeeds.
                    consumer.commit(message=message, asynchronous=False)
                    break
                except psycopg2.Error:
                    logger.exception("PostgreSQL write failed; retrying score")
                    if connection is not None:
                        connection.close()
                    connection = None
                    time.sleep(3)
    finally:
        if connection is not None:
            connection.close()
        consumer.close()


if __name__ == "__main__":
    main()
