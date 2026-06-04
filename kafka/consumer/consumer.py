import os
import json
import logging
import psycopg2
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER   = os.getenv('KAFKA_BROKER',    'kafka:9092')
KAFKA_TOPIC    = os.getenv('KAFKA_TOPIC',     'ecom_transactions')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID',  'ecom_consumer_group')
PG_HOST        = os.getenv('POSTGRES_HOST',   'postgres')
PG_PORT        = os.getenv('POSTGRES_PORT',   '5432')
PG_USER        = os.getenv('POSTGRES_USER',   'ecom_user')
PG_PASS        = os.getenv('POSTGRES_PASSWORD','ecom_pass')
PG_DB          = os.getenv('POSTGRES_DB',     'ecom_db')

INSERT_SQL = """
    INSERT INTO raw_transactions (
        order_id, customer_id, order_status,
        order_purchase_ts, order_approved_ts,
        order_delivered_ts, order_estimated_ts,
        product_id, product_category,
        price, freight_value,
        payment_type, payment_value,
        review_score, seller_state, customer_state
    ) VALUES (
        %(order_id)s, %(customer_id)s, %(order_status)s,
        %(order_purchase_ts)s, %(order_approved_ts)s,
        %(order_delivered_ts)s, %(order_estimated_ts)s,
        %(product_id)s, %(product_category)s,
        %(price)s, %(freight_value)s,
        %(payment_type)s, %(payment_value)s,
        %(review_score)s, %(seller_state)s, %(customer_state)s
    )
"""


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB
    )


def safe_value(val, default=None):
    return val if val not in ('', 'nan', None) else default


def consume():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id':          KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([KAFKA_TOPIC])

    conn   = get_pg_conn()
    cursor = conn.cursor()
    count  = 0
    batch  = []
    BATCH_SIZE = 500

    logger.info(f'Consumer started. Listening on topic: {KAFKA_TOPIC}')

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f'Kafka error: {msg.error()}')
                break

            record = json.loads(msg.value().decode('utf-8'))
            batch.append({
                'order_id':          record.get('order_id'),
                'customer_id':       record.get('customer_id'),
                'order_status':      record.get('order_status'),
                'order_purchase_ts': safe_value(record.get('order_purchase_ts')),
                'order_approved_ts': safe_value(record.get('order_approved_ts')),
                'order_delivered_ts':safe_value(record.get('order_delivered_ts')),
                'order_estimated_ts':safe_value(record.get('order_estimated_ts')),
                'product_id':        record.get('product_id'),
                'product_category':  record.get('product_category', 'unknown'),
                'price':             safe_value(record.get('price'), 0),
                'freight_value':     safe_value(record.get('freight_value'), 0),
                'payment_type':      record.get('payment_type'),
                'payment_value':     safe_value(record.get('payment_value'), 0),
                'review_score':      safe_value(record.get('review_score')),
                'seller_state':      record.get('seller_state'),
                'customer_state':    record.get('customer_state'),
            })

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(INSERT_SQL, batch)
                conn.commit()
                count += len(batch)
                logger.info(f'Inserted {count} records so far...')
                batch = []

    except KeyboardInterrupt:
        pass
    finally:
        if batch:
            cursor.executemany(INSERT_SQL, batch)
            conn.commit()
            count += len(batch)
        logger.info(f'Consumer stopped. Total records inserted: {count}')
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == '__main__':
    import time
    logger.info('Waiting 15s for Kafka and Postgres to be ready...')
    time.sleep(15)
    consume()
