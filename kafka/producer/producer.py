import os
import json
import time
import logging
from pathlib import Path
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC  = os.getenv('KAFKA_TOPIC',  'ecom_transactions')
DATA_PATH    = os.getenv('DATA_PATH',    '/app/data')

REQUIRED_CSV_FILES = [
    'olist_orders_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_products_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_sellers_dataset.csv',
    'product_category_name_translation.csv',
]


def delivery_report(err, msg):
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def load_and_merge(data_path: str) -> list[dict]:
    import pandas as pd

    data_dir = Path(data_path)
    missing_files = [
        filename for filename in REQUIRED_CSV_FILES
        if not (data_dir / filename).is_file()
    ]
    if missing_files:
        visible_files = sorted(path.name for path in data_dir.glob('*')) if data_dir.exists() else []
        raise FileNotFoundError(
            f'Missing required CSV file(s) in {data_dir}: {", ".join(missing_files)}. '
            f'Files visible there: {visible_files}. '
            'If you are running Docker, start the producer with docker compose so ./data '
            'is mounted to /app/data.'
        )

    orders    = pd.read_csv(data_dir / 'olist_orders_dataset.csv')
    items     = pd.read_csv(data_dir / 'olist_order_items_dataset.csv')
    products  = pd.read_csv(data_dir / 'olist_products_dataset.csv')
    payments  = pd.read_csv(data_dir / 'olist_order_payments_dataset.csv')
    reviews   = pd.read_csv(data_dir / 'olist_order_reviews_dataset.csv')
    customers = pd.read_csv(data_dir / 'olist_customers_dataset.csv')
    sellers   = pd.read_csv(data_dir / 'olist_sellers_dataset.csv')
    category  = pd.read_csv(data_dir / 'product_category_name_translation.csv')

    # Merge step by step
    df = orders.merge(items,     on='order_id',    how='left')
    df = df.merge(products,      on='product_id',  how='left')
    df = df.merge(category,      on='product_category_name', how='left')
    df = df.merge(payments,      on='order_id',    how='left')
    df = df.merge(reviews[['order_id','review_score']], on='order_id', how='left')
    df = df.merge(customers[['customer_id','customer_state']], on='customer_id', how='left')
    df = df.merge(sellers[['seller_id','seller_state']], on='seller_id', how='left')

    df['product_category'] = df.get('product_category_name_english', df.get('product_category_name', 'unknown'))

    cols = [
        'order_id', 'customer_id', 'order_status',
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_customer_date', 'order_estimated_delivery_date',
        'product_id', 'product_category',
        'price', 'freight_value',
        'payment_type', 'payment_value',
        'review_score', 'seller_state', 'customer_state'
    ]
    df = df[[c for c in cols if c in df.columns]].fillna('')
    logger.info(f'Loaded {len(df)} merged records')
    return df.to_dict(orient='records')


def produce(records: list[dict]):
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    logger.info(f'Starting producer → topic: {KAFKA_TOPIC}')

    for i, record in enumerate(records):
        msg = {
            'order_id':          record.get('order_id', ''),
            'customer_id':       record.get('customer_id', ''),
            'order_status':      record.get('order_status', ''),
            'order_purchase_ts': record.get('order_purchase_timestamp', ''),
            'order_approved_ts': record.get('order_approved_at', ''),
            'order_delivered_ts':record.get('order_delivered_customer_date', ''),
            'order_estimated_ts':record.get('order_estimated_delivery_date', ''),
            'product_id':        record.get('product_id', ''),
            'product_category':  record.get('product_category', 'unknown'),
            'price':             record.get('price', 0),
            'freight_value':     record.get('freight_value', 0),
            'payment_type':      record.get('payment_type', ''),
            'payment_value':     record.get('payment_value', 0),
            'review_score':      record.get('review_score', 0),
            'seller_state':      record.get('seller_state', ''),
            'customer_state':    record.get('customer_state', ''),
        }
        producer.produce(
            KAFKA_TOPIC,
            key=msg['order_id'],
            value=json.dumps(msg),
            callback=delivery_report
        )
        if i % 10000 == 0:
            producer.poll(0)
            logger.info(f'Produced {i} records...')

    producer.flush()
    logger.info(f'Done. Total records produced: {len(records)}')


if __name__ == '__main__':
    logger.info('Waiting 10s for Kafka to be ready...')
    time.sleep(10)
    records = load_and_merge(DATA_PATH)
    produce(records)
