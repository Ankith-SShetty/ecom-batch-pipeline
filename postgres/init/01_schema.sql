CREATE TABLE IF NOT EXISTS raw_transactions (
    id                SERIAL PRIMARY KEY,
    order_id          VARCHAR(50),
    customer_id       VARCHAR(50),
    order_status      VARCHAR(30),
    order_purchase_ts TIMESTAMP,
    order_approved_ts TIMESTAMP,
    order_delivered_ts TIMESTAMP,
    order_estimated_ts TIMESTAMP,
    product_id        VARCHAR(50),
    product_category  VARCHAR(100),
    price             NUMERIC(10,2),
    freight_value     NUMERIC(10,2),
    payment_type      VARCHAR(30),
    payment_value     NUMERIC(10,2),
    review_score      INTEGER,
    seller_state      VARCHAR(10),
    customer_state    VARCHAR(10),
    ingested_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feature_set_quarterly (
    id                      SERIAL PRIMARY KEY,
    quarter                 VARCHAR(10),
    product_category        VARCHAR(100),
    total_orders            INTEGER,
    total_revenue           NUMERIC(12,2),
    avg_order_value         NUMERIC(10,2),
    avg_freight_value       NUMERIC(10,2),
    avg_delivery_days       NUMERIC(6,2),
    avg_review_score        NUMERIC(4,2),
    return_rate             NUMERIC(5,4),
    top_payment_type        VARCHAR(30),
    top_seller_state        VARCHAR(10),
    top_customer_state      VARCHAR(10),
    processed_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_order_id ON raw_transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_category ON raw_transactions(product_category);
CREATE INDEX IF NOT EXISTS idx_raw_purchase_ts ON raw_transactions(order_purchase_ts);
CREATE INDEX IF NOT EXISTS idx_feature_quarter ON feature_set_quarterly(quarter);
