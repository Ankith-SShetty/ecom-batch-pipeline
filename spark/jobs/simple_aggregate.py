import psycopg2

conn = psycopg2.connect(
    host="postgres", port=5432,
    user="ecom_user", password="ecom_pass", dbname="ecom_db"
)
cursor = conn.cursor()

print("Running aggregation...")

cursor.execute("""
    INSERT INTO feature_set_quarterly (
        quarter, product_category, total_orders, total_revenue,
        avg_order_value, avg_freight_value, avg_delivery_days,
        avg_review_score, return_rate, top_payment_type,
        top_seller_state, top_customer_state
    )
    SELECT
        CONCAT(EXTRACT(YEAR FROM order_purchase_ts), '-Q',
               EXTRACT(QUARTER FROM order_purchase_ts)) as quarter,
        product_category,
        COUNT(*) as total_orders,
        ROUND(SUM(price)::numeric, 2) as total_revenue,
        ROUND(AVG(price)::numeric, 2) as avg_order_value,
        ROUND(AVG(freight_value)::numeric, 2) as avg_freight_value,
        ROUND(AVG(EXTRACT(DAY FROM (order_delivered_ts - order_purchase_ts)))::numeric, 2) as avg_delivery_days,
        ROUND(AVG(review_score)::numeric, 2) as avg_review_score,
        ROUND(AVG(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)::numeric, 4) as return_rate,
        MODE() WITHIN GROUP (ORDER BY payment_type) as top_payment_type,
        MODE() WITHIN GROUP (ORDER BY seller_state) as top_seller_state,
        MODE() WITHIN GROUP (ORDER BY customer_state) as top_customer_state
    FROM raw_transactions
    WHERE order_purchase_ts IS NOT NULL
    GROUP BY quarter, product_category
    ORDER BY quarter, total_revenue DESC;
""")

conn.commit()
cursor.execute("SELECT COUNT(*) FROM feature_set_quarterly;")
count = cursor.fetchone()[0]
print(f"Done! Feature rows written: {count}")
cursor.close()
conn.close()