import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, quarter, year, avg, count, sum as _sum,
    round as _round, first, when
)

PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_USER = os.getenv('POSTGRES_USER', 'ecom_user')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'ecom_pass')
PG_DB   = os.getenv('POSTGRES_DB',   'ecom_db')
PG_URL  = f'jdbc:postgresql://{PG_HOST}:5432/{PG_DB}'
PG_PROPS = {
    'user':     PG_USER,
    'password': PG_PASS,
    'driver':   'org.postgresql.Driver'
}


def create_spark():
    return (SparkSession.builder
            .appName('EcomQuarterlyBatch')
            .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0')
            .getOrCreate())


def read_raw(spark: SparkSession):
    df = spark.read.jdbc(url=PG_URL, table='raw_transactions', properties=PG_PROPS)
    print(f'Raw records loaded: {df.count()}')
    return df


def clean(df):
    df = df.dropDuplicates(['order_id', 'product_id'])
    df = df.filter(col('price') > 0)
    df = df.filter(col('order_purchase_ts').isNotNull())
    df = df.withColumn('price',         col('price').cast('double'))
    df = df.withColumn('freight_value', col('freight_value').cast('double'))
    df = df.withColumn('payment_value', col('payment_value').cast('double'))
    df = df.withColumn('review_score',  col('review_score').cast('integer'))
    df = df.withColumn('order_purchase_ts', col('order_purchase_ts').cast('timestamp'))
    df = df.withColumn('order_delivered_ts', col('order_delivered_ts').cast('timestamp'))
    return df


def add_time_features(df):
    df = df.withColumn('yr',  year(col('order_purchase_ts')))
    df = df.withColumn('qtr', quarter(col('order_purchase_ts')))
    df = df.withColumn('quarter_label', F.concat_ws('-Q', col('yr'), col('qtr')))
    df = df.withColumn(
        'delivery_days',
        when(
            col('order_delivered_ts').isNotNull() & col('order_purchase_ts').isNotNull(),
            F.datediff(col('order_delivered_ts'), col('order_purchase_ts'))
        ).otherwise(None)
    )
    df = df.withColumn(
        'is_cancelled',
        when(col('order_status') == 'canceled', 1).otherwise(0)
    )
    return df


def aggregate(df):
    feature_df = (df
        .groupBy('quarter_label', 'product_category')
        .agg(
            count('order_id').alias('total_orders'),
            _round(_sum('price'), 2).alias('total_revenue'),
            _round(avg('price'), 2).alias('avg_order_value'),
            _round(avg('freight_value'), 2).alias('avg_freight_value'),
            _round(avg('delivery_days'), 2).alias('avg_delivery_days'),
            _round(avg('review_score'), 2).alias('avg_review_score'),
            _round(avg('is_cancelled'), 4).alias('return_rate'),
            first('payment_type').alias('top_payment_type'),
            first('seller_state').alias('top_seller_state'),
            first('customer_state').alias('top_customer_state'),
        )
        .withColumnRenamed('quarter_label', 'quarter')
    )
    print(f'Aggregated feature rows: {feature_df.count()}')
    return feature_df


def write_features(feature_df):
    (feature_df.write
     .jdbc(url=PG_URL, table='feature_set_quarterly', mode='append', properties=PG_PROPS))
    print('Feature set written to PostgreSQL successfully.')


if __name__ == '__main__':
    spark = create_spark()
    raw   = read_raw(spark)
    clean_df    = clean(raw)
    enriched_df = add_time_features(clean_df)
    feature_df  = aggregate(enriched_df)
    write_features(feature_df)
    spark.stop()
    print('Spark job completed.')
