from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark import pipelines as dp


CATALOG   = "dbr_dev"
BRONZE_DB = "tokariev_bronze"
SILVER_DB = "tokariev_silver"

@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.table(
    name=f"{SILVER_DB}.silver_orders",
    comment="Cleansed orders with proper timestamps and status",
    table_properties={"quality": "silver"},
)
def silver_orders():
    return (
        dp.read("bcomm_olist_orders")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
        .filter(F.col("order_status").isNotNull())
    )
 
 
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("positive_price", "price > 0")
@dp.table(
    name=f"{SILVER_DB}.silver_order_items",
    comment="Cleansed order items with numeric types",
    table_properties={"quality": "silver"},
)
def silver_order_items():
    return (
        dp.read("bcomm_olist_order_items")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
    )
 
 
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.table(
    name=f"{SILVER_DB}.silver_customers",
    comment="Cleansed customers with normalised state codes",
    table_properties={"quality": "silver"},
)
def silver_customers():
    return (
        dp.read("bcomm_olist_customers")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
    )
 
 
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.table(
    name=f"{SILVER_DB}.silver_products",
    comment="Cleansed products with numeric dimensions",
    table_properties={"quality": "silver"},
)
def silver_products():
    return (
        dp.read("bcomm_olist_products")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("product_weight_g", F.col("product_weight_g").cast(DoubleType()))
        .withColumn("product_length_cm", F.col("product_length_cm").cast(DoubleType()))
        .withColumn("product_height_cm", F.col("product_height_cm").cast(DoubleType()))
        .withColumn("product_width_cm", F.col("product_width_cm").cast(DoubleType()))
        .withColumn("product_photos_qty", F.col("product_photos_qty").cast(IntegerType()))
        .withColumn("product_name_lenght", F.col("product_name_lenght").cast(IntegerType()))
        .withColumn("product_description_lenght", F.col("product_description_lenght").cast(IntegerType()))
    )
 
 
@dp.expect_or_drop("valid_review_score", "review_score BETWEEN 1 AND 5")
@dp.table(
    name=f"{SILVER_DB}.silver_order_reviews",
    comment="Cleansed reviews with score as integer",
    table_properties={"quality": "silver"},
)
def silver_order_reviews():
    return (
        dp.read("bcomm_olist_order_reviews")
        .filter(
            (F.length(F.col("review_id")) == 32) & (F.length(F.col("order_id")) == 32)
        )
        .withColumn("review_score", F.coalesce(F.col("review_score").cast(IntegerType()), F.lit(None)))
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .filter(F.col("review_score").isNotNull())
        .dropDuplicates()
        .drop("source", "ingestion_date")
    )
 
 
@dp.expect_or_drop("valid_payment_value", "payment_value >= 0")
@dp.table(
    name=f"{SILVER_DB}.silver_order_payments",
    comment="Cleansed payments with numeric values",
    table_properties={"quality": "silver"},
)
def silver_order_payments():
    return (
        dp.read("bcomm_olist_order_payments")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
    )


@dp.table(
    name=f"{SILVER_DB}.silver_geolocation",
    comment="Cleansed geolocation data",
    table_properties={"quality": "silver"},
)
def silver_geolocation():
    return (
        dp.read("bcomm_olist_geolocation")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("geolocation_zip_code_prefix", F.col("geolocation_zip_code_prefix").cast(IntegerType()))
        .withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))
        .withColumn("geolocation_city", F.initcap(F.trim(F.col("geolocation_city"))))
        .withColumn("geolocation_state", F.upper(F.col("geolocation_state")))
    )


@dp.table(
    name=f"{SILVER_DB}.silver_sellers",
    comment="Cleansed sellers data",
    table_properties={"quality": "silver"},
)
def silver_sellers():
    return (
        dp.read("bcomm_olist_sellers")
        .dropDuplicates()
        .drop("source", "ingestion_date")
        .withColumn("seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast(IntegerType()))
        .withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.col("seller_state")))
    )


@dp.table(
    name=f"{SILVER_DB}.silver_product_category_translation",
    comment="Product category translation reference",
    table_properties={"quality": "silver"},
)
def silver_product_category_translation():
    return (
        dp.read("bcomm_product_category_name_translation")
        .dropDuplicates()
        .drop("source", "ingestion_date")
    )

 