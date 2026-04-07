import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark import pipelines as dp

CATALOG   = "dbr_dev"
SILVER_DB = "tokariev_silver"
GOLD_DB   = "tokariev_gold"

# DIMENSION TABLES

@dp.table(
    name=f"{GOLD_DB}.dim_customers",
    schema="""
        customer_id STRING,
        customer_unique_id STRING MASK dbr_dev.tokariev_gold.pii_mask,
        customer_zip_code_prefix STRING,
        customer_city STRING,
        customer_state STRING
    """
)
def dim_customers():
    return (
        dp.read(f"{CATALOG}.{SILVER_DB}.silver_customers")
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        )
        .dropDuplicates(["customer_id"])
    )


@dp.table(
    name=f"{GOLD_DB}.dim_products",
)
def dim_products():
    return (
        dp.read(f"{CATALOG}.{SILVER_DB}.silver_products")
        .join(
            dp.read(f"{CATALOG}.{SILVER_DB}.silver_product_category_translation"),
            "product_category_name",
            how="left"
        )
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        )
        .dropDuplicates(["product_id"])
    )


@dp.table(
    name=f"{GOLD_DB}.dim_sellers",
    row_filter="ROW FILTER seller_filter ON (seller_id)"
)
def dim_sellers():
    return (
        dp.read(f"{CATALOG}.{SILVER_DB}.silver_sellers")
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )
        .dropDuplicates(["seller_id"])
    )

# FACT TABLES

@dp.table(
    name=f"{GOLD_DB}.fact_order_items",
)
def fact_order_items():
    return (
        dp.read(f"{CATALOG}.{SILVER_DB}.silver_order_items")
        .join(
            dp.read(f"{CATALOG}.{SILVER_DB}.silver_orders"),
            "order_id",
            how="left"
        )
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "shipping_limit_date",
            "price",
            "freight_value"
        )
    )


# ANALYTICS / SUMMARY TABLES

@dp.table(
    name=f"{GOLD_DB}.gold_bcomm_olist_orders_per_state",
)
def gold_orders_per_state():
    return (
        dp.read(f"{CATALOG}.{GOLD_DB}.fact_order_items")
        .join(
            dp.read(f"{CATALOG}.{GOLD_DB}.dim_customers"),
            "customer_id",
            how="left"
        )
        .groupBy("customer_state")
        .agg(
            F.round(
                F.count(F.when(F.col("order_status") == "delivered", True)) / F.count("order_id"),
                2
            ).alias("delivered_percentage")
        )
        .withColumn("iso_state", F.concat(F.lit("BR-"), F.col("customer_state")))
    )

@dp.table(
    name="tokariev_gold.gold_bcomm_olist_product_categories_count"
)
def gold_product_categories_count():
    return (
        dp.read(f"{CATALOG}.{GOLD_DB}.dim_products")
        .groupBy("product_category_name_english")
        .agg(F.count("product_id").alias("product_count"))
    )