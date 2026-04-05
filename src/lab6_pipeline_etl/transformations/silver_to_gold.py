import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


CATALOG   = "dbr_dev"
SILVER_DB = "tokariev_silver"
GOLD_DB   = "tokariev_gold"

# DIMENSION TABLES

@dlt.table(
    name=f"{GOLD_DB}.dim_customers",
    comment="Customer dimension with geographic information",
    table_properties={"quality": "gold"},
)
def dim_customers():
    return (
        dlt.read(f"{CATALOG}.{SILVER_DB}.silver_customers")
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        )
        .dropDuplicates(["customer_id"])
    )


@dlt.table(
    name=f"{GOLD_DB}.dim_products",
    comment="Product dimension with translated categories and specifications",
    table_properties={"quality": "gold"},
)
def dim_products():
    return (
        dlt.read(f"{CATALOG}.{SILVER_DB}.silver_products")
        .join(
            dlt.read(f"{CATALOG}.{SILVER_DB}.silver_product_category_translation"),
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


@dlt.table(
    name=f"{GOLD_DB}.dim_sellers",
    comment="Seller dimension with geographic information",
    table_properties={"quality": "gold"},
)
def dim_sellers():
    return (
        dlt.read(f"{CATALOG}.{SILVER_DB}.silver_sellers")
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )
        .dropDuplicates(["seller_id"])
    )

# FACT TABLES

@dlt.table(
    name=f"{GOLD_DB}.fact_order_items",
    comment="Fact table: Order items with order details and metrics",
    table_properties={"quality": "gold"},
)
def fact_order_items():
    return (
        dlt.read(f"{CATALOG}.{SILVER_DB}.silver_order_items")
        .join(
            dlt.read(f"{CATALOG}.{SILVER_DB}.silver_orders"),
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

@dlt.table(
    name=f"{GOLD_DB}.gold_bcomm_olist_orders_per_state",
    comment="Analytics: Delivery success rate by customer state",
    table_properties={"quality": "gold"},
)
def gold_orders_per_state():
    return (
        dlt.read(f"{CATALOG}.{GOLD_DB}.fact_order_items")
        .join(
            dlt.read(f"{CATALOG}.{GOLD_DB}.dim_customers"),
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
    )
