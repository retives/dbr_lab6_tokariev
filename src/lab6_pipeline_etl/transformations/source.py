import dlt
import kagglehub
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
import shutil
import os
 

 
CATALOG   = "dbr_dev"
BRONZE_DB = "tokariev_bronze"

 
def kaggle_path(filename: str) -> str:
    """
    Downloads the olist dataset via kagglehub and returns
    the volume path for a given CSV file.
    Requires KAGGLE_USERNAME and KAGGLE_KEY env vars (or ~/.kaggle/kaggle.json).
    """
    # Set kagglehub cache to Unity Catalog volume
    os.environ["KAGGLEHUB_CACHE"] = "/Volumes/dbr_dev/tokariev_bronze/brazilian_ecommerce_volume/"

    dataset_dir = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    return f"{dataset_dir}/{filename}"

@dlt.table(
    name="bcomm_olist_orders",
    comment="Raw orders data ingested from Kaggle olist dataset",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_orders_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_order_items",
    comment="Raw order items data",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_order_items_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_customers",
    comment="Raw customers data",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_customers_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_products",
    comment="Raw products data",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_products_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_sellers",
    comment="Raw sellers data",
    table_properties={"quality": "bronze"},
)
def bronze_sellers():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_sellers_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_order_reviews",
    comment="Raw order reviews data",
    table_properties={"quality": "bronze"},
)
def bronze_order_reviews():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_order_reviews_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_order_payments",
    comment="Raw order payments data",
    table_properties={"quality": "bronze"},
)
def bronze_order_payments():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_order_payments_dataset.csv")
    )
 
 
@dlt.table(
    name="bcomm_olist_geolocation",
    comment="Raw geolocation data",
    table_properties={"quality": "bronze"},
)
def bronze_geolocation():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_geolocation_dataset.csv")
    )
 
 