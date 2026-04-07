import dlt
import kagglehub
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
import shutil
import os
from pyspark import pipelines as dp


 
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

@dp.table(
    name="bcomm_olist_orders",
)
def bronze_orders():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_orders_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_order_items",
)
def bronze_order_items():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_order_items_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_customers",
)
def bronze_customers():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_customers_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_products",
)
def bronze_products():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_products_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_sellers",
)
def bronze_sellers():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_sellers_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_order_reviews",
)
def bronze_order_reviews():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_order_reviews_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_order_payments",
)
def bronze_order_payments():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_order_payments_dataset.csv")
    )
 
 
@dp.table(
    name="bcomm_olist_geolocation",
)
def bronze_geolocation():
    return spark.read.option("header", True).csv(
        kaggle_path("olist_geolocation_dataset.csv")
    )

@dp.table(
     name="bcomm_product_category_name_translation"
)
def bronze_product_category_name_translation():
     return spark.read.option("header", True).csv(
         kaggle_path("product_category_name_translation.csv")
     )
 