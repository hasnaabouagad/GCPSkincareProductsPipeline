import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, to_date, col
from pyspark.sql.window import Window
from pyspark.sql.types import *
from functools import reduce


#logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#variables
BQ_PROJECT = "GCP_PROJECT_ID"
BQ_DATASET = "BIGQUERY_DATASET"
GCS_STAGING_BUCKET = "GCP_STAGING_BUCKET"

#Paths
PRODUCTS_PATH = "gs://BUCKET_NAME/raw/product_info.csv"
REVIEW_PATHS = [
    "gs://BUCKET_NAME/raw/reviews_0-250.csv",
    "gs://BUCKET_NAME/raw/reviews_250-500.csv",
    "gs://BUCKET_NAME/raw/reviews_500-750.csv",
    "gs://BUCKET_NAME/raw/reviews_750-1250.csv",
    "gs://BUCKET_NAME/raw/reviews_1250-end.csv"
]

#Spark Session
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Dataproc to BigQuery") \
        .getOrCreate()
    return spark

#Products processing
def load_products(spark):
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand_id", IntegerType(), True),
        StructField("brand_name", StringType(), True),
        StructField("loves_count", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("reviews", FloatType(), True),
        StructField("size", StringType(), True),
        StructField("variation_type", StringType(), True),
        StructField("variation_value", StringType(), True),
        StructField("variation_desc", StringType(), True),
        StructField("ingredients", StringType(), True),
        StructField("price_usd", FloatType(), True),
        StructField("value_price_usd", FloatType(), True),
        StructField("sale_price_usd", FloatType(), True),
        StructField("limited_edition", IntegerType(), True),
        StructField("new", IntegerType(), True),
        StructField("online_only", IntegerType(), True),
        StructField("out_of_stock", IntegerType(), True),
        StructField("sephora_exclusive", IntegerType(), True),
        StructField("highlights", StringType(), True),
        StructField("primary_category", StringType(), True),
        StructField("secondary_category", StringType(), True),
        StructField("tertiary_category", StringType(), True),
        StructField("child_count", IntegerType(), True),
        StructField("child_max_price", FloatType(), True),
        StructField("child_min_price", FloatType(), True)
    ])

    df = spark.read \
        .option("header", "true") \
        .schema(products_schema) \
        .csv(PRODUCTS_PATH)

    columns_needed = [
        "product_id", "product_name", "brand_id", "brand_name", "loves_count",
        "rating", "reviews", "size", "variation_type", "variation_value",
        "ingredients", "price_usd", "highlights", "primary_category",
        "secondary_category", "tertiary_category"
    ]

    df = df.select(columns_needed)
    df = df.withColumnRenamed("rating", "average_rating")

    critical_columns = [
        "product_id",
        "product_name",
        "brand_id",
        "brand_name",
        "price_usd",
        "primary_category"
    ]

    df = df.na.drop(subset=critical_columns)

    logger.info(f"Loaded products data with {df.count()} records after dropping nulls.")
    return df

#Reviews processing
def load_reviews(spark):
    reviews_schema = StructType([
        StructField("Unnamed: 0", IntegerType(), True),
        StructField("author_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("is_recommended", FloatType(), True),
        StructField("helpfulness", FloatType(), True),
        StructField("total_feedback_count", IntegerType(), True),
        StructField("total_neg_feedback_count", IntegerType(), True),
        StructField("total_pos_feedback_count", IntegerType(), True),
        StructField("submission_time", StringType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_title", StringType(), True),
        StructField("skin_tone", StringType(), True),
        StructField("eye_color", StringType(), True),
        StructField("skin_type", StringType(), True),
        StructField("hair_color", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("price_usd", FloatType(), True)
    ])

    df_list = [
        spark.read.csv(p, header=True, schema=reviews_schema)
        for p in REVIEW_PATHS
    ]

    df = reduce(lambda df1, df2: df1.unionByName(df2), df_list)

    df = df.drop("Unnamed: 0")

    
    df = df.na.drop(subset=['author_id', 'rating', 'submission_time', 'product_id'])

    
    df = df.withColumn("submission_time", to_date(col("submission_time"), "yyyy-MM-dd"))

    
    window_spec = Window.orderBy("submission_time")
    df = df.withColumn("review_id", row_number().over(window_spec))

    logger.info(f"Loaded reviews data with {df.count()} records after dropping nulls and adding review_id.")
    return df

#Writing processed data to BQ
def write_to_bigquery(df, table_name):
    full_table_name = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    logger.info(f"Writing data to BigQuery table: {full_table_name}")
    df.write \
      .format("bigquery") \
      .option("table", full_table_name) \
      .option("temporaryGcsBucket", GCS_STAGING_BUCKET) \
      .mode("append") \
      .save()
    logger.info(f"Finished writing table {full_table_name}")

def main():
    try:
        spark = create_spark_session()

        df_products = load_products(spark)
        df_reviews = load_reviews(spark)

        write_to_bigquery(df_products, "products")
        write_to_bigquery(df_reviews, "reviews")

        spark.stop()
        logger.info("Spark job completed successfully.")

    except Exception as e:
        logger.error(f"Error in Spark job: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
