from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Spark processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.json('hdfs://namenode:9000/bycicles/export.json')

# select the data
scraping_data = df.select('Titles', 'Prices', 'Name', 'N_titles', 'N_prices', 'rin', 'brand')

# Export the dataframe into the Hive table bycicles_data
scraping_data.write.mode("append").insertInto("bycicles_data")