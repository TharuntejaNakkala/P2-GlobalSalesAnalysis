import subprocess
import sys
try:
    import openpyxl
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, expr
from pyspark.sql.types import DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("Global Sales ETL").getOrCreate()

# Load currency conversion rates
currency_rates = {
  "INR": 1,
  "JPY": 0.57,
  "LKR": 0.26,
  "HKD": 10.93,
  "OMR": 216.63,
  "EUR": 89.34,
  "QAR": 22.89,
  "NOK": 8.21
}

# Broadcast currency rates
dict_broadcast = spark.sparkContext.broadcast(currency_rates)

# UDF to convert to INR
def convert_to_inr_udf(sale, currency):
    rate = dict_broadcast.value.get(currency, 1)
    return round(sale * rate, 2)

convert_udf = udf(convert_to_inr_udf, DoubleType())

# Helper to load and tag dataframe
def load_file(path, fmt, country, currency):
    if fmt == "csv":
        df = spark.read.option("header", True).csv(path)
    elif fmt == "json":
        df = spark.read.option("multiline", True).json(path)
    elif fmt == "excel":
        import pandas as pd
        temp = pd.read_excel(path)
        df = spark.createDataFrame(temp)
    df = df.withColumn("Country", expr(f"'{country}'"))
    df = df.withColumn("Currency", expr(f"'{currency}'"))
    return df

# Load file-based data
japan_df = load_file("gs://narayana123/Japan_Sales.csv", "csv", "Japan", "JPY")
sri_df = load_file("gs://narayana123/SriLanka_Sales.json", "json", "SriLanka", "LKR")
hk_df = load_file("gs://narayana123/HongKong_Sales.xlsx", "excel", "HongKong", "HKD")

# Load SQL Server (India)
india_df = spark.read.format("jdbc").option("url", "jdbc:sqlserver://34.29.209.11:1433;databaseName=india").option("user", "sqlserver").option("password", "12345").option("dbtable", "India_Sales").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") .load().withColumn("Country", expr("'India'")).withColumn("Currency", expr("'INR'"))

# Load Postgres (Norway)
norway_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://34.21.48.176:5432/postgres").option("user", "postgres").option("password", "12345").option("dbtable", "Norway_Sales").load().withColumn("Country", expr("'Norway'")).withColumn("Currency", expr("'NOK'"))

# Load MySQL (Oman, Germany, Qatar)
def load_mysql(country, currency,db="p2project"):
    return spark.read.format("jdbc").option("url", f"jdbc:mysql://34.57.106.30:3306/{db}").option("user", "yagna").option("password", "12345").option("dbtable", f"{country}_Sales").load().withColumn("Country", expr(f"'{country}'")).withColumn("Currency", expr(f"'{currency}'"))

oman_df = load_mysql("Oman", "OMR")
germany_df = load_mysql("Germany", "EUR")
qatar_df = load_mysql("Qatar", "QAR")

# Combine all data
all_df = japan_df.unionByName(sri_df).unionByName(hk_df)#.unionByName(oman_df).unionByName(germany_df).unionByName(qatar_df)#.unionByName(india_df).).unionByName(norway_df)

# Drop nulls
clean_df = all_df.dropna()

# Convert Sale to INR and compute Amount and Tax
clean_df = clean_df.withColumn("Sale_INR", convert_udf(col("Sale"), col("Currency")))
clean_df = clean_df.withColumn("Amount", col("Qty") * col("Sale_INR"))
clean_df = clean_df.withColumn("Tax", col("Amount") * 0.05)

# Final columns
final_df = clean_df.selectExpr(
    "Product as Pname",
    "Category as PCategory",
    "Qty as QtySold",
    "Sale_INR as Price",
    "Amount",
    "Tax",
    "Country"
)

# Save as CSV to GCS
final_df.write.option("header", True).mode("overwrite").csv("gs://narayana123/exports/product_summary")

print("ETL pipeline completed using Dataproc. Output saved to GCS.")
