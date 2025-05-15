import pyspark
from delta import *
from pyspark.sql import functions as sf
from delta.tables import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

table = "./tmp/delta-table"
spark = configure_spark_with_delta_pip(builder).getOrCreate()


data = spark.range(0, 5).withColumn("date_partition", sf.date_add(sf.current_date(), 1))
data.write.mode("append").partitionBy("date_partition").format("delta").save(table)


df = spark.read.format("delta").load(table)

df.show()

deltaTable = DeltaTable.forPath(spark, table)
deltaTable.delete("id < 2")


df2 = spark.read.format("parquet").load(
    "./delta-table/_delta_log/00000000000000000010.checkpoint.parquet"
)

df2.show(n=10, truncate=False, vertical=True)
