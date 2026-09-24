from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()
df = spark.read.csv("nasdaq_etl_taller.csv",header=True,inferSchema=True)

#1
print(df.count())

#2
df.select(median("close")).show(5)

#3
df.select(max("close")).show(5)

#4
df.groupBy("Ticker").agg(
    sum("Volume").alias("volumenTotalNegociado")
).show()

#5
df.agg(sum("Volume")).show()

#6
df.filter((df.Date=="2025-01-15") & (col("Open") < col("Close"))).show()

#7
df.groupBy("Ticker").agg(
    avg("Close").alias("promedioCierre"),
).show()

#8
df = df.withColumn("rango", col("High") - col("Low"))
df.orderBy(col("rango").desc()).show()

#9
df = df.withColumn("variacionPorcentual", ((col("Close") - col("Open"))/col("Close"))*100) 
df.groupBy("Ticker").agg(
    avg("variacionPorcentual").alias("promedioVariacionPorcentual")
).show()

#10
df.select("Ticker","Date","Volume").orderBy(col("volume").desc()).where(df.Date.isin("2025-01-15","2025-01-13")).show(5)