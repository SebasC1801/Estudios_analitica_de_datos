from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()
df = spark.read.csv("nasdaq_etl_taller.csv",header=True,inferSchema=True)

#1 Cuántos registros contiene el dataset?
df.createOrReplaceTempView("ESTUDIO")
spark.sql(""" SELECT COUNT(*) FROM ESTUDIO""").alias("Cantidad datos").show()

#2 Cuál es el precio promedio de cierre (Close) considerando todas las acciones? 
df.agg(avg("close").alias("promedio cierre")).show(5)

#3 Cuál fue el precio de cierre máximo registrado? 
df.select(max("close")).show(5)

#4 Cuál empresa presentó el mayor volumen total negociado? 
df.groupBy("Ticker").agg(
    sum("Volume").alias("volumenTotalNegociado")
).orderBy(col("volumenTotalNegociado").desc()).show(1)

#5 Cuál fue el volumen total negociado considerando todas las empresas? 
df.agg(sum("Volume").alias("totalVolumenNegociado")).show()

#6 Los registros corresponden a días en los que el precio de cierre fue superior al precio de apertura son 15? 
spark.sql("""Select Count(*) from ESTUDIO where Open < Close""").alias("cantidadDatosO<C").show()

#7 Cuál empresa presentó el mayor precio promedio de cierre? 
df.groupBy("Ticker").agg(
    avg("Close").alias("promedioCierre")
    ).orderBy(col("promedioCierre").desc()).show(1)

#8 El mayor rango diario registrado? Recuerde: Rango = High – Low es 9. 
df = df.withColumn("rango", col("High") - col("Low"))
df.orderBy(col("rango").desc()).show(1)

#9 ¿Cuál empresa presentó el mayor promedio de variación porcentual entre apertura y cierre? 
df = df.withColumn("variacionPorcentual", ((col("Close") - col("Open"))/col("Close"))*100) 
df.groupBy("Ticker").agg(
    avg("variacionPorcentual").alias("promedioVariacionPorcentual")
).orderBy(col("promedioVariacionPorcentual").desc()).show(1)

#10 Cuál fue el registro con mayor volumen individual? 
df.select("Ticker","Date","Volume").orderBy(col("volume").desc()).where(df.Date.isin("2025-01-15","2025-01-13")).show(1)