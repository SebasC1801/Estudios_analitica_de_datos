# =====================================================================
# EXAMEN 1 - CONSULTAS SOBRE LA BOLSA NASDAQ (PySpark + SQL)
# Dataset: nasdaq_etl_taller.csv  (columnas: Date, Ticker, Open, High, Low, Close, Volume)
#   Ticker = nombre corto de la empresa | Open = precio al abrir | Close = precio al cerrar
#   High = precio mas alto del dia | Low = precio mas bajo del dia | Volume = acciones negociadas
# Ejecutar desde la carpeta exam1/:  python consultas.py
# =====================================================================
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()

# Lee el CSV (header=True: primera fila = nombres; inferSchema=True: Spark adivina los tipos).
df = spark.read.csv("nasdaq_etl_taller.csv", header=True, inferSchema=True)

# Convierte el dataset en una "tabla SQL" llamada ESTUDIO para poder usar spark.sql(...).
df.createOrReplaceTempView("ESTUDIO")


# ---------------------------------------------------------------------
# 1. ¿Cuántos registros contiene el dataset?
# Cuenta todas las filas de la tabla.
# ---------------------------------------------------------------------
spark.sql(""" SELECT COUNT(*) FROM ESTUDIO""").alias("Cantidad datos").show()


# ---------------------------------------------------------------------
# 2. ¿Cuál es el precio promedio de cierre (Close) de todas las acciones?
# agg(avg(...)) saca el promedio de toda la columna Close.
# ---------------------------------------------------------------------
df.agg(avg("close").alias("promedio cierre")).show(5)


# ---------------------------------------------------------------------
# 3. ¿Cuál fue el precio de cierre máximo registrado?
# max() devuelve el valor mas grande de Close.
# ---------------------------------------------------------------------
df.select(max("close")).show(5)


# ---------------------------------------------------------------------
# 4. ¿Cuál empresa presentó el mayor volumen total negociado?
# Agrupa por empresa, suma su Volume, ordena de mayor a menor y muestra solo la primera.
# ---------------------------------------------------------------------
df.groupBy("Ticker").agg(
    sum("Volume").alias("volumenTotalNegociado")
).orderBy(col("volumenTotalNegociado").desc()).show(1)


# ---------------------------------------------------------------------
# 5. ¿Cuál fue el volumen total negociado considerando todas las empresas?
# Suma todo el Volume de la tabla (sin agrupar).
# ---------------------------------------------------------------------
df.agg(sum("Volume").alias("totalVolumenNegociado")).show()


# ---------------------------------------------------------------------
# 6. ¿Cuántos registros son días en que el cierre fue mayor a la apertura? (¿son 15?)
# Cuenta las filas donde Open < Close (el precio subio en el dia). Version SQL.
# ---------------------------------------------------------------------
spark.sql("""Select Count(*) from ESTUDIO where Open < Close""").alias("cantidadDatosO<C").show()


# ---------------------------------------------------------------------
# 7. ¿Cuál empresa presentó el mayor precio promedio de cierre?
# Promedio de Close por empresa, ordenado de mayor a menor, se queda con la primera.
# ---------------------------------------------------------------------
df.groupBy("Ticker").agg(
    avg("Close").alias("promedioCierre")
).orderBy(col("promedioCierre").desc()).show(1)


# ---------------------------------------------------------------------
# 8. ¿Cuál es el mayor rango diario registrado? (Rango = High - Low)
# Crea la columna "rango" y muestra la fila con el rango mas grande.
# ---------------------------------------------------------------------
df = df.withColumn("rango", col("High") - col("Low"))
df.orderBy(col("rango").desc()).show(1)


# ---------------------------------------------------------------------
# 9. ¿Cuál empresa tiene el mayor promedio de variación porcentual entre apertura y cierre?
# Variacion % = (Close - Open) / Close * 100 (cuanto cambio el precio en el dia, en porcentaje).
# Se calcula por fila, luego se promedia por empresa y se muestra la mayor.
# ---------------------------------------------------------------------
df = df.withColumn("variacionPorcentual", ((col("Close") - col("Open")) / col("Close")) * 100)
df.groupBy("Ticker").agg(
    avg("variacionPorcentual").alias("promedioVariacionPorcentual")
).orderBy(col("promedioVariacionPorcentual").desc()).show(1)


# ---------------------------------------------------------------------
# 10. ¿Cuál fue el registro con mayor volumen individual?
# Ordena por Volume de mayor a menor y muestra el primero.
# El where limita a las fechas 2025-01-15 y 2025-01-13 (opciones que daba la pregunta).
# ---------------------------------------------------------------------
df.select("Ticker", "Date", "Volume").orderBy(col("volume").desc()).where(df.Date.isin("2025-01-15", "2025-01-13")).show(1)
