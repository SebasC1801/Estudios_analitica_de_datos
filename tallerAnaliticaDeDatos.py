from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, max, round, abs, count

spark = SparkSession.builder \
    .appName("Taller_NASDAQ_PySpark") \
    .getOrCreate()

# Carga del dataset
df = spark.read.csv("nasdaq_etl_taller.csv", header=True, inferSchema=True)

# Transformaciones: columnas derivadas usadas en las preguntas 8 y 9
df_transformado = df.withColumn("Rango", col("High") - col("Low")) \
                    .withColumn("Variacion_Pct", abs((col("Close") - col("Open")) / col("Open")) * 100)


# ------------------------------------------------------------------
# Pregunta 1: ¿Cuántos registros contiene el dataset?
# ------------------------------------------------------------------
df.select(count("*").alias("Total_Registros")).show()


# ------------------------------------------------------------------
# Pregunta 2: ¿Cuál es el precio promedio de cierre (Close) considerando
# todas las acciones?
# ------------------------------------------------------------------
df.select(round(avg("Close"), 2).alias("Promedio_Close_General")).show()


# ------------------------------------------------------------------
# Pregunta 3: ¿Cuál fue el precio de cierre máximo registrado?
# ------------------------------------------------------------------
df.select(max("Close").alias("Maximo_Close")).show()


# ------------------------------------------------------------------
# Pregunta 4: ¿Cuál empresa presentó el mayor volumen total negociado?
# (la primera fila del resultado, ordenado descendente, es la respuesta)
# ------------------------------------------------------------------
df.groupBy("Ticker") \
  .agg(sum("Volume").alias("Volumen_Total")) \
  .orderBy(col("Volumen_Total").desc()) \
  .show()


# ------------------------------------------------------------------
# Pregunta 5: ¿Cuál fue el volumen total negociado considerando todas
# las empresas?
# ------------------------------------------------------------------
df.select(sum("Volume").alias("Volumen_Total_General")).show()


# ------------------------------------------------------------------
# Pregunta 6: ¿Los registros con Close > Open son 15? (V/F)
# ------------------------------------------------------------------
df.filter(col("Close") > col("Open")) \
  .select(count("*").alias("Dias_Close_Mayor_Open")) \
  .show()


# ------------------------------------------------------------------
# Pregunta 7: ¿Cuál empresa presentó el mayor promedio de precio de cierre?
# (la primera fila del resultado, ordenado descendente, es la respuesta)
# ------------------------------------------------------------------
df.groupBy("Ticker") \
  .agg(round(avg("Close"), 2).alias("Promedio_Close")) \
  .orderBy(col("Promedio_Close").desc()) \
  .show()


# ------------------------------------------------------------------
# Pregunta 8: ¿El mayor rango diario (High - Low) es 9? (V/F)
# ------------------------------------------------------------------
df_transformado.select(max("Rango").alias("Maximo_Rango_Diario")).show()


# ------------------------------------------------------------------
# Pregunta 9: ¿Cuál empresa presentó el mayor promedio de variación
# porcentual entre apertura y cierre?
# (la primera fila del resultado, ordenado descendente, es la respuesta)
# ------------------------------------------------------------------
df_transformado.groupBy("Ticker") \
  .agg(round(avg("Variacion_Pct"), 2).alias("Promedio_Variacion_Pct")) \
  .orderBy(col("Promedio_Variacion_Pct").desc()) \
  .show()


# ------------------------------------------------------------------
# Pregunta 10: ¿Cuál fue el registro con mayor volumen individual?
# (show(1) trae solo la primera fila, que es la de mayor volumen)
# ------------------------------------------------------------------
df.orderBy(col("Volume").desc()) \
  .select("Ticker", "Date", "Volume") \
  .show(1)

spark.stop()