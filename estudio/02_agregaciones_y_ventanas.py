# =====================================================================
# 02 - COLUMNAS NUEVAS, AGRUPAR (groupBy), ORDENAR Y VENTANAS (Window)
# Dataset: datasets/ventas1.csv
# Como usarlo: descomenta (quita el #) la consulta que quieras probar.
# Ejecutar desde la carpeta estudio/:  python 02_agregaciones_y_ventanas.py
# =====================================================================
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()

df = spark.read.csv("datasets/ventas1.csv", header=True, inferSchema=True)


# =====================================================================
# WITHCOLUMN: CREAR UNA COLUMNA NUEVA
# =====================================================================

# Crea la columna "total" = cantidad x precio. Recibe: nombre nuevo (texto) y la formula.
# La columna solo existe mientras corre el programa (no cambia el CSV).
df = df.withColumn("total", col("cantidad") * col("precio_unitario"))

# Ahora "total" se puede usar en un select como cualquier otra columna.
# df.select("nombre_cliente", "producto", "cantidad", "precio_unitario", "total").show()

# Crea la columna "mes" (numero del 1 al 12) sacandolo de la fecha.
df = df.withColumn("mes", month("fecha"))


# =====================================================================
# GROUPBY + AGG: AGRUPAR Y CALCULAR (sum, avg, count...)
# groupBy junta las filas que comparten un valor; agg calcula algo por cada grupo.
# alias() le pone un nombre bonito a la columna resultado.
# =====================================================================

# Por cada ciudad: suma de ventas, promedio (redondeado a 2 decimales) y cuantas ventas hubo.
# df.groupBy("ciudad").agg(
#     sum("total").alias("ventas_totales"),
#     round(avg("total"), 2).alias("promedioVentas"),
#     count("total").alias("numVentas")
# ).show()

# Lo mismo pero por MES, ordenado de enero a diciembre (estadisticas de ventas por mes).
df.groupBy("mes").agg(
    sum("total").alias("ventas_totales"),
    round(avg("total"), 2).alias("promedioVentas"),
    count("total").alias("numVentas")
).orderBy(col("mes").asc()).show()


# =====================================================================
# ORDERBY: ORDENAR
# asc() = de menor a mayor (o A-Z)   |   desc() = de mayor a menor (o Z-A)
# =====================================================================

# Ordena todas las ventas por total, de la mas barata a la mas cara.
# df.orderBy(col("total").asc()).show()

# Ordena por ciudad (A-Z) y, dentro de cada ciudad, por total de mayor a menor.
# Ademas deja solo 3 ciudades con filter.
# df.orderBy(
#     col("ciudad").asc(),
#     col("total").desc()
# ).filter(df.ciudad.isin("Bogota", "Pasto", "Medellin")).show()


# =====================================================================
# WINDOW: VENTANAS
# groupBy colapsa las filas (una fila por grupo). Window NO: conserva todas las filas
# y calcula cosas "dentro de cada grupo".
#   partitionBy -> separa los datos en grupos (aqui: un grupo por cliente)
#   orderBy     -> ordena dentro de cada grupo (aqui: por mes)
# =====================================================================
ventana = Window \
    .partitionBy("nombre_cliente") \
    .orderBy("mes")

# row_number() numera las filas de cada cliente: 1 = su primera venta, 2 = la segunda...
# Sirve para saber cual fue la primera compra o para eliminar duplicados (quedarse con numero 1).
df.withColumn("numero_venta", row_number().over(ventana))  # .show()
