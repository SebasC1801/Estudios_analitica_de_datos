# =====================================================================
# 04 - LIMPIEZA DE DATOS (nulos, tipos, espacios, mayusculas, duplicados)
# Dataset: datasets/tienda_sucia.csv (trae errores a proposito)
# Como usarlo: descomenta (quita el #) las revisiones que quieras ver.
# Ejecutar desde la carpeta estudio/:  python 04_limpieza_datos.py
# Orden recomendado: 1) revisar -> 2) arreglar -> 3) verificar
# =====================================================================
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("tiendaBuilder") \
    .master("local[*]") \
    .getOrCreate()

td = spark.read.csv("datasets/tienda_sucia.csv", header=True, inferSchema=True)
td.createOrReplaceTempView("td")


# =====================================================================
# PARTE 1: REVISAR (detectar que esta sucio)
# =====================================================================

# Ver todas las filas sin repetir (40 filas).
# td.select("id_venta", "fecha", "nombre_cliente", "id_cliente", "ciudad", "categoria", "producto", "cantidad", "precio_unitario", "metodo_pago", "estado", "email").distinct().show(40)

# Muestra el tipo de cada columna. Si una columna de numeros sale como "string", esta contaminada.
# td.printSchema()

# Estadisticas: cantidad, promedio, desviacion, minimo y maximo.
# td.describe().show()

# CONTAR NULOS por columna.
#   [ ... for c in td.columns] -> repite lo mismo para cada columna (es Python puro)
#   when(col(c).isNull(), c)   -> si la celda es nula, la cuenta
#   alias(c)                   -> deja el nombre original de la columna (si no, Spark inventa uno feo)
# td.select([
#     count(when(col(c).isNull(), c)).alias(c)
#     for c in td.columns
# ]).show()

# CONTAR TEXTOS VACIOS por columna (ej: "   "). trim quita los espacios y luego compara con "".
# td.select([
#     count(when(trim(col(c)) == "", c)).alias(c)
#     for c in td.columns
# ]).show()

# VER QUE VALORES HAY en una columna con su cantidad de repeticiones.
# Mejor que distinct cuando hay muchos valores. truncate=False evita que corte el texto.
# td.groupBy("ciudad").count().orderBy(col("count").desc()).show(50, truncate=False)
# td.groupBy("categoria").count().show()
# td.groupBy("metodo_pago").count().show()
# td.groupBy("producto").count().show(50, truncate=False)

# DUPLICADOS (version 1): agrupa por TODAS las columnas y muestra las que aparecen mas de una vez.
# OJO: id_venta puede diferir aunque la venta sea la misma, por eso no sirve esta version.
# td.groupBy(td.columns).count().filter(col("count") > 1).show(truncate=False)

# DUPLICADOS (version corregida): agrupa por todas las columnas MENOS id_venta.
# td.groupBy(
#     "fecha", "id_cliente", "nombre_cliente", "ciudad", "categoria", "producto",
#     "cantidad", "precio_unitario", "metodo_pago", "estado", "email"
# ).count().filter(col("count") > 1).show(50, truncate=False)


# =====================================================================
# PARTE 2: ARREGLAR
# =====================================================================

# ---------------------------------------------------------------------
# 2.1 CANTIDAD: texto basura -> numero entero
# ---------------------------------------------------------------------

# Revisar: try_cast convierte a entero y si no puede (texto, "NA") deja NULL en vez de fallar.
# Se muestran los invalidos: nulos o menores/iguales a 0.
# td.withColumn("cantidad_num", expr("try_cast(cantidad as int)")) \
#     .filter(col("cantidad_num").isNull() | (col("cantidad_num") <= 0)) \
#     .select("id_venta", "cantidad", "cantidad_num").show(50)

# Arreglar: reescribe la columna cantidad ya como entero.
td = td.withColumn("cantidad", expr("try_cast(cantidad as int)"))

# Borrar las filas con cantidad nula o negativa/cero. Quedan solo cantidades validas.
td = td.filter(col("cantidad").isNotNull() & (col("cantidad") > 0))

# Verificar:
# td.select("cantidad").distinct().show()

# ---------------------------------------------------------------------
# 2.2 PRECIO UNITARIO: texto basura -> decimal (double)
# ---------------------------------------------------------------------

# Revisar los precios invalidos (nulos o <= 0).
# td.select("precio_unitario").distinct().show()
# td.withColumn("precio_num", expr("try_cast(precio_unitario as double)")) \
#     .filter(col("precio_num").isNull() | (col("precio_num") <= 0)) \
#     .select("id_venta", "precio_unitario", "precio_num").show(50, truncate=False)

# Arreglar: convierte a decimal.
td = td.withColumn("precio_unitario", expr("try_cast(precio_unitario as double)"))

# Borrar las filas con precio nulo o <= 0.
td = td.filter(col("precio_unitario").isNotNull() & (col("precio_unitario") > 0))

# Verificar:
# td.select("precio_unitario").distinct().show(truncate=False)

# ---------------------------------------------------------------------
# 2.3 TEXTOS: quitar espacios sobrantes (" pasto " -> "pasto")
# trim se aplica a todas las columnas de texto.
# ---------------------------------------------------------------------
td = td.withColumn("nombre_cliente", trim(col("nombre_cliente")))
td = td.withColumn("ciudad", trim(col("ciudad")))
td = td.withColumn("categoria", trim(col("categoria")))
td = td.withColumn("producto", trim(col("producto")))
td = td.withColumn("metodo_pago", trim(col("metodo_pago")))
td = td.withColumn("estado", trim(col("estado")))
td = td.withColumn("email", trim(col("email")))

# ---------------------------------------------------------------------
# 2.4 CIUDAD: unificar escritura (bogota, BOGOTA, Bogotá -> Bogotá)
# ---------------------------------------------------------------------

# Primero todo a minusculas (upper() hace lo contrario: todo a MAYUSCULAS).
td = td.withColumn("ciudad", lower(col("ciudad")))

# Luego pone el nombre correcto: mayuscula inicial y tilde.
#   when(condicion, valor) -> si se cumple, usa ese valor
#   otherwise(...)         -> si no coincide con ninguno, deja el valor como estaba (evita nulos)
td = td.withColumn(
    "ciudad",
    when(col("ciudad").isin("bogota", "bogotá"), "Bogotá")
    .when(col("ciudad").isin("pasto"), "Pasto")
    .when(col("ciudad").isin("cali"), "Cali")
    .when(col("ciudad").isin("medellin", "medellín"), "Medellín")
    .when(col("ciudad").isin("armenia"), "Armenia")
    .when(col("ciudad").isin("manizales"), "Manizales")
    .otherwise(col("ciudad"))
)

# ---------------------------------------------------------------------
# 2.5 CIUDADES NULAS: quitar las filas sin ciudad (nulas o con el texto "NULL")
# ---------------------------------------------------------------------
td = td.filter(col("ciudad").isNotNull() & (col("ciudad") != "NULL"))


# =====================================================================
# PARTE 3: VERIFICAR EL RESULTADO
# =====================================================================

# Ciudades finales con su cantidad (ya deben verse limpias y sin repetir).
td.groupBy("ciudad").count().orderBy(col("ciudad")).show(50, truncate=False)

# Cuantas filas quedaron despues de limpiar.
print("Registros después de limpiar:", td.count())
