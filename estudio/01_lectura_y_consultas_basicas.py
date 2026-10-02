# =====================================================================
# 01 - LECTURA Y CONSULTAS BASICAS (select, distinct, filter, where)
# Dataset: datasets/ventas1.csv
# Como usarlo: descomenta (quita el #) la consulta que quieras probar.
# Ejecutar desde la carpeta estudio/:  python 01_lectura_y_consultas_basicas.py
# =====================================================================
from pyspark.sql import *
from pyspark.sql.functions import *  # los imports con * van al final para evitar choques de nombres

# ---------------------------------------------------------------------
# INICIAR SPARK (siempre va primero)
# ---------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()

# ---------------------------------------------------------------------
# LEER EL CSV
# header=True      -> la primera fila son los nombres de las columnas (OJO: es "header", no "headers")
# inferSchema=True -> Spark adivina el tipo de cada columna (numero, texto, fecha)
# ---------------------------------------------------------------------
df = spark.read.csv("datasets/ventas1.csv", header=True, inferSchema=True)


# =====================================================================
# VER LOS DATOS
# =====================================================================

# Muestra la tabla. Con un numero, ej: show(5), muestra solo esas filas.
# df.show()

# Muestra las columnas y el tipo de dato de cada una (texto, entero, decimal...).
# df.printSchema()


# =====================================================================
# SELECT: ESCOGER COLUMNAS
# =====================================================================

# Muestra solo las columnas que pides. Deben escribirse igual que en el CSV.
# df.select("nombre_cliente", "metodo_pago", "producto", "precio_unitario").show()

# Muestra solo la columna ciudad (con repetidos).
# df.select("ciudad").show()

# DISTINCT: muestra cada ciudad una sola vez, sin repetir. Sirve para ver que valores existen.
# df.select("ciudad").distinct().show()


# =====================================================================
# FILTER: QUEDARSE SOLO CON LAS FILAS QUE CUMPLEN UNA CONDICION
# =====================================================================

# Solo las ventas hechas en Pasto.
# df.filter(df.ciudad == "Pasto").show()

# Varias condiciones a la vez: cada una va entre parentesis y se unen con & (y).
# Aqui: Pasto Y cantidad menor a 3 Y pago con tarjeta Y cliente Ana Torres.
# df.filter(
#     (df.ciudad == "Pasto") &
#     (df.cantidad < 3) &
#     (df.metodo_pago == "Tarjeta") &
#     (df.nombre_cliente == "Ana Torres")
# ).show()

# ISIN: la columna puede valer cualquiera de los valores de la lista (es como un "o").
# Aqui: ventas donde la cantidad fue 1, 2 o 3.
# df.filter(df.cantidad.isin(1, 2, 3)).show()

# Se puede mezclar isin con otras condiciones.
# df.filter(
#     (df.ciudad == "Pasto") &
#     (df.cantidad.isin(1, 2, 3)) &
#     (df.metodo_pago == "Tarjeta")
# ).show()


# =====================================================================
# WHERE: IGUAL QUE FILTER (son sinonimos, hacen exactamente lo mismo)
# =====================================================================

# Ventas con precio unitario mayor a 145000.
# df.where(df.precio_unitario > 145000).show()

# Precio > 145000 Y pago con tarjeta Y cantidad 2 o 3.
# df.where(
#     (df.precio_unitario > 145000) &
#     (df.metodo_pago == "Tarjeta") &
#     (df.cantidad.isin(2, 3))
# ).show()
