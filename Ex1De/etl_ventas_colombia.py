# =====================================================================
# EXAMEN ETL - VENTAS COLOMBIA (PySpark)
# Archivo de entrada: ventas_colombia_sucio.csv (misma carpeta)
# Ejecutar desde la carpeta Ex1De:  python etl_ventas_colombia.py
# Pasos: 1) leer sucio  2) limpiar segun reglas  3) responder las 10 preguntas
# =====================================================================
import os

# ---------------------------------------------------------------------
# ARREGLO PARA WINDOWS: en algunas maquinas Spark falla al arrancar con
# "Unable to establish loopback connection". Esto lo soluciona.
# Debe ir ANTES de crear la sesion de Spark.
# ---------------------------------------------------------------------
if os.name == "nt":
    os.makedirs("C:/Temp", exist_ok=True)
    os.environ["JAVA_TOOL_OPTIONS"] = "-Djdk.net.unixdomain.tmpdir=C:/Temp"

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("ETL_ventas_colombia") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # oculta mensajes de aviso para ver solo resultados

# =====================================================================
# 1. EXTRACT: LEER EL CSV SUCIO
# Todo se lee como texto (sin inferSchema) para convertir nosotros con cuidado.
# utf-8 para que se vean bien las tildes.
# =====================================================================
ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ventas_colombia_sucio.csv")
raw = spark.read.csv(ruta, header=True, inferSchema=False, encoding="utf-8")
print("Registros originales:", raw.count())


# =====================================================================
# 2. TRANSFORM: LIMPIEZA
# =====================================================================

# ---------------------------------------------------------------------
# 2.1 Quitar espacios y convertir textos vacios "" en NULL (para poder detectarlos)
# ---------------------------------------------------------------------
df = raw
for c in df.columns:
    df = df.withColumn(c, when(trim(col(c)) == "", None).otherwise(trim(col(c))))

# ---------------------------------------------------------------------
# 2.2 Convertir tipos. try_cast / try_to_timestamp NO fallan con basura:
# devuelven NULL (ej: la fecha "not_a_date" queda NULL).
# ---------------------------------------------------------------------
df = df \
    .withColumn("order_date", expr("try_to_timestamp(order_date)")) \
    .withColumn("quantity", expr("try_cast(quantity as double)")) \
    .withColumn("unit_price_cop", expr("try_cast(unit_price_cop as double)")) \
    .withColumn("discount", expr("try_cast(discount as double)")) \
    .withColumn("returned_qty", expr("try_cast(returned_qty as double)")) \
    .withColumn("customer_age", expr("try_cast(customer_age as int)")) \
    .withColumn("shipping_days", expr("try_cast(shipping_days as int)"))

# ---------------------------------------------------------------------
# 2.3 NORMALIZAR TEXTOS (solo variaciones de escritura, NO se inventan datos):
# " bogota " / "MEDELLIN" / "Laptop Pro14" / "Audifonos Bluetooth" -> nombre oficial.
# clave = minusculas + sin tildes + sin espacios, y se busca en un diccionario.
# ---------------------------------------------------------------------
def clave(c):
    sin_tildes = translate(lower(c), "áéíóúü", "aeiouu")
    return regexp_replace(sin_tildes, r"\s+", "")

def normalizar(nombre_col, oficiales):
    dic = {}
    for oficial in oficiales:
        k = oficial.lower()
        for a, b in zip("áéíóúü", "aeiouu"):
            k = k.replace(a, b)
        dic[k.replace(" ", "")] = oficial
    mapa = create_map(*[x for kv in dic.items() for x in (lit(kv[0]), lit(kv[1]))])
    # si la clave no esta en el diccionario, se deja el texto como estaba
    return coalesce(mapa[clave(col(nombre_col))], col(nombre_col))

ciudades = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Santa Marta",
            "Manizales", "Bucaramanga", "Pereira", "Pasto"]
productos = ["Laptop Pro 14", "Smartphone X", "Cámara Web HD", "Monitor 24", "Disco SSD 1TB",
             "Tablet 10", "Parlante Smart", "Smartwatch Fit", "Teclado Mecánico",
             "Audífonos Bluetooth", "Impresora WiFi", "Mouse Inalámbrico"]

df = df.withColumn("city", normalizar("city", ciudades)) \
       .withColumn("product", normalizar("product", productos))

# ---------------------------------------------------------------------
# 2.4 REGLAS DE CALIDAD: los registros que no cumplen se ELIMINAN
# ---------------------------------------------------------------------
limpio = df.filter(
    col("order_id").isNotNull() &                                   # order_id no nulo
    col("order_date").isNotNull() & (year("order_date") == 2025) &  # fecha valida y del 2025
    col("city").isNotNull() & col("product").isNotNull() &          # ciudad y producto no nulos
    (col("quantity") > 0) & (col("quantity") <= 20) &               # 0 < cantidad <= 20
    (col("unit_price_cop") > 0) & (col("unit_price_cop") < 10000000) &  # 0 < precio < 10.000.000
    col("customer_age").between(18, 100) &                          # edad entre 18 y 100
    col("shipping_days").between(1, 15) &                           # dias de envio entre 1 y 15
    (col("returned_qty") >= 0) & (col("returned_qty") <= col("quantity"))  # devueltas entre 0 y quantity
)

# Un solo registro por pedido (si order_id se repite, se queda con uno).
limpio = limpio.dropDuplicates(["order_id"])

# Columna de apoyo: valor de la venta = unidades x precio unitario (sin descuento).
# (Con esta formula el promedio de P6 da 1.518.048, igual a la opcion A del examen.)
limpio = limpio.withColumn("valor_venta", col("quantity") * col("unit_price_cop"))

limpio.createOrReplaceTempView("ventas")  # tabla SQL llamada "ventas"
print("Registros después de limpiar:", limpio.count())


# =====================================================================
# 3. RESPUESTAS (todas solo sobre pedidos con status = 'Entregado')
# =====================================================================
entregados = limpio.filter(col("status") == "Entregado")
entregados.createOrReplaceTempView("entregados")
print("Pedidos entregados:", entregados.count())

print("\n--- P1. Producto con mayor cantidad de unidades vendidas ---")
spark.sql("""
    SELECT product, SUM(quantity) AS unidades
    FROM entregados GROUP BY product ORDER BY unidades DESC
""").show(5, truncate=False)

print("--- P2. Promedio de unidades devueltas por pedido (status = Devuelto) ---")
limpio.filter(col("status") == "Devuelto").agg(
    round(avg("returned_qty"), 2).alias("promedio_devueltas")).show()

print("--- P3. Ciudad con mayor número de pedidos entregados ---")
spark.sql("""
    SELECT city, COUNT(*) AS pedidos
    FROM entregados GROUP BY city ORDER BY pedidos DESC
""").show(5, truncate=False)

print("--- P4. Categoría con mayores ingresos ---")
spark.sql("""
    SELECT category, ROUND(SUM(valor_venta)) AS ingresos
    FROM entregados GROUP BY category ORDER BY ingresos DESC
""").show(5, truncate=False)

print("--- P5. Canal de venta con más pedidos entregados ---")
spark.sql("""
    SELECT channel, COUNT(*) AS pedidos
    FROM entregados GROUP BY channel ORDER BY pedidos DESC
""").show(5, truncate=False)

print("--- P6. Promedio del valor de venta por pedido entregado (COP) ---")
entregados.agg(round(avg("valor_venta")).alias("promedio_valor_venta")).show()

print("--- P7. Departamento con mayor cantidad de unidades vendidas ---")
spark.sql("""
    SELECT department, SUM(quantity) AS unidades
    FROM entregados GROUP BY department ORDER BY unidades DESC
""").show(5, truncate=False)

print("--- P8. Método de pago más usado en pedidos entregados ---")
spark.sql("""
    SELECT payment_method, COUNT(*) AS pedidos
    FROM entregados GROUP BY payment_method ORDER BY pedidos DESC
""").show(5, truncate=False)

print("--- P9. Promedio de días de envío (pedidos entregados) ---")
entregados.agg(round(avg("shipping_days"), 2).alias("promedio_dias_envio")).show()

print("--- P10. Tipo de cliente con mayores ingresos ---")
spark.sql("""
    SELECT customer_type, ROUND(SUM(valor_venta)) AS ingresos
    FROM entregados GROUP BY customer_type ORDER BY ingresos DESC
""").show(5, truncate=False)
