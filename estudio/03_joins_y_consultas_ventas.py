# =====================================================================
# 03 - JOINS (unir tablas) Y CONSULTAS DE VENTAS (PySpark y SQL)
# Datasets: datasets/clientes.csv, productos.csv, ventas.csv
# Como usarlo: descomenta (quita el #) la consulta que quieras probar.
# Ejecutar desde la carpeta estudio/:  python 03_joins_y_consultas_ventas.py
# =====================================================================
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("EstudioPySpark") \
    .master("local[*]") \
    .getOrCreate()

# ---------------------------------------------------------------------
# LEER LAS 3 TABLAS
# createOrReplaceTempView("nombre") las registra como tablas SQL
# para poder usar spark.sql(""" select ... """).
# ---------------------------------------------------------------------
clientes = spark.read.csv("datasets/clientes.csv", header=True, inferSchema=True)
clientes.createOrReplaceTempView("clientes")

productos = spark.read.csv("datasets/productos.csv", header=True, inferSchema=True)
productos.createOrReplaceTempView("productos")

ventas = spark.read.csv("datasets/ventas.csv", header=True, inferSchema=True)

# ver las tablas:
# clientes.show()
# ventas.show()
# productos.show()


# =====================================================================
# JOIN: UNIR DOS TABLAS
# Forma:  tablaPrincipal.join(otraTabla, pk == fk, "tipo")
#   pk == fk -> columna que tienen en comun (ej: id_cliente en ambas)
#   tipo     -> "inner" (solo coincidencias), "left", "right"
# =====================================================================

# Une ventas con clientes: a cada venta le pega los datos de su cliente.
ventasClientes = ventas.join(
    clientes,
    ventas.id_cliente == clientes.id_cliente,
    "inner"
)
# ventasClientes.select("id_venta", "nombre", "id_producto").show()

# Unir 3 tablas: encadena varios .join (con \ al final de cada linea).
# El select final escoge que columnas quedan (indicando de que tabla viene cada una).
ventas_completas = ventas \
    .join(clientes, ventas.id_cliente == clientes.id_cliente, "inner") \
    .join(productos, ventas.id_producto == productos.id_producto, "inner") \
    .select(
        ventas.id_venta,
        ventas.fecha,
        ventas.id_cliente,
        clientes.nombre,
        clientes.ciudad,
        ventas.id_producto,
        productos.producto,
        productos.categoria,
        productos.precio,
        ventas.cantidad,
        ventas.metodo_pago,
        ventas.estado)

# Registra la tabla unida para poder usarla con SQL.
ventas_completas.createOrReplaceTempView("ventasCompletas")
# ventas_completas.select("id_venta", "nombre", "producto", "cantidad", "estado").show()

# Columna nueva "total" = cantidad x precio. Mejor crearla sobre la tabla ya unida.
ventas_completas = ventas_completas.withColumn("total", col("cantidad") * col("precio"))
# ventas_completas.select("id_venta", "nombre", "producto", "cantidad", "precio", "total", "estado").show()


# =====================================================================
# WINDOW CON ACUMULADO
# Suma "corrida": en cada fila suma la cantidad de esa fila + todas las anteriores de su producto.
#   rowsBetween(unboundedPreceding, currentRow) = "desde la primera fila hasta la actual"
# =====================================================================
ventana = Window \
    .partitionBy("producto") \
    .orderBy("categoria") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# ventas_completas.withColumn(
#     "total_acumulado", sum("cantidad").over(ventana)
# ).select("producto", "categoria", "total_acumulado").show()


# =====================================================================
# CONTEOS GENERALES
# =====================================================================

# Cuantos productos hay en el catalogo.
# spark.sql("""select count(*) as cantidadProductos from productos""").show()

# Cuantas ventas hay en total.
# spark.sql("""select count(*) as cantidadDatos from ventasCompletas""").show()

# Ventas que NO fueron canceladas.
# spark.sql("""select count(*) as ventasNoCanceladas from ventasCompletas where estado != "Cancelada" """).show()

# Ventas completadas.
# spark.sql("""select count(*) as ventasCompletadas from ventasCompletas where estado == "Completada" """).show()

# Ventas canceladas.
# spark.sql("""select count(*) as ventasCanceladas from ventasCompletas where estado == "Cancelada" """).show()


# =====================================================================
# PRECIOS DE PRODUCTOS
# =====================================================================

# Precio promedio de todos los productos (redondeado).
# productos.agg(round(avg("precio")).alias("precioPromedioProductos")).show()

# Producto mas caro (show(1) = solo la primera fila).
# productos.select("producto", "precio").orderBy(col("precio").desc()).show(1)

# Producto mas barato.
# productos.select("producto", "precio").orderBy(col("precio").asc()).show(1)

# Precio promedio por categoria, de mayor a menor.
# productos.groupBy("categoria").agg(round(avg("precio")).alias("precioPromedio")).orderBy(col("precioPromedio").desc()).show()

# Categoria con el precio promedio mas alto (usando la tabla unida).
# ventas_completas.groupBy("categoria").agg(round(avg("precio")).alias("precioPromedio")).orderBy(col("precioPromedio").desc()).show(1)


# =====================================================================
# UNIDADES VENDIDAS
# =====================================================================

# Total de unidades vendidas.
# ventas_completas.agg(sum("cantidad").alias("cantidadUnidadesVendidas")).show()

# Promedio de unidades por venta.
# ventas_completas.agg(avg("cantidad").alias("promedioUnidadesVenta")).show()

# Unidades vendidas por producto, del que mas se vendio al que menos (first() trae la categoria del grupo).
# ventas_completas.groupBy("producto").agg(sum("cantidad").alias("cantidadTotal"), first("categoria")).orderBy(col("cantidadTotal").desc()).show()

# Unidades vendidas por ciudad y producto.
# ventas_completas.groupBy("ciudad", "producto").agg(sum("cantidad").alias("cantidadTotal")).orderBy(col("cantidadTotal").desc()).show()


# =====================================================================
# CUANTAS VENTAS HAY POR... (count(*) + group by)
# Cada una tiene su version "TOP 1" (con order by desc + show(1)) para saber cual es el mayor.
# =====================================================================

# Ventas por cliente.
# spark.sql(""" select nombre, count(*) as cantidadVentas from ventasCompletas group by nombre """).show()
# Cliente que mas compras hizo.
# spark.sql(""" select nombre, count(*) as cantidadVendido from ventasCompletas group by nombre order by cantidadVendido desc """).show(1)
# Cliente que menos compras hizo.
# spark.sql(""" select nombre, count(*) as cantidadV from ventasCompletas group by nombre order by cantidadV asc """).show(1)

# Ventas por producto.
# spark.sql(""" select producto, count(*) as cantidadVendido from ventasCompletas group by producto """).show()
# Producto mas vendido.
# spark.sql(""" select producto, count(*) as cantidadVendido from ventasCompletas group by producto order by cantidadVendido desc """).show(1)

# Ventas por ciudad.
# spark.sql(""" select ciudad, count(*) as cantidadVendido from ventasCompletas group by ciudad """).show()
# Ciudad que mas vende.
# spark.sql(""" select ciudad, count(*) as cantidadVendido from ventasCompletas group by ciudad order by cantidadVendido desc """).show(1)

# Ventas por metodo de pago.
# spark.sql(""" select metodo_pago, count(*) as cantidadVendido from ventasCompletas group by metodo_pago """).show()
# Metodo de pago mas usado.
# spark.sql(""" select metodo_pago, count(*) as cantidadVendido from ventasCompletas group by metodo_pago order by cantidadVendido desc """).show(1)


# =====================================================================
# DINERO (suma del precio por grupo)
# =====================================================================

# Valor total por cliente.
# ventas_completas.groupBy("nombre").agg(sum("precio").alias("valorTotal")).show()

# Ciudad con mayor valor total.
# ventas_completas.groupBy("ciudad").agg(sum("precio").alias("valorTotal")).orderBy(col("valorTotal").desc()).show(1)

# Promedio de "total" por mes, del mes que mas vende al que menos (ACTIVA: es la que se ejecuta por defecto).
ventas_completas.withColumn("mes", month("fecha")).groupBy("mes").agg(avg("total").alias("promediototal")).orderBy(col("promediototal").desc()).show()
