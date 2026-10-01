from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("datasets/EstudioPySpark") \
    .master("local[*]") \
    .getOrCreate()

clientes = spark.read.csv(
    "datasets/clientes.csv",
    header=True,
    inferSchema=True
)

clientes.createOrReplaceTempView("clientes")
#spark.sql(""" select count(*) as cantidadClientes from clientes """).show()

productos = spark.read.csv(
    "datasets/productos.csv",
    header=True,
    inferSchema=True
)
productos.createOrReplaceTempView("productos")

ventas = spark.read.csv(
    "datasets/ventas.csv",
    header=True,
    inferSchema=True
)

#clientes.show()
#ventas.show()
#productos.show()

# al hacer un join se debe declarar una variable que sea la fusion de las tablas, luego se usa la funcion join(), teniendo como primer
#  valor la variable principal y como parametro la variable de la tabla que se desea unir seguido de los pk == fk y como ultimo 
# parametro el tipo de join en comillas("inner","right","left" )
# ej:
ventasClientes = ventas.join(
    clientes,
    ventas.id_cliente == clientes.id_cliente,
    "inner"
)
#ventasClientes.select("id_venta","nombre","id_Producto").show()

#si se desea unir 2 tablas, se deben separar los joins por /
#ej:

ventas_completas = ventas \
    .join(clientes,ventas.id_cliente == clientes.id_cliente,"inner") \
    .join(productos,ventas.id_producto == productos.id_producto,"inner"
    ).select(ventas.id_venta,
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

ventas_completas.createOrReplaceTempView("ventasCompletas")#
#ventas_completas.select("id_venta","nombre","Producto","cantidad","estado").show()

#si se desea crear una columna nueva, es recomendable hacerlos desde la variable que une las tablas
ventas_completas = ventas_completas.withColumn(
    "total",
    col("cantidad") * col("precio")
)
#ventas_completas.select("id_venta","nombre","Producto","cantidad","precio","total","estado").show()

ventana = Window \
    .partitionBy("producto") \
    .orderBy("categoria") \
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )

ventas_completas.withColumn(
    "total_acumulado",
    sum("cantidad").over(ventana)
).select(
    "producto",
    "categoria",
    "total_acumulado"
)#.show()

ventas_completas.withColumn("mes",month("fecha")).groupBy("mes").agg(avg("total").alias("promediototal")).orderBy(col("promediototal").desc()).show()
#ventas_completas.groupBy("producto").agg(sum("cantidad").alias("cantidadTotal"),first("categoria")).orderBy(col("cantidadTotal").desc()).show()

#spark.sql(""" select nombre, count(*) as cantidadV from ventasCompletas group by nombre order by cantidadV asc """).show(1)

#ventas_completas.groupBy("ciudad","producto").agg(sum("cantidad").alias("cantidadTotal")).orderBy(col("cantidadTotal").desc()).show()

#spark.sql("""select count(*) as cantidadDatos from ventasCompletas""").show()

#spark.sql("""select count(*) as cantidadProductos from productos""").show()

#spark.sql("""select count(*) from ventasCompletas as v where v.estado != "Cancelada" """).alias("totalVentasCompletadas").show()

#productos.agg(round(avg("precio")).alias("precioPromedioProductos")).show()

#productos.select("producto","precio").orderBy(col("precio").desc()).show(1)

#productos.select("producto","precio").orderBy(col("precio").asc()).show(1)

#ventas_completas.agg(sum("cantidad").alias("cantidadUnidadesVendidas")).show()

#ventas_completas.agg(avg("cantidad").alias("promedioUnidadesVenta")).show()

#spark.sql("""select count(*) from ventasCompletas as v where v.estado == "Completada" """).alias("totalVentasCompletadas").show()

#spark.sql("""select count(*) from ventasCompletas as v where v.estado == "Cancelada" """).alias("totalVentasCompletadas").show()

#spark.sql(""" select nombre, count(*) as cantidadVentas from ventasCompletas group by nombre """).show()

#spark.sql(""" select producto,count(*) as cantidadVendido from ventasCompletas group by producto""").show()

#spark.sql(""" select ciudad,count(*) as cantidadVendido from ventasCompletas group by ciudad""").show()

#productos.groupBy("categoria").agg(round(avg("precio")).alias("precioPromedio")).orderBy(col("precioPromedio").desc()).show()

#spark.sql(""" select producto,count(*) as cantidadVendido from ventasCompletas group by producto order by cantidadVendido desc""").show(1)

#spark.sql(""" select nombre,count(*) as cantidadVendido from ventasCompletas group by nombre order by cantidadVendido desc""").show(1)

#spark.sql(""" select ciudad,count(*) as cantidadVendido from ventasCompletas group by ciudad order by cantidadVendido desc""").show(1)

#spark.sql(""" select metodo_pago,count(*) as cantidadVendido from ventasCompletas group by metodo_pago order by cantidadVendido desc""").show(1)

#spark.sql(""" select metodo_pago,count(*) as cantidadVendido from ventasCompletas group by metodo_pago """).show()

#ventas_completas.groupBy("categoria").agg(round(avg("precio")).alias("precioPromedio")).orderBy(col("precioPromedio").desc()).show(1)

#ventas_completas.groupBy("nombre").agg(sum("precio").alias("valorTotal")).show()

#ventas_completas.groupBy("ciudad").agg(sum("precio").alias("valorTotal")).orderBy(col("valorTotal").desc()).show(1)
