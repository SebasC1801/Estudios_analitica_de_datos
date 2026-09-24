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

productos = spark.read.csv(
    "datasets/productos.csv",
    header=True,
    inferSchema=True
)

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
    .join(
        clientes,
        ventas.id_cliente == clientes.id_cliente,
        "left"
    ) \
    .join(
        productos,
        ventas.id_producto == productos.id_producto,
        "left"
    )

#ventas_completas.select("id_venta","nombre","Producto","cantidad","estado").show()

#si se desea crear una columna nueva, es recomendable hacerlos desde la variable que une las tablas
#ventas_completas = ventas_completas.withColumn(
#    "total",
#    col("cantidad") * col("precio")
#)
#ventas_completas.select("id_venta","nombre","Producto","cantidad","precio","total","estado").show()

ventana = Window \
    .partitionBy("id_cliente") \
    .orderBy("fecha") \
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )

ventas_completas.withColumn(
    "total_acumulado",
    sum("total").over(ventana)
).select(
    "id_cliente",
    "fecha",
    "total",
    "total_acumulado"
).show()


