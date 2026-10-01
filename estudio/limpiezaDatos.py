from pyspark.sql import *
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

spark = SparkSession.builder\
	.appName("tiendaBuilder")\
	.master("local[*]") \
    .getOrCreate()

td = spark.read.csv("datasets/tienda_sucia.csv",header=True,inferSchema=True)
td.createOrReplaceTempView("td")

#td.select("id_venta","fecha","nombre_cliente","id_cliente","ciudad","categoria","producto","cantidad","precio_unitario","metodo_pago","estado","email").distinct().show(40)
#td.printSchema()#es necesario hacerlo para verificar que tipos de datos hay, si los datos numericos dice string es claro ejemplo de contaminacion 
td.describe()#.show()# nos da la cantidad, media, desviacion estandar, minimos y maximos

#verificar nulos
td.select([#se pone llaves para representar codigo python puro
	count(when(col(c).isNull(),c)).alias(c)#cuenta cuantos datos hay nulos, 
	#y when usa 2 parametros, el primero es si se cumple la condicion de nulo este seguira asi, de no serlo dejara la variable c 
	#tal cual como esta
	# para evitar los nombres tipicos de spark, se le coloca alias y c (nombre de todas las variables tal cual esta)
	# para evitar que se renombre las variables de las columnas
	for c in td.columns # aplicara el cambio a cada columna del dataset
	])#.show()

#datos con espacios vacios ej: "  pasto  "
td.select([
	count(when(trim(col(c))=="",c)#trim se encargara de eliminar los valores vacios elresto sigue la misma logica anterior
	).alias(c)
	for c in td.columns 
	])#.show()

#verificacion de valores de variables
#td.groupBy("ciudad") \.count() \.orderBy(col("count").desc()) \.show(50, truncate=False)

#td.groupBy("categoria").count().show()

#td.groupBy("metodo_pago").count().show()

#td.groupBy("producto").count().show(50, truncate=False)

#valores duplicados
#hay que tener en cuenta que llama todas las columnas y que id_venta significa una sola venta y puede duplicarse
#dado q no todas las ventas pueden ser la misma a pesar de tener valores similares
#td.groupBy(td.columns)\.count()\.filter(col("count") > 1)\.show(truncate=False)#esto es para evitar que ponga limite de caracteres


#correccion teniendo en cuenta que id_venta tiene diferencias
#td.groupBy("fecha","id_cliente","nombre_cliente","ciudad","categoria","producto","cantidad","precio_unitario","metodo_pago","estado","email").count()\.filter(col("count") > 1) \.show(50, truncate=False)


#solucionar errores
#casos de string en variables que deben ser tipo int
td.withColumn(
    "cantidad_num",
    col("cantidad").cast("int")
).filter(
    col("cantidad_num").isNull() |
    (col("cantidad_num") <= 0)
).show(50)

# la siguiente consulta es para verificar valores negativos, para ello se requiere solo tener el tipado int en la variable
#si no tiene int se debe hacer la validacion anterior
#td.filter(col("cantidad").cast("int") <= 0).show(50, truncate=False)