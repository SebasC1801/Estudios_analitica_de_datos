from pyspark.sql import *
from pyspark.sql.functions import * # dejar importaciones al final con * para evitar inconvenientes de funcionalidad

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()

#     aqui debe estar la instacioa Spark      
#      V                             V< header no headers    
df =  spark.read.csv("datasets/ventas1.csv",header=True,inferSchema=True)

#df.show() #muestra los valores de la tabla con sus variables, 
           #el valor numerico que se ponga en la funcion es la cantidad de variables que va a retornar
           
#df.printSchema() # muestra solo las variables de la tabla con su tipado del dato

#df.select("nombre_cliente","metodo_pago","producto", "precio_unitario").show()#selecciona la o las variables del dataset para imprimir sus productos, la variable debe estar exactamente escrita como en el csv

#df.select("ciudad").show()
#df.select("ciudad").distinct().show()#verifica todos los tipos de dato que hay de esa variable

#df.filter(df.ciudad == "Pasto").show()#filtra e imprime los datos dependiendo de todos los valores posibles de la variable

#df.filter((df.ciudad == "Pasto") & (df.cantidad < 3) & (df.metodo_pago == "Tarjeta") & (df.nombre_cliente == "Ana Torres")).show()# si se desea filtrar mas de una variable, se deben colorcar los primeros parametros en parentesis y un & de operadores logicos cuando se vaya a añadir otra variable
#df.filter(df.cantidad.isin(1,2,3)).show() #si se desea colocar mas de una condicion en una misma variable se deve usar la funcion isin() con las condiciones de esa variable adentro y divididas por , aparentemente no hay diferencia o limite de parametros
#df.filter((df.ciudad == "Pasto") & (df.cantidad.isin(1,2,3)) & (df.metodo_pago == "Tarjeta")).show()# asi se pueden mezclar las funciones

#df.where(df.precio_unitario > 145000).show()
#df.where((df.precio_unitario > 145000) & (df.metodo_pago == "Tarjeta") & (df.cantidad.isin(2,3))).show()#hasta el momento funciona exactamente igual que filter

df = df.withColumn("total", col("cantidad") * col("precio_unitario"))# genera la columna total, en los parametros se requiere el parametro de la nueva columna en string y seguido sus valores de donde se deriva esta nueva columna
#esta nueva columna solo existe mientras esta ejecutandose el programa
#df.select("nombre_cliente","producto","cantidad","precio_unitario","total").show()#ahora se puede llamar esa columna en un select para ver sus valores

#df.groupBy("ciudad").agg( #crea un filtro por ciudad para usar las siguientes funciones, si se quiere usar sum/count/avg, se debe añadir la funcion agg
#    sum("total").alias("ventas_totales"),#sumatoria total de la variable
#    round(avg("total"),2).alias("promedioVentas"),#promedio
#    count("total").alias("numVentas")#numero o cantidad total
#    ).show()

#df.orderBy(#ordena la columna de mayor a menor o viceversa
#    col("total").asc()#desc es descendente y asc ascendente
#).show()


#df.orderBy(
#    col("ciudad").asc(),#si el valor de la variable es string se ordena alfabeticamente
#    col("total").desc()
#).filter(df.ciudad.isin("Bogota","Pasto","Medellin")).show()

df = df.withColumn("mes",month("fecha"))

df.groupBy("mes").agg( #crea un filtro por ciudad para usar las siguientes funciones, si se quiere usar sum/count/avg, se debe añadir la funcion agg
    sum("total").alias("ventas_totales"),#sumatoria total de la variable
    round(avg("total"),2).alias("promedioVentas"),#promedio
    count("total").alias("numVentas")#numero o cantidad total
    ).orderBy(col("mes").asc()).show()

#funcion de estadisticas de ventas por mes ordenadas por orden menor a mayor 

#groupBy() solo puede ordenar por una variable a la vez, pero window puede definir el orden de varias variables
# particion divide las tablas por esa variable
# y order by las ordena, provocando que se retorne las ventas de cada cliente por mes, cosa que no se puede solo con groupBy  
ventana = Window \
    .partitionBy("nombre_cliente") \
    .orderBy("mes")

df.withColumn(
    "numero_venta",
    row_number().over(ventana)# aqui le estamos diciendo que asigne un valor unico basandose en las 
    #especificaciones de window, en pocas palabras over() sobreescribe la condicion
# row_number() sirve para asignar como una especie de id que ayuda en la eliminacion de valores duplicados, por eso se le asigna una columna
#lo que le estamos dicioendo es que le asigne la primer venta al cliente por mes
).show()


