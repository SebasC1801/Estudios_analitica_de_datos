#ambiente y dependencias:
--------------------------------------------
	creación de ambiente:

 		Python -m venv venv
--------------------------------------------
	apertura de ambiente:

		venv\scripts\actívate
--------------------------------------------
	instalación de dependencias de pyspark:

		pip install py4j

		pip install pyspark
--------------------------------------------

#consultas SQL puras:

orden de comandos en sql:
---------------------------------------------
SELECT (tabla1.atributos)
FROM (tabla1)
JOIN (tabla2)
ON (condición donde son iguales tabla uno y 2, ej: (pk)clientes.id_cliente = (fk)compras.id_cliente)
WHERE(condiciones especificas como por ejemplo encontrar quien gasto menos de 12000)
GROUP(agrupar los datos obtenidos por las funciones(count, sum, max, min) dependiendo de un solo atributo, ej: group by id agrupar todo lo relacionado a ese único id)
HAVING(filtra grupos después del GROUP BY.Ejemplo:HAVING SUM(cantidad) > 5)
ORDER(ordena de forma ascendente(dejar vacio) o descendente(escribir desc))
LIMIT(limita la cantidad de datos al retornar al final, ej: limit 3 hara que solo salgan 3 entidades en la tabla)

funciones
---------------------------------------------
avg()  |promedio
min()  |minimo
max()  |maximo
sum()  |suma de todos los valores contables de una columna
count()|cuenta el total de todos los valores de una columna

operadores logicos
---------------------------------------------
=           | igual a
!=          | diferente a
<>          | diferente a
is null     | es nulo
is not null | no es nulo
between     | que este entre a y b ej: compras.precio between 10 and 20
not between | que no este entre a y b ej: compras.precio not between 10 and 20
in          | busca varios valores en una lista ej: where type in (1,2,3)
not in      | busca varios valores que sean distintos en una lista ej: where type not in (1,2,3)
like        | busca parecidos en atributos de tipo string ej: where nombre like ar/ respuesta edgar,edwar

