Extracción, transformación y almacenamiento de datos  
ETL  
1. Objetivo 
Desarrollar un proceso en Python utilizando Scrapy que permita obtener información desde 
diferentes fuentes web, transformarla, almacenarla en una base de datos relacional y realizar 
consultas para analizar la información. 
El ejercicio integra las etapas fundamentales de un proceso ETL: Extract, Transform and 
Load, un enfoque utilizado para llevar datos desde distintas fuentes hacia estructuras 
preparadas para análisis.  
2. Situación propuesta 
Una organización desea construir un catálogo de productos a partir de información 
disponible en diferentes sitios web. 
Para el ejercicio se proporcionan dos fuentes: 
• Una fuente relacionada con productos tecnológicos. 
https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops   
• Una fuente relacionada con libros.  
https://books.toscrape.com/  
Construir un proceso que permita: 
Etapa 1: Exploración de las fuentes 
Para cada sitio, identificar: 
• URL.  
• Tipo de información disponible.  
• Estructura de la página.  
• Elementos que pueden ser extraídos.  
• Información común entre las fuentes.  
• Información particular de cada fuente.  
• Existencia de varias páginas.  
• Posibles problemas de calidad de los datos.  
Preguntas 
1. ¿Qué información se puede obtener?  
2. ¿Qué información es común entre las dos fuentes?  
Etapa 2: Extracción 
Crear un programa que utilice Scrapy para obtener los datos. 
El programa deberá: 
1. Conectarse a la fuente.  
2. Descargar la información.  
3. Analizar la estructura HTML.  
4. Identificar los elementos requeridos.  
5. Almacenar los resultados.  
6. Crear un DataFrame.  
7. Mostrar los primeros registros.  
Etapa 3: Transformación 
Una vez obtenidos los datos, realizar un proceso de limpieza. 
Por ejemplo: 
• Eliminar espacios innecesarios.  
• Convertir precios a valores numéricos.  
• Convertir calificaciones a números.  
• Normalizar nombres.  
• Identificar valores faltantes.  
• Eliminar duplicados.  
• Estandarizar nombres de columnas.  
• Validar tipos de datos.  
El objetivo es obtener información preparada para almacenarse y analizarse. La 
transformación ETL normalmente incluye limpieza, validación, estructuración y 
eliminación de inconsistencias.  
Etapa 4: Diseño de la base de datos 
Diseñar una base de datos que permita almacenar información procedente de diferentes 
fuentes. 
No es necesario crear una tabla independiente para cada página. Se debe analizar primero 
qué información puede ser común y qué información es específica. 
Una propuesta general sería: 
FUENTES 
PRODUCTOS 
TECNOLOGÍA                         
Tablas sugeridas 
Fuentes 
• id_fuente 
• nombre 
• url 
Productos 
• id_producto 
• id_fuente 
• tipo 
LIBROS 
CATEGORÍAS 
• nombre 
• precio 
• calificacion 
• url 
Tecnológia 
• id_producto 
• descripcion 
Libros 
• id_producto 
• id_categoria 
• disponibilidad 
• otros_datos 
Categorías 
• id_categoria 
• nombre 
La idea es utilizar llaves primarias y foráneas para establecer las relaciones, implementa la 
base de datos y gestiona productos de diferentes fuentes de datos 
Consultas sql 
1. Mostrar el producto junto con la fuente. 
2. Mostrar información relacionada con categorías. 
3. ¿Cuántos productos existen de cada tipo? 
4. ¿Cuál es el producto más costoso? 
5. ¿Cuál es el producto más económico? 
6. ¿Cuál es el precio promedio? 
7. ¿Cuál es la calificación promedio? 
8. ¿Qué fuente proporciona más productos? 
Entregables. 
Código del desarrollo, un pdf con el diagrama de la base de datos propuesto, así mismo 
las  respuestas a las consultas realizadas.