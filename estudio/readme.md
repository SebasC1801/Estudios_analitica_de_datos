# Guía de estudio: SQL + PySpark

## Contenido de la carpeta

| Archivo | Qué trae |
|---|---|
| `01_lectura_y_consultas_basicas.py` | Leer CSV, `show`, `printSchema`, `select`, `distinct`, `filter`, `where`, `isin` |
| `02_agregaciones_y_ventanas.py` | `withColumn`, `groupBy` + `agg`, `orderBy`, `Window` + `row_number` |
| `03_joins_y_consultas_ventas.py` | `join` de 2 y 3 tablas, SQL con `spark.sql`, ~30 consultas de ventas |
| `04_limpieza_datos.py` | Nulos, textos vacíos, duplicados, `try_cast`, `trim`, `lower`, `when/otherwise` |
| `datasets/` | CSV usados por los scripts (`clientes`, `productos`, `ventas`, `ventas1`, `tienda_sucia`) |
| `../exam1/consultas.py` | 10 consultas resueltas del taller NASDAQ |

Los scripts se ejecutan **desde la carpeta `estudio/`** (leen `datasets/...` con ruta relativa):

```bash
cd estudio
python 01_lectura_y_consultas_basicas.py
```

Las consultas están comentadas con `#`: quita el `#` de la que quieras probar.

---

## 1. Ambiente e instalación

### Requisito: Java (JDK)

PySpark necesita Java 17 (o 11/21). Revisar si está instalado:

```bash
java -version
```

Si no está: instalar Temurin JDK 17 (https://adoptium.net) y configurar `JAVA_HOME`.
**No se necesita `npm`**: todo se instala con `pip`.

### Crear el ambiente (solo la primera vez)

```bash
python -m venv ambiente
```

### Activar el ambiente (cada vez que abras una terminal)

PowerShell:

```powershell
ambiente\Scripts\Activate.ps1
```

CMD:

```bash
ambiente\Scripts\activate
```

Git Bash:

```bash
source ambiente/Scripts/activate
```

> Si PowerShell bloquea el script: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.
> Para salir del ambiente: `deactivate`.

### Instalar dependencias

```bash
pip install pyspark
pip install py4j
```

(`py4j` es el puente Python ↔ Java; `pyspark` ya lo trae, pero se instala por si acaso.)

Para el proyecto de scraping/ETL (carpeta `scraps/`):

```bash
pip install scrapy pandas
```

Comprobar que todo quedó bien:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## 2. Orden de un SELECT en SQL

```sql
SELECT   columnas              -- qué columnas quiero ver
FROM     tabla1                -- de qué tabla
JOIN     tabla2                -- unir con otra tabla
ON       tabla1.pk = tabla2.fk -- cómo se conectan (ej: clientes.id_cliente = compras.id_cliente)
WHERE    condición             -- filtra FILAS antes de agrupar (ej: precio < 12000)
GROUP BY columna               -- junta filas iguales (ej: GROUP BY id_cliente)
HAVING   condición de grupo    -- filtra GRUPOS después de agrupar (ej: HAVING SUM(cantidad) > 5)
ORDER BY columna [DESC]        -- ordena: vacío/ASC = menor a mayor, DESC = mayor a menor
LIMIT    n                     -- devuelve solo n filas (ej: LIMIT 3)
```

Ejemplo completo:

```sql
SELECT c.nombre, SUM(v.cantidad) AS total
FROM ventas v
JOIN clientes c ON v.id_cliente = c.id_cliente
WHERE v.estado = 'Completada'
GROUP BY c.nombre
HAVING SUM(v.cantidad) > 5
ORDER BY total DESC
LIMIT 3;
```

### Funciones de agregación

| Función | Qué hace |
|---|---|
| `avg()` | promedio |
| `min()` | mínimo |
| `max()` | máximo |
| `sum()` | suma de los valores de una columna |
| `count()` | cuenta cuántos valores hay |

### Operadores lógicos

| Operador | Qué hace | Ejemplo |
|---|---|---|
| `=` | igual a | `estado = 'Completada'` |
| `!=` / `<>` | diferente de | `estado != 'Cancelada'` |
| `IS NULL` | es nulo | `email IS NULL` |
| `IS NOT NULL` | no es nulo | `email IS NOT NULL` |
| `BETWEEN` | entre a y b (incluye los extremos) | `precio BETWEEN 10 AND 20` |
| `NOT BETWEEN` | fuera de ese rango | `precio NOT BETWEEN 10 AND 20` |
| `IN` | está en una lista | `WHERE type IN (1,2,3)` |
| `NOT IN` | no está en la lista | `WHERE type NOT IN (1,2,3)` |
| `LIKE` | busca patrones en texto (`%` = cualquier cosa) | `nombre LIKE 'ed%'` → Edgar, Edwar |

---

## 3. Chuleta rápida de PySpark

| Quiero... | Código |
|---|---|
| Iniciar Spark | `spark = SparkSession.builder.appName("x").getOrCreate()` |
| Leer CSV | `spark.read.csv("ruta.csv", header=True, inferSchema=True)` |
| Usar SQL sobre un DataFrame | `df.createOrReplaceTempView("t")` → `spark.sql("select ... from t")` |
| Ver tabla / tipos | `df.show()` / `df.printSchema()` |
| Elegir columnas | `df.select("a", "b")` |
| Valores sin repetir | `df.select("a").distinct()` |
| Filtrar | `df.filter((df.a == 1) & (df.b > 2))` (o `where`) |
| Valor en lista | `df.a.isin(1, 2, 3)` |
| Columna nueva | `df.withColumn("total", col("a") * col("b"))` |
| Agrupar y calcular | `df.groupBy("a").agg(sum("b").alias("s"))` |
| Ordenar | `df.orderBy(col("a").desc())` |
| Unir tablas | `t1.join(t2, t1.id == t2.id, "inner")` |
| Ventana | `Window.partitionBy("a").orderBy("b")` + `row_number().over(ventana)` |
| Mes de una fecha | `month("fecha")` |
| Texto a número sin error | `expr("try_cast(col as int)")` |
| Quitar espacios / minúsculas | `trim(col("a"))` / `lower(col("a"))` |
| Condicional | `when(cond, "x").otherwise("y")` |
| Contar nulos | `count(when(col(c).isNull(), c))` |
