# Examen ETL - Ventas Colombia (PySpark)

Proceso ETL sobre `ventas_colombia_sucio.csv` y respuestas a las 10 preguntas.

## Archivos

| Archivo | Qué es |
|---|---|
| `ventas_colombia_sucio.csv` | Datos originales con errores de calidad (12.030 filas) |
| `etl_ventas_colombia.py` | Script que limpia los datos y responde las 10 preguntas |

---

## Cómo ejecutarlo (paso a paso)

Requisito: Java 17 instalado (`java -version`).

**1. Abrir una terminal en la carpeta del proyecto** (`DataAnalitycs`).

**2. Activar el ambiente** (si no existe: `python -m venv ambiente`):

```powershell
ambiente\Scripts\Activate.ps1
```

**3. Instalar PySpark** (solo si no está instalado):

```bash
pip install pyspark py4j
```

**4. Entrar a la carpeta y ejecutar:**

```bash
cd Ex1De
python etl_ventas_colombia.py
```

Imprime los registros antes y después de limpiar y, debajo, la tabla de cada pregunta (P1 a P10). **La respuesta es la primera fila de cada tabla.**

> Si Spark falla al arrancar con `Unable to establish loopback connection`, el script ya trae el arreglo
> (crea `C:/Temp` y define `JAVA_TOOL_OPTIONS`). Si aun así falla, cierra y abre la terminal.

---

## Qué hace la limpieza (reglas del examen)

No se imputan ni inventan valores: lo que incumple una regla **se elimina**; los textos con solo variación de formato **se normalizan**.

| Paso | Qué se hace |
|---|---|
| Espacios y vacíos | `trim` a todo; los textos vacíos pasan a `NULL` |
| Tipos | `try_cast` a número y `try_to_timestamp` a fecha (la basura como `not_a_date` queda `NULL`, sin romper el programa) |
| Normalizar `city` y `product` | `" bogota "`, `MEDELLIN`, `Laptop Pro14`, `Audifonos Bluetooth`, `mouse inalambrico`... → nombre oficial (`Bogotá`, `Medellín`, `Laptop Pro 14`, `Audífonos Bluetooth`...) |
| `order_id` | no nulo y un solo registro por pedido (`dropDuplicates`) |
| `order_date` | convertible a timestamp y del año 2025 |
| `city`, `product` | no nulos |
| `quantity` | `> 0` y `<= 20` |
| `unit_price_cop` | `> 0` y `< 10.000.000` |
| `customer_age` | entre 18 y 100 |
| `shipping_days` | entre 1 y 15 |
| `returned_qty` | entre 0 y `quantity` |

Resultado: **12.030 → 11.758** registros limpios; **9.152** con estado `Entregado`.

`valor_venta = quantity * unit_price_cop` (sin descuento). Con esa fórmula P6 da exactamente 1.518.048, igual a la opción A; con descuento no coincide con ninguna opción.

---

## Respuestas obtenidas

| # | Pregunta | Resultado | Opción |
|---|---|---|---|
| 1 | Producto con más unidades vendidas (entregados) | Smartphone X (2.245) | **B** |
| 2 | Promedio de unidades devueltas por pedido (estado Devuelto) | 1.70 | **B** |
| 3 | Ciudad con más pedidos entregados | Bogotá (2.271) | **A** |
| 4 | Categoría con mayores ingresos | Computadores | **C** |
| 5 | Canal con más pedidos entregados | Web (2.347) | **B** |
| 6 | Promedio del valor de venta por pedido entregado | 1.518.048 COP | **A** |
| 7 | Departamento con más unidades vendidas | Cundinamarca (3.821) | **D** |
| 8 | Método de pago más usado | Contraentrega (2.395) | **A** |
| 9 | Promedio de días de envío | 3.12 | **A** |
| 10 | Tipo de cliente con mayores ingresos | Retail (4.816 mil millones) | **C** |

### Notas / dudas

- **P2:** opciones A 1.76, B 1.70, C 1.02, D 1.38. El resultado es 1.70, que coincide exactamente con la **B**.
- **P6:** opciones A 1.518.048, B 245.000, C 525.000, D 385.000. Sin descuento da 1.518.048 (**A**); con descuento daria 1.418.791, que no coincide con ninguna.
- **P10:** Retail y Pyme están muy cerca (4.816 vs 4.793 mil millones). La respuesta depende de aplicar bien la limpieza, así que vale revisar esa pregunta.
- **P1, P3, P7:** los nombres de ciudad/producto salen bien gracias a la normalización (sin ella, `bogota`, `Laptop Pro14`, etc. se contarían aparte).
