# Examen ETL - Ventas Colombia (PySpark)

**¿Qué hace esto?** Toma un archivo de ventas con errores (`ventas_colombia_sucio.csv`), lo **limpia** y **responde las 10 preguntas** del examen.

| Archivo | Qué es |
|---|---|
| `ventas_colombia_sucio.csv` | Los datos con errores (12.030 filas) |
| `etl_ventas_colombia.py` | El programa: limpia los datos y contesta las 10 preguntas |
| `README.md` | Esta guía |

---

# PARTE 1 - Instalar lo que se necesita (solo la primera vez)

Si ya lo tienes instalado, salta a la Parte 2. Para comprobarlo, mira el paso 1.3.

### 1.1 Instalar Python
1. Entra a https://www.python.org/downloads/ y descarga Python.
2. Abre el instalador y **marca la casilla "Add python.exe to PATH"** (abajo, muy importante).
3. Dale "Install Now".

### 1.2 Instalar Java (Spark lo necesita para funcionar)
1. Entra a https://adoptium.net y descarga **Temurin 17 (JDK)** para Windows.
2. Instálalo dándole "Siguiente" a todo. En el instalador, activa la opción **"Set JAVA_HOME variable"** si aparece.

### 1.3 Comprobar que quedaron bien
Abre una terminal (mira la Parte 2, paso 2) y escribe:

```bash
python --version
java -version
```

Los dos deben mostrar un número de versión. Si dicen "no se reconoce el comando", cierra la terminal, ábrela otra vez y repite; si sigue igual, reinstala marcando la casilla del PATH.

---

# PARTE 2 - Abrir la carpeta en Sublime Text y la terminal

### Paso 1: abrir la carpeta en Sublime Text
1. Abre **Sublime Text**.
2. Menú **File → Open Folder...**
3. Escoge la carpeta **`DataAnalitycs`** (la grande, no solo `Ex1De`) y dale "Seleccionar carpeta".
4. A la izquierda verás los archivos. Abre `Ex1De` y haz clic en `etl_ventas_colombia.py` para verlo.

### Paso 2: abrir la terminal (elige UNA de estas dos formas)

**Forma A - La más fácil (terminal de Windows):**
1. Abre el **Explorador de archivos** y entra a la carpeta `DataAnalitycs`.
2. Haz clic en la **barra de arriba donde sale la ruta**, borra lo que dice, escribe `powershell` y presiona **Enter**.
3. Se abre una ventana azul/negra. Esa es la terminal y ya está dentro de la carpeta correcta.

**Forma B - Terminal dentro de Sublime Text (con el paquete Terminus):**
1. En Sublime: presiona **Ctrl + Shift + P**.
2. Escribe `Install Package Control` y Enter (si no te aparece, ya lo tienes instalado; sigue al punto 3).
3. Presiona **Ctrl + Shift + P** otra vez, escribe `Package Control: Install Package` y Enter.
4. Escribe `Terminus`, selecciónalo y Enter. Espera a que termine.
5. Presiona **Ctrl + Shift + P**, escribe `Terminus: Open Default Shell in Project Root` y Enter.
6. Se abre una terminal abajo, ya dentro de `DataAnalitycs`.

---

# PARTE 3 - Ejecutar el examen

Escribe estos comandos en la terminal, **uno por uno**, dándole Enter a cada uno.

### Paso 1: entrar al "ambiente" (la caja donde está PySpark instalado)

```powershell
ambiente\Scripts\Activate.ps1
```

Si funciona, verás **(ambiente)** al inicio de la línea.

- **Si sale un error en rojo diciendo que "la ejecución de scripts está deshabilitada":** escribe esto, responde `S` y repite el comando de arriba:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

- **Si estás usando CMD (pantalla negra) en vez de PowerShell**, el comando es:

```bash
ambiente\Scripts\activate
```

- **Si la carpeta `ambiente` no existe** (por ejemplo, en otro computador), créala e instala PySpark una sola vez:

```bash
python -m venv ambiente
```

```powershell
ambiente\Scripts\Activate.ps1
```

```bash
pip install pyspark
pip install py4j
```

### Paso 2: entrar a la carpeta del examen

```bash
cd Ex1De
```

### Paso 3: ejecutar el programa

```bash
python etl_ventas_colombia.py
```

Tarda cerca de **1 minuto** (Spark es lento al arrancar). Es normal que salgan mensajes raros mientras trabaja.

### Paso 4: leer las respuestas
Al final verás una tabla por cada pregunta: `P1`, `P2`, ... `P10`.
**La respuesta es la PRIMERA FILA de cada tabla** (la de arriba). Compárala con las opciones del examen (A, B, C, D).

---

# OPCIONAL - Ejecutar con un botón dentro de Sublime Text

Así no tienes que escribir el comando; presionas **Ctrl + B** y corre.

1. Menú **Tools → Build System → New Build System...**
2. Borra todo lo que aparezca y pega esto (cambia la ruta por la de **tu** carpeta, usando `/` y no `\`):

```json
{
    "cmd": ["C:/Users/sebastian/Documents/Herramientas computacionales/DataAnalitycs/ambiente/Scripts/python.exe", "-u", "$file"],
    "working_dir": "$file_path",
    "selector": "source.python"
}
```

3. Guárdalo con el nombre `PySpark.sublime-build`.
4. Menú **Tools → Build System → PySpark** (déjalo marcado).
5. Abre `etl_ventas_colombia.py` y presiona **Ctrl + B**. Las respuestas salen en el panel de abajo.

---

# ¿Qué hace la limpieza? (explicado fácil)

El examen dice: **no se inventan datos**. Lo que está mal se **borra**; lo que solo está escrito distinto se **arregla**.

**Se arregla (solo escritura):**
- Espacios de sobra: `" bogota "` → `Bogotá`
- Mayúsculas/minúsculas y tildes: `MEDELLIN` → `Medellín`, `mouse inalambrico` → `Mouse Inalámbrico`
- Nombres mal escritos del mismo producto: `Laptop Pro14` → `Laptop Pro 14`

**Se borra la fila si:**

| Campo | Regla |
|---|---|
| `order_id` | No puede estar vacío y no puede repetirse (queda 1 sola fila por pedido) |
| `order_date` | Debe ser una fecha real y del año 2025 (por ejemplo `not_a_date` se borra) |
| `city`, `product` | No pueden estar vacíos |
| `quantity` | Mayor que 0 y máximo 20 |
| `unit_price_cop` | Mayor que 0 y menor que 10.000.000 |
| `customer_age` | Entre 18 y 100 |
| `shipping_days` | Entre 1 y 15 |
| `returned_qty` | Entre 0 y la cantidad comprada |

**Resultado:** de 12.030 filas quedan **11.758**, y **9.152** son pedidos "Entregado".

**Valor de venta** = cantidad × precio unitario (sin descuento). Así el promedio de la P6 da exactamente 1.518.048, igual a la opción A del examen.

---

# Respuestas que da el programa

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

### Notas
- **P2:** opciones A 1.76, B 1.70, C 1.02, D 1.38. El resultado es 1.70, igual a la **B**.
- **P6:** opciones A 1.518.048, B 245.000, C 525.000, D 385.000. Sin descuento da 1.518.048 (**A**). Con descuento daría 1.418.791, que no coincide con ninguna.
- **P10:** Retail y Pyme quedan muy cerca (4.816 vs 4.793 mil millones). Si te sobra tiempo, revisa esta.

---

# Si algo falla

| Problema | Solución |
|---|---|
| `python` no se reconoce | Reinstala Python marcando "Add python.exe to PATH" y abre una terminal nueva |
| `java` no se reconoce | Instala Java 17 (Parte 1.2) y abre una terminal nueva |
| `No module named pyspark` | No activaste el ambiente (Parte 3, paso 1) o falta `pip install pyspark` |
| Error `Unable to establish loopback connection` | El programa ya trae el arreglo (crea `C:/Temp`). Cierra y abre la terminal y vuelve a correrlo |
| `can't open file ... etl_ventas_colombia.py` | Estás en la carpeta equivocada: escribe `cd Ex1De` (o `cd ..` y luego `cd Ex1De`) |
| Sale el texto con símbolos raros (`�`) en lugar de tildes | Solo es la pantalla de la terminal; los resultados están bien |
