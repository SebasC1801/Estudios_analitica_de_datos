import csv  # Esta línea importa la librería csv, que permite crear y escribir archivos CSV.
import json  # Esta línea importa la librería json, que permite convertir objetos JSON a texto y viceversa.
import os  # Esta línea importa os, que permite leer variables de entorno y manejar rutas del sistema.
import sqlite3  # Esta línea importa sqlite3, que permite crear y consultar una base de datos SQLite local.
import time  # Esta línea importa time, que permite pausar la ejecución para evitar saturar la API.
from pathlib import Path  # Esta línea importa Path, que facilita la manipulación de rutas de archivo.
from typing import Any, Dict, Iterable, List, Optional  # Esta línea importa tipos de Python para documentar y organizar mejor las funciones.
import requests  # Esta línea importa requests, que permite hacer peticiones HTTP a una API.


API_URL = os.getenv("API_URL", "https://api.github.com/users/microsoft/repos")  # Esta línea define la URL de la API. Si no se configura una variable de entorno, usa una API pública de ejemplo.
API_TOKEN = os.getenv("API_TOKEN", "")  # Esta línea obtiene el token de autenticación si la API lo exige; si no existe, queda vacío.
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))  # Esta línea define el tiempo máximo de espera por respuesta de la API.
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))  # Esta línea define cuántos registros se solicitan por página.
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "."))  # Esta línea define la carpeta donde se guardarán los archivos generados.
DB_NAME = os.getenv("DB_NAME", "etl_api.db")  # Esta línea define el nombre del archivo SQLite que se generará.
CSV_NAME = os.getenv("CSV_NAME", "etl_api.csv")  # Esta línea define el nombre del archivo CSV que se generará.
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))  # Esta línea define el máximo de páginas que se consultan; si vale 0, no hay límite.


def build_headers() -> Dict[str, str]:  # Esta función crea los encabezados HTTP necesarios para llamar a la API.
    headers = {  # Esta línea inicia el diccionario con los encabezados básicos.
        "Accept": "application/json",  # Esta línea indica que la API debe devolver JSON.
        "Content-Type": "application/json",  # Esta línea indica que el contenido enviado es JSON.
    }
    if API_TOKEN:  # Esta condición verifica si existe un token para autenticación.
        headers["Authorization"] = f"Bearer {API_TOKEN}"  # Esta línea agrega el token al encabezado Authorization.
    return headers  # Esta línea devuelve los encabezados listos para la petición.


def extract_records(payload: Any) -> List[Dict[str, Any]]:  # Esta función extrae la lista de registros útiles dentro de la respuesta API.
    if isinstance(payload, list):  # Esta condición verifica si la respuesta es una lista directa.
        return [item for item in payload if isinstance(item, dict)]  # Esta línea devuelve solo los elementos que son diccionarios.

    if isinstance(payload, dict):  # Esta condición verifica si la respuesta es un diccionario.
        for key in ("data", "results", "items", "records", "rows"):  # Este bucle prueba las claves más comunes en respuestas JSON.
            value = payload.get(key)  # Esta línea obtiene el valor asociado a esa clave.
            if isinstance(value, list):  # Esta condición verifica si el valor es una lista.
                return [item for item in value if isinstance(item, dict)]  # Esta línea devuelve solo los elementos diccionario de esa lista.
        if payload:  # Esta condición verifica si el diccionario tiene contenido aunque no tenga una clave esperada.
            return [payload]  # Esta línea envuelve el diccionario como un único registro.

    return []  # Esta línea devuelve una lista vacía si la respuesta no tiene registros válidos.


def get_next_url(payload: Any, current_url: str) -> Optional[str]:  # Esta función detecta la URL de la siguiente página, si existe.
    if not isinstance(payload, dict):  # Esta condición verifica si la respuesta no es un diccionario.
        return None  # Esta línea devuelve None para indicar que no hay paginación.

    next_url = payload.get("next") or payload.get("next_url") or payload.get("nextPage")  # Esta línea busca posibles claves para la siguiente página.
    if isinstance(next_url, str) and next_url:  # Esta condición comprueba si existe una URL válida.
        return next_url  # Esta línea devuelve la URL de la siguiente página.

    if "links" in payload and isinstance(payload["links"], dict):  # Esta condición verifica si la API usa el campo links para paginación.
        next_url = payload["links"].get("next")  # Esta línea extrae la URL de la siguiente página dentro de links.
        if isinstance(next_url, str) and next_url:  # Esta condición valida la URL extraída.
            return next_url  # Esta línea devuelve la URL de la siguiente página.

    return None  # Esta línea devuelve None si no se detecta paginación.


def fetch_all_pages(api_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:  # Esta función consulta todas las páginas disponibles de la API y junta los registros.
    all_rows: List[Dict[str, Any]] = []  # Esta línea crea la lista donde se acumularán todos los datos de todas las páginas.
    url = api_url  # Esta línea inicia la URL actual con la URL base.
    page = 1  # Esta línea inicia el contador de páginas en 1.

    while url:  # Este bucle continúa mientras exista una URL para consultar.
        if MAX_PAGES and page > MAX_PAGES:  # Esta condición detiene la carga si se supera el máximo de páginas configurado.
            break  # Esta línea sale del bucle.

        response = requests.get(  # Esta línea realiza la petición HTTP a la API.
            url,  # Esta línea indica la URL a consultar.
            headers=build_headers(),  # Esta línea envía los encabezados necesarios.
            params=params,  # Esta línea envía los parámetros de paginación.
            timeout=API_TIMEOUT,  # Esta línea define el tiempo máximo de espera.
        )
        response.raise_for_status()  # Esta línea lanza un error si la respuesta HTTP no fue exitosa.

        payload = response.json()  # Esta línea convierte la respuesta JSON en un objeto Python.
        rows = extract_records(payload)  # Esta línea obtiene los registros útiles del JSON.
        if not rows:  # Esta condición verifica si no hubo registros.
            break  # Esta línea termina la extracción.

        all_rows.extend(rows)  # Esta línea agrega los registros de la página actual a la lista total.

        next_url = get_next_url(payload, url)  # Esta línea intenta detectar la URL de la siguiente página.
        if not next_url:  # Esta condición termina la extracción si no hay página siguiente.
            break  # Esta línea sale del bucle.

        url = next_url  # Esta línea reemplaza la URL actual por la siguiente página.
        params = None  # Esta línea elimina los parámetros anteriores para evitar duplicarlos.
        page += 1  # Esta línea incrementa el contador de páginas.
        time.sleep(0.2)  # Esta línea espera un poco antes de la siguiente petición para no saturar la API.

    return all_rows  # Esta línea devuelve la lista completa con todos los registros extraídos.


def normalize_value(value: Any) -> Any:  # Esta función convierte valores complejos a un formato que SQLite puede guardar sin errores.
    if value is None or isinstance(value, (str, int, float, bool)):  # Esta condición verifica si el valor ya es seguro para guardar.
        return value  # Esta línea devuelve el valor sin cambios.
    if isinstance(value, (dict, list, tuple)):  # Esta condición verifica si el valor es un diccionario o lista.
        return json.dumps(value, ensure_ascii=False)  # Esta línea convierte el valor a texto JSON.
    return str(value)  # Esta línea convierte cualquier otro tipo de dato a texto.


def flatten_record(data: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:  # Esta función aplaniza los datos anidados para convertirlos en columnas planas.
    flattened: Dict[str, Any] = {}  # Esta línea crea el diccionario final con los datos planos.

    def recurse(value: Any, key: str = "") -> None:  # Esta función recorre recursivamente los valores anidados.
        if isinstance(value, dict):  # Esta condición verifica si el valor es un diccionario.
            for child_key, child_value in value.items():  # Este bucle recorre cada campo del diccionario.
                new_key = f"{key}{sep}{child_key}" if key else child_key  # Esta línea arma el nombre de la columna anidada.
                recurse(child_value, new_key)  # Esta línea llama recursivamente a la misma función con el valor hijo.
        elif isinstance(value, list):  # Esta condición verifica si el valor es una lista.
            flattened[key] = normalize_value(value)  # Esta línea guarda la lista convertida a JSON.
        else:  # Esta condición aplica cuando el valor es un dato simple.
            flattened[key] = normalize_value(value)  # Esta línea guarda el valor simple o convertido.

    recurse(data, parent_key)  # Esta línea inicia la recursión sobre el registro completo.
    return flattened  # Esta línea devuelve el registro ya aplanado.


def normalize_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:  # Esta función transforma todos los registros para que queden listos para CSV y SQLite.
    normalized: List[Dict[str, Any]] = []  # Esta línea crea la lista con los registros normalizados.
    for row in rows:  # Este bucle recorre cada registro del conjunto.
        if isinstance(row, dict):  # Esta condición verifica que cada elemento sea un diccionario.
            normalized.append(flatten_record(row))  # Esta línea aplana y guarda cada registro transformado.
    return normalized  # Esta línea devuelve la lista completa transformada.


def write_csv(rows: List[Dict[str, Any]], file_path: Path) -> None:  # Esta función escribe los datos en un archivo CSV.
    if not rows:  # Esta condición evita crear un CSV vacío.
        return  # Esta línea sale de la función.

    file_path.parent.mkdir(parents=True, exist_ok=True)  # Esta línea crea la carpeta de destino si no existe.
    fieldnames = sorted({key for row in rows for key in row.keys()})  # Esta línea obtiene todas las columnas y las ordena alfabéticamente.

    with file_path.open("w", encoding="utf-8", newline="") as csv_file:  # Esta línea abre el archivo CSV para escribirlo en UTF-8.
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)  # Esta línea crea el escritor CSV con todas las columnas.
        writer.writeheader()  # Esta línea escribe la fila de encabezados.
        for row in rows:  # Este bucle recorre cada registro para escribirlo.
            writer.writerow({key: row.get(key, "") for key in fieldnames})  # Esta línea escribe una fila con los valores correspondientes.


def write_sqlite(rows: List[Dict[str, Any]], db_path: Path) -> None:  # Esta función guarda los registros en una base de datos SQLite.
    if not rows:  # Esta condición evita crear una tabla vacía.
        return  # Esta línea sale de la función.

    db_path.parent.mkdir(parents=True, exist_ok=True)  # Esta línea crea la carpeta de la base de datos si hace falta.
    columns = sorted({key for row in rows for key in row.keys()})  # Esta línea reúne todas las columnas de todos los registros.
    table_name = "api_data"  # Esta línea define el nombre de la tabla SQLite.

    conn = sqlite3.connect(db_path)  # Esta línea abre la conexión con la base de datos.
    try:  # Este bloque intenta ejecutar la creación e inserción de la tabla.
        cur = conn.cursor()  # Esta línea crea un cursor SQL para ejecutar sentencias.
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")  # Esta línea elimina la tabla anterior para evitar duplicados.
        placeholders = ", ".join(["?" for _ in columns])  # Esta línea crea marcadores para los valores de cada columna.
        column_sql = ", ".join([f'"{column}" TEXT' for column in columns])  # Esta línea define todas las columnas como tipo TEXT.
        cur.execute(f'CREATE TABLE {table_name} ({column_sql})')  # Esta línea crea la tabla con las columnas generadas.

        insert_columns = ", ".join(f'"{column}"' for column in columns)  # Esta línea prepara los nombres de columnas para la inserción.
        insert_sql = f'INSERT INTO {table_name} ({insert_columns}) VALUES ({placeholders})'  # Esta línea arma la sentencia SQL INSERT.
        for row in rows:  # Este bucle recorre cada registro para insertarlo en la tabla.
            record = [row.get(col, "") for col in columns]  # Esta línea crea una lista con los valores del registro.
            cur.execute(insert_sql, record)  # Esta línea inserta los datos en la tabla.

        conn.commit()  # Esta línea guarda los cambios realizados en la base de datos.
    finally:  # Este bloque asegura que la conexión se cierre aunque ocurra un error.
        conn.close()  # Esta línea cierra la conexión con la base de datos.


def main() -> None:  # Esta función ejecuta el flujo completo del ETL.
    params = {"page_size": PAGE_SIZE}  # Esta línea prepara los parámetros de paginación estándar.
    if "api.github.com" in API_URL:  # Esta condición detecta si la API es GitHub para usar el parámetro correcto.
        params = {"per_page": PAGE_SIZE}  # Esta línea cambia el nombre del parámetro de paginación para GitHub.
    rows = fetch_all_pages(API_URL, params=params)  # Esta línea realiza la extracción completa de todas las páginas.
    normalized = normalize_rows(rows)  # Esta línea transforma los registros para dejarlos listos para guardar.

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Esta línea crea la carpeta de salida si no existe.
    csv_path = OUTPUT_DIR / CSV_NAME  # Esta línea define la ruta del archivo CSV final.
    db_path = OUTPUT_DIR / DB_NAME  # Esta línea define la ruta del archivo SQLite final.

    write_csv(normalized, csv_path)  # Esta línea guarda los registros en el archivo CSV.
    write_sqlite(normalized, db_path)  # Esta línea guarda los registros en la base SQLite.

    print(f"Registros extraídos: {len(normalized)}")  # Esta línea imprime cuántos registros se obtuvieron.
    print(f"Archivo CSV: {csv_path.resolve()}")  # Esta línea imprime la ruta absoluta del archivo CSV generado.
    print(f"Base de datos SQLite: {db_path.resolve()}")  # Esta línea imprime la ruta absoluta de la base SQLite generada.


if __name__ == "__main__":  # Esta línea verifica si el archivo se está ejecutando como un programa principal.
    main()  # Esta línea ejecuta la función principal del ETL.
