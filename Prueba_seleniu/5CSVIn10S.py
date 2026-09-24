import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ============================================================
# CONFIGURACIÓN
# ============================================================

# SOLO CAMBIA ESTA URL
URL = "https://github.com/SeeingBlue/uap-corpus-viewer"

# El mismo CSV se descargará 5 veces
CANTIDAD_DESCARGAS = 5

# 5 segundos entre cada extracción
INTERVALO = 5

# Carpeta de descargas
DOWNLOAD_DIR = Path.home() / "Downloads"
DOWNLOAD_DIR.mkdir(
    parents=True,
    exist_ok=True
)

# Base de datos
DATABASE = Path(__file__).with_name(
    "descargas.db"
)


# ============================================================
# SQLITE
# ============================================================

def crear_base_datos():

    with sqlite3.connect(DATABASE) as db:

        db.execute("""
            CREATE TABLE IF NOT EXISTS archivos_descargados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre_archivo TEXT NOT NULL,
                ruta_archivo TEXT NOT NULL,
                url TEXT NOT NULL,
                fecha_descarga TEXT NOT NULL,
                tamano_bytes INTEGER NOT NULL
            )
        """)

        db.commit()


# ============================================================
# REGISTRAR DESCARGA
# ============================================================

def registrar_descarga(archivo, url):

    datos = pd.DataFrame([
        {
            "nombre_archivo": archivo.name,
            "ruta_archivo": str(archivo),
            "url": url,
            "fecha_descarga":
                datetime.now().isoformat(
                    timespec="seconds"
                ),
            "tamano_bytes":
                archivo.stat().st_size
        }
    ])

    with sqlite3.connect(DATABASE) as db:

        datos.to_sql(
            "archivos_descargados",
            db,
            if_exists="append",
            index=False
        )


# ============================================================
# BUSCAR CSV
# ============================================================

def buscar_csv(driver, url):

    print()
    print("Buscando archivos CSV...")

    enlaces = driver.find_elements(
        By.CSS_SELECTOR,
        "a[href]"
    )

    csv_urls = []

    for enlace in enlaces:

        href = enlace.get_attribute("href")

        if not href:
            continue

        csv_url = urljoin(
            url,
            href
        )

        csv_url = csv_url.strip()

        parsed = urlparse(csv_url)

        if (
            parsed.path.lower().endswith(".csv")
            and csv_url not in csv_urls
        ):

            csv_urls.append(csv_url)

    return csv_urls


# ============================================================
# OBTENER NOMBRE DEL CSV
# ============================================================

def obtener_nombre_archivo(csv_url, numero):

    parsed = urlparse(csv_url)

    nombre = Path(
        parsed.path
    ).name

    if not nombre:

        nombre = "dataset.csv"

    if not nombre.lower().endswith(".csv"):

        nombre = "dataset.csv"

    if numero == 1:

        return nombre

    ruta = Path(nombre)

    return (
        f"{ruta.stem}_{numero}"
        f"{ruta.suffix}"
    )


# ============================================================
# CREAR SESIÓN DE REQUESTS DESDE SELENIUM
# ============================================================

def crear_sesion_desde_selenium(driver):

    session = requests.Session()

    # --------------------------------------------------------
    # Copiar cookies de Chrome
    # --------------------------------------------------------

    cookies = driver.get_cookies()

    for cookie in cookies:

        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain"),
            path=cookie.get("path", "/")
        )

    # --------------------------------------------------------
    # Headers similares a Chrome
    # --------------------------------------------------------

    session.headers.update({

        "User-Agent":
            driver.execute_script(
                "return navigator.userAgent;"
            ),

        "Accept":
            "text/csv,text/plain,"
            "application/octet-stream,"
            "*/*",

        "Accept-Language":
            "es-CO,es;q=0.9,en;q=0.8",

        "Referer":
            driver.current_url,

        "Connection":
            "keep-alive"
    })

    return session


# ============================================================
# DESCARGAR CSV
# ============================================================

def descargar_csv(
    session,
    csv_url,
    destino
):

    print()
    print("Descargando CSV...")
    print(f"URL: {csv_url}")

    respuesta = session.get(
        csv_url,
        stream=True,
        timeout=120,
        allow_redirects=True
    )

    print(
        f"HTTP: {respuesta.status_code}"
    )

    respuesta.raise_for_status()

    with open(
        destino,
        "wb"
    ) as archivo:

        for bloque in respuesta.iter_content(
            chunk_size=1024 * 1024
        ):

            if bloque:

                archivo.write(
                    bloque
                )

    return destino


# ============================================================
# VALIDAR CSV CON PANDAS
# ============================================================

def analizar_csv(archivo):

    try:

        dataframe = pd.read_csv(
            archivo,
            low_memory=False
        )

        print()
        print("PANDAS")
        print("-" * 40)

        print(
            f"Registros: "
            f"{len(dataframe):,}"
        )

        print(
            f"Columnas: "
            f"{len(dataframe.columns)}"
        )

        print(
            "Columnas:"
        )

        for columna in dataframe.columns:

            print(
                f"  - {columna}"
            )

        return dataframe

    except Exception as error:

        print()
        print(
            f"Advertencia Pandas: {error}"
        )

        return None


# ============================================================
# CREAR BASE DE DATOS
# ============================================================

crear_base_datos()


# ============================================================
# CONFIGURAR CHROME
# ============================================================

options = Options()

options.add_argument(
    "--start-maximized"
)

options.add_experimental_option(
    "prefs",
    {
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
)


# ============================================================
# INICIAR SELENIUM
# ============================================================

driver = webdriver.Chrome(
    options=options
)

wait = WebDriverWait(
    driver,
    30
)

descargados = []


# ============================================================
# PROCESO PRINCIPAL
# ============================================================

try:

    print()
    print("=" * 70)
    print("DESCARGADOR AUTOMÁTICO DE UN CSV")
    print("=" * 70)

    print()
    print("URL:")
    print(URL)

    # --------------------------------------------------------
    # ABRIR PÁGINA
    # --------------------------------------------------------

    print()
    print("Abriendo página...")

    driver.get(URL)

    wait.until(
        EC.presence_of_element_located(
            (
                By.TAG_NAME,
                "body"
            )
        )
    )

    time.sleep(3)

    # --------------------------------------------------------
    # BUSCAR CSV
    # --------------------------------------------------------

    csv_urls = buscar_csv(
        driver,
        URL
    )

    print()
    print(
        f"CSV encontrados: "
        f"{len(csv_urls)}"
    )

    if not csv_urls:

        raise Exception(
            "No se encontraron archivos CSV "
            "en la página."
        )

    # --------------------------------------------------------
    # MOSTRAR CSV
    # --------------------------------------------------------

    for numero, csv_url in enumerate(
        csv_urls,
        start=1
    ):

        print(
            f"{numero}. {csv_url}"
        )

    # --------------------------------------------------------
    # SELECCIONAR EL PRIMER CSV
    # --------------------------------------------------------

    csv_url = csv_urls[0]

    print()
    print("CSV seleccionado:")
    print(csv_url)

    print()
    print(
        f"Este archivo se descargará "
        f"{CANTIDAD_DESCARGAS} veces."
    )

    # --------------------------------------------------------
    # CREAR SESIÓN AUTENTICADA
    # --------------------------------------------------------

    print()
    print(
        "Creando sesión de descarga..."
    )

    session = crear_sesion_desde_selenium(
        driver
    )

    print(
        "Sesión creada correctamente."
    )

    # ========================================================
    # CICLO DE 5 DESCARGAS
    # ========================================================

    for numero in range(
        1,
        CANTIDAD_DESCARGAS + 1
    ):

        print()
        print("-" * 70)

        print(
            f"DESCARGA {numero}/"
            f"{CANTIDAD_DESCARGAS}"
        )

        print("-" * 70)

        # ----------------------------------------------------
        # NOMBRE DEL ARCHIVO
        # ----------------------------------------------------

        nombre = obtener_nombre_archivo(
            csv_url,
            numero
        )

        destino = DOWNLOAD_DIR / nombre

        # ----------------------------------------------------
        # ELIMINAR SI EXISTE
        # ----------------------------------------------------

        if destino.exists():

            destino.unlink()

        # ----------------------------------------------------
        # DESCARGAR
        # ----------------------------------------------------

        try:

            archivo = descargar_csv(
                session,
                csv_url,
                destino
            )

            # ------------------------------------------------
            # VALIDAR
            # ------------------------------------------------

            if not archivo.exists():

                raise Exception(
                    "El archivo no fue creado."
                )

            tamano = archivo.stat().st_size

            if tamano == 0:

                raise Exception(
                    "El archivo está vacío."
                )

            # ------------------------------------------------
            # PANDAS
            # ------------------------------------------------

            dataframe = analizar_csv(
                archivo
            )

            # ------------------------------------------------
            # SQLITE
            # ------------------------------------------------

            registrar_descarga(
                archivo,
                csv_url
            )

            descargados.append(
                archivo
            )

            # ------------------------------------------------
            # RESULTADO
            # ------------------------------------------------

            print()
            print("✓ EXTRACCIÓN COMPLETADA")

            print(
                f"Archivo: "
                f"{archivo.name}"
            )

            print(
                f"Tamaño: "
                f"{tamano:,} bytes"
            )

            print(
                "Pandas: "
                + (
                    "✓"
                    if dataframe is not None
                    else "⚠"
                )
            )

            print(
                "SQLite: ✓"
            )

        except Exception as error:

            print()
            print(
                "✗ ERROR"
            )

            print(
                f"{error}"
            )

        # ----------------------------------------------------
        # ESPERAR 5 SEGUNDOS
        # ----------------------------------------------------

        if numero < CANTIDAD_DESCARGAS:

            print()
            print(
                f"Esperando "
                f"{INTERVALO} segundos..."
            )

            time.sleep(
                INTERVALO
            )


finally:

    driver.quit()

    print()
    print("Chrome cerrado.")


# ============================================================
# RESULTADO FINAL
# ============================================================

print()
print("=" * 70)
print("PROCESO TERMINADO")
print("=" * 70)

print()

print(
    f"Extracciones exitosas: "
    f"{len(descargados)}/"
    f"{CANTIDAD_DESCARGAS}"
)

print()

for archivo in descargados:

    print(
        f"✓ {archivo.name}"
    )

print()

print(
    f"Carpeta: {DOWNLOAD_DIR}"
)

print(
    f"Base de datos: {DATABASE}"
)