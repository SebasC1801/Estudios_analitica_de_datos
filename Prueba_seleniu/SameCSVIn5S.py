import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd

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

# Cantidad de veces que se descargará el mismo CSV
CANTIDAD_DESCARGAS = 5

# Tiempo entre cada descarga
INTERVALO = 5

# Carpeta de descargas
DOWNLOAD_DIR = Path.home() / "Downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Base de datos SQLite
DATABASE = Path(__file__).with_name("descargas.db")


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
# OBTENER NOMBRE DEL CSV
# ============================================================

def obtener_nombre_csv(url):

    nombre = Path(
        urlparse(url).path
    ).name

    if not nombre.lower().endswith(".csv"):

        nombre = "dataset.csv"

    return nombre


# ============================================================
# ESPERAR A QUE EL CSV TERMINE DE DESCARGARSE
# ============================================================

def esperar_descarga(
    archivo_objetivo,
    timeout=300
):

    limite = time.time() + timeout

    ultimo_tamano = -1
    tiempo_sin_cambios = 0

    while time.time() < limite:

        # ----------------------------------------------------
        # Comprobar archivo temporal de Chrome
        # ----------------------------------------------------

        archivo_temporal = Path(
            str(archivo_objetivo) + ".crdownload"
        )

        if archivo_temporal.exists():

            ultimo_tamano = -1
            tiempo_sin_cambios = 0

            time.sleep(1)

            continue

        # ----------------------------------------------------
        # Comprobar que existe el CSV
        # ----------------------------------------------------

        if archivo_objetivo.exists():

            try:

                tamano_actual = (
                    archivo_objetivo.stat().st_size
                )

            except FileNotFoundError:

                time.sleep(1)

                continue

            # ------------------------------------------------
            # Comprobar que el tamaño se estabilizó
            # ------------------------------------------------

            if tamano_actual == ultimo_tamano:

                tiempo_sin_cambios += 1

            else:

                tiempo_sin_cambios = 0

                ultimo_tamano = tamano_actual

            # ------------------------------------------------
            # Si lleva 2 segundos sin cambiar,
            # consideramos terminada la descarga
            # ------------------------------------------------

            if tiempo_sin_cambios >= 2:

                return archivo_objetivo

        time.sleep(1)

    raise TimeoutError(
        f"La descarga no terminó después de "
        f"{timeout} segundos: "
        f"{archivo_objetivo.name}"
    )


# ============================================================
# CONFIGURAR CHROME
# ============================================================

options = Options()

options.add_experimental_option(
    "prefs",
    {
        "download.default_directory":
            str(DOWNLOAD_DIR),

        "download.prompt_for_download":
            False,

        "download.directory_upgrade":
            True,

        "safebrowsing.enabled":
            True,

        "profile.default_content_setting_values.automatic_downloads":
            1
    }
)


# ============================================================
# CREAR BASE DE DATOS
# ============================================================

crear_base_datos()


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

    # ========================================================
    # ABRIR PÁGINA
    # ========================================================

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

    time.sleep(2)


    # ========================================================
    # BUSCAR CSV
    # ========================================================

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

        # Convertir URL relativa en absoluta
        csv_url = urljoin(
            URL,
            href
        )

        # Detectar enlaces CSV
        if ".csv" in csv_url.lower():

            if csv_url not in csv_urls:

                csv_urls.append(csv_url)


    # ========================================================
    # MOSTRAR RESULTADOS
    # ========================================================

    print()

    print(
        f"CSV encontrados: {len(csv_urls)}"
    )

    if not csv_urls:

        raise Exception(
            "No se encontraron archivos CSV "
            "en la página."
        )

    for numero, csv_url in enumerate(
        csv_urls,
        start=1
    ):

        print(
            f"{numero}. {csv_url}"
        )


    # ========================================================
    # SELECCIONAR PRIMER CSV
    # ========================================================

    csv_url = csv_urls[0]

    nombre_csv = obtener_nombre_csv(
        csv_url
    )

    archivo_destino = (
        DOWNLOAD_DIR /
        nombre_csv
    )

    print()

    print("CSV seleccionado:")

    print(csv_url)

    print()

    print(
        f"Este archivo se descargará "
        f"{CANTIDAD_DESCARGAS} veces."
    )


    # ========================================================
    # PERMITIR DESCARGAS
    # ========================================================

    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {
            "behavior": "allow",

            "downloadPath":
                str(DOWNLOAD_DIR)
        }
    )


    # ========================================================
    # DESCARGAR 5 VECES
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
        # ELIMINAR ARCHIVO ANTERIOR
        # ----------------------------------------------------

        if archivo_destino.exists():

            print(
                "Eliminando archivo anterior..."
            )

            try:

                archivo_destino.unlink()

            except PermissionError:

                print(
                    "El archivo está siendo utilizado."
                )

                raise


        # ----------------------------------------------------
        # ELIMINAR ARCHIVO TEMPORAL
        # ----------------------------------------------------

        archivo_temporal = Path(
            str(archivo_destino) + ".crdownload"
        )

        if archivo_temporal.exists():

            archivo_temporal.unlink()


        # ----------------------------------------------------
        # DESCARGAR
        # ----------------------------------------------------

        print()

        print(
            "Descargando mediante Selenium..."
        )

        print(
            f"URL: {csv_url}"
        )


        # Navegar directamente al CSV.
        #
        # Chrome detectará que es un archivo
        # y comenzará la descarga automáticamente.

        driver.get(csv_url)


        # ----------------------------------------------------
        # ESPERAR DESCARGA
        # ----------------------------------------------------

        print(
            "Esperando a que termine la descarga..."
        )

        archivo_descargado = esperar_descarga(
            archivo_destino,
            timeout=300
        )


        # ----------------------------------------------------
        # REGISTRAR EN SQLITE
        # ----------------------------------------------------

        registrar_descarga(
            archivo_descargado,
            csv_url
        )

        descargados.append(
            archivo_descargado
        )


        # ----------------------------------------------------
        # RESULTADO
        # ----------------------------------------------------

        print()

        print(
            "✓ DESCARGA COMPLETADA"
        )

        print(
            f"Archivo: "
            f"{archivo_descargado.name}"
        )

        print(
            f"Tamaño: "
            f"{archivo_descargado.stat().st_size:,} bytes"
        )

        print(
            "SQLite: ✓"
        )


        # ----------------------------------------------------
        # INTERVALO
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

    print(
        "Chrome cerrado."
    )


# ============================================================
# RESULTADO FINAL
# ============================================================

print()

print("=" * 70)

print(
    "PROCESO TERMINADO"
)

print("=" * 70)

print()

print(
    f"Archivos descargados: "
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

print()

print(
    f"Base de datos: {DATABASE}"
)