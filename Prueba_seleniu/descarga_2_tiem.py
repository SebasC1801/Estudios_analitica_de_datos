import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

# Ruta donde quieres que se descargue el archivo
download_directory = Path.home() / "Downloads"
download_directory.mkdir(parents=True, exist_ok=True)
database_path = Path(__file__).with_name("descargas.db")


def create_downloads_table():
    """Crea la tabla de archivos descargados"""
    with sqlite3.connect(database_path) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS archivos_descargados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre_archivo TEXT NOT NULL,
                ruta_archivo TEXT NOT NULL,
                url TEXT NOT NULL,
                fecha_descarga TEXT NOT NULL,
                tamano_bytes INTEGER NOT NULL
            )
        """)


def save_download(file_path, url):
    """Guarda los datos de un archivo descargado usando pandas y SQL."""
    download_data = pd.DataFrame([{
        "nombre_archivo": file_path.name,
        "ruta_archivo": str(file_path),
        "url": url,
        "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
        "tamano_bytes": file_path.stat().st_size,
    }])

    with sqlite3.connect(database_path) as connection:
        download_data.to_sql(
            "archivos_descargados",
            connection,
            if_exists="append",
            index=False,
        )

def wait_for_download(file_name, files_before, timeout=300):
    """Espera hasta que Chrome cree y termine de escribir un archivo nuevo."""
    end_time = time.time() + timeout

    while time.time() < end_time:
        matching_files = [
            file_path for file_path in download_directory.iterdir()
            if file_path.is_file()
            and file_path.name not in files_before
            and file_path.suffix.lower() == ".csv"
            if file_path.name == file_name or file_path.name.startswith(f"{Path(file_name).stem} (")
        ]
        if matching_files:
            downloaded_file = max(matching_files, key=lambda path: path.stat().st_mtime)
            temporary_file = downloaded_file.with_name(f"{downloaded_file.name}.crdownload")
            if not temporary_file.exists():
                return downloaded_file
        time.sleep(1)

    raise TimeoutError(f"La descarga no terminó en {timeout} segundos: {file_name}")


create_downloads_table()

# Configurar Chrome para descargar archivos automáticamente
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": str(download_directory),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=chrome_options)

try:
    # 1. Abrir la página de descarga
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    driver.get(url)

    # 2. Obtener cinco enlaces CSV diferentes del mismo directorio
    csv_links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.csv']")[:5]
    if len(csv_links) < 5:
        raise NoSuchElementException("No se encontraron cinco archivos CSV")

    # 3. Descargar y registrar cada archivo, con cinco segundos entre inicios
    for index, download_link in enumerate(csv_links, start=1):
        file_url = download_link.get_attribute("href")
        file_name = file_url.rsplit("/", 1)[-1]
        files_before = {file_path.name for file_path in download_directory.iterdir()}

        download_link.click()
        print(f"Descarga {index}/5 iniciada: {file_name}")

        downloaded_file = wait_for_download(file_name, files_before)
        save_download(downloaded_file, file_url)
        print(f"Registrado en SQLite: {downloaded_file.name}")

        if index < 5:
            time.sleep(5)

finally:
    # Cerrar el navegador
    driver.quit()
