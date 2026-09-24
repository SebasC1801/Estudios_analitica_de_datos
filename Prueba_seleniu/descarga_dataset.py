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


def wait_for_download(file_name, timeout=60):
    file_path = download_directory / file_name
    temporary_path = download_directory / f"{file_name}.crdownload"
    end_time = time.time() + timeout

    while time.time() < end_time:
        if file_path.exists() and not temporary_path.exists():
            return file_path
        time.sleep(1)

    raise TimeoutError(f"La descarga no terminó: {file_name}")


def save_downloads(downloaded_files, url):
    download_data = pd.DataFrame([
        {
            "nombre_archivo": file_path.name,
            "ruta_archivo": str(file_path),
            "url": url,
            "fecha_descarga": datetime.now().isoformat(timespec="seconds"),
            "tamano_bytes": file_path.stat().st_size,
        }
        for file_path in downloaded_files
    ])

    with sqlite3.connect(database_path) as connection:
        download_data.to_sql(
            "archivos_descargados",
            connection,
            if_exists="append",
            index=False,
        )


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

    # 2. Obtener los enlaces CSV y seleccionar cinco archivos
    csv_links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.csv']")
    files_to_download = csv_links[:5]

    if not files_to_download:
        raise NoSuchElementException("No se encontraron archivos CSV")

    # 3. Iniciar una descarga cada cinco segundos
    for index, download_link in enumerate(files_to_download, start=1):
        file_name = download_link.text.strip() or download_link.get_attribute("href").split("/")[-1]
        download_link.click()
        print(f"Descarga {index}/{len(files_to_download)} iniciada: {file_name}")

        if index < len(files_to_download):
            time.sleep(5)

    # 4. Esperar a que terminen y registrar los archivos en SQLite
    downloaded_files = [
        wait_for_download(
            link.text.strip() or link.get_attribute("href").split("/")[-1]
        )
        for link in files_to_download
    ]
    save_downloads(downloaded_files, url)
    print(f"Se descargaron y registraron {len(downloaded_files)} archivos en {database_path}")

finally:
    # Cerrar el navegador
    driver.quit()
