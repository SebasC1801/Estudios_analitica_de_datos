from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Configurar opciones de Chrome
chrome_options = Options()
chrome_options.add_experimental_option("detach", True)  # Mantener la ventana abierta

# Selenium Manager obtiene automáticamente el controlador compatible.
driver = webdriver.Chrome(options=chrome_options)

# Abrir Google para probar
driver.get("https://www.google.com")
