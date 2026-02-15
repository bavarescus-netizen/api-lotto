import requests
from bs4 import BeautifulSoup
import re

URL = "https://loteriadehoy.com/animalito/lottoactivo/resultados/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

def normalizar_animal(texto):
    # Elimina números (ej: "01 Delfín" -> "delfin") y acentos
    limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', texto)
    return limpio.lower().strip()

def obtener_ultimo_resultado():
    try:
        r = requests.get(URL, headers=HEADERS, timeout=15)
        if r.status_code != 200: return None

        soup = BeautifulSoup(r.text, "html.parser")
        # Buscamos la primera fila de datos del cuerpo de la tabla
        fila = soup.select_one("table tr:nth-of-type(2)") 

        if not fila: return None

        columnas = fila.find_all("td")
        if len(columnas) < 3: return None

        # Normalización Crítica para el Motor V4
        animal_raw = columnas[2].text.strip()
        animal_listo = normalizar_animal(animal_raw)

        return {
            "fecha": columnas[0].text.strip(),
            "hora": columnas[1].text.strip().upper(), # Estandarizar "10:00 AM"
            "animalito": animal_listo,
            "loteria": "Lotto Activo"
        }
    except Exception as e:
        print(f"❌ Error en Scraper: {e}")
        return None
