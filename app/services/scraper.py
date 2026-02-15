import requests
from bs4 import BeautifulSoup

URL = "https://loteriadehoy.com/animalito/lottoactivo/resultados/"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def obtener_ultimo_resultado():

    r = requests.get(URL, headers=HEADERS, timeout=15)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    tabla = soup.find("table")

    if not tabla:
        return None

    fila = tabla.find("tr", class_="yellow")  # última fila (resultado reciente)

    if not fila:
        return None

    columnas = fila.find_all("td")

    return {
        "fecha": columnas[0].text.strip(),
        "hora": columnas[1].text.strip(),
        "animalito": columnas[2].text.strip().lower(),
        "loteria": "Lotto Activo"
    }
