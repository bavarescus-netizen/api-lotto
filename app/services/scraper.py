import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime, timedelta

# 🔹 CONFIGURACIÓN DB
DB_CONFIG = {
    "host": "localhost",
    "database": "tu_base",
    "user": "tu_usuario",
    "password": "tu_password"
}

BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo/historico/{}/{}"


# -------------------------------------------------
# 🔎 OBTENER ÚLTIMA FECHA EN BASE
# -------------------------------------------------
def obtener_ultima_fecha(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fecha 
            FROM historico 
            ORDER BY fecha DESC, hora DESC 
            LIMIT 1
        """)
        result = cur.fetchone()

        if result:
            return result[0]  # tipo DATE
        else:
            raise Exception("La tabla historico está vacía.")


# -------------------------------------------------
# 🧮 CALCULAR RANGO
# -------------------------------------------------
def calcular_rango(conn):
    ultima_fecha = obtener_ultima_fecha(conn)
    fecha_inicio = ultima_fecha  # IMPORTANTE: NO sumamos día
    fecha_fin = datetime.now().date() - timedelta(days=1)

    if fecha_inicio > fecha_fin:
        print("Base ya está actualizada hasta ayer.")
        return None, None

    return fecha_inicio, fecha_fin


# -------------------------------------------------
# 🌐 SCRAPEAR WEB
# -------------------------------------------------
def scrapear_rango(fecha_inicio, fecha_fin):
    url = BASE_URL.format(fecha_inicio, fecha_fin)
    print(f"Scrapeando: {url}")

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    resultados = []

    filas = soup.find_all("tr")

    for fila in filas:
        columnas = fila.find_all("td")

        if len(columnas) >= 4:
            fecha = columnas[0].text.strip()
            hora = columnas[1].text.strip()
            numero = columnas[2].text.strip()
            animal = columnas[3].text.strip()

            try:
                fecha_convertida = datetime.strptime(fecha, "%d/%m/%Y").date()
            except:
                continue

            resultados.append((fecha_convertida, hora, numero, animal))

    print(f"{len(resultados)} registros encontrados.")
    return resultados


# -------------------------------------------------
# 💾 INSERTAR EN DB
# -------------------------------------------------
def insertar_resultados(conn, datos):
    with conn.cursor() as cur:
        for registro in datos:
            cur.execute("""
                INSERT INTO historico (fecha, hora, numero, animal)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fecha, hora) DO NOTHING
            """, registro)

    conn.commit()
    print("Datos insertados correctamente.")


# -------------------------------------------------
# 🚀 FUNCIÓN PRINCIPAL
# -------------------------------------------------
def actualizar_historico():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        fecha_inicio, fecha_fin = calcular_rango(conn)

        if not fecha_inicio:
            return

        datos = scrapear_rango(fecha_inicio, fecha_fin)

        if datos:
            insertar_resultados(conn, datos)
        else:
            print("No se encontraron nuevos datos.")

    finally:
        conn.close()


# -------------------------------------------------
# ▶ EJECUCIÓN DIRECTA
# -------------------------------------------------
if __name__ == "__main__":
    actualizar_historico()
