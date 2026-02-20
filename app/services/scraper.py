import httpx # Usamos httpx que es más rápido y moderno que requests para FastAPI
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import asyncio

URL_ULTIMO = "https://loteriadehoy.com/animalito/lottoactivo/resultados/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def normalizar_animal(texto):
    # Limpia: "01 Delfín" -> "delfin"
    limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', texto)
    return limpio.lower().strip()

async def obtener_ultimo_resultado():
    """Busca el resultado más reciente para el ciclo infinito"""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(URL_ULTIMO, headers=HEADERS, timeout=10)
            if r.status_code != 200: return None

            soup = BeautifulSoup(r.text, "html.parser")
            fila = soup.select_one("table tr:nth-of-type(2)") 
            if not fila: return None

            columnas = fila.find_all("td")
            if len(columnas) < 3: return None

            return {
                "fecha": columnas[0].text.strip(),
                "hora": columnas[1].text.strip().upper(),
                "animalito": normalizar_animal(columnas[2].text.strip()),
                "loteria": "Lotto Activo"
            }
    except Exception as e:
        print(f"❌ Error en Scraper: {e}")
        return None

async def descargar_rango_historico(fecha_inicio: datetime, fecha_fin: datetime):
    """Descarga datos masivos y los devuelve como lista de diccionarios"""
    datos = []
    fecha_actual = fecha_inicio
    
    async with httpx.AsyncClient() as client:
        while fecha_actual <= fecha_fin:
            # La web usa rangos de 7 días
            fin_rango = fecha_actual + timedelta(days=6)
            url = (
                f"https://loteriadehoy.com/animalito/lottoactivo/historico/"
                f"{fecha_actual.strftime('%Y-%m-%d')}/"
                f"{fin_rango.strftime('%Y-%m-%d')}/"
            )

            try:
                print(f"📡 Descargando bloque: {fecha_actual.date()}")
                r = await client.get(url, headers=HEADERS, timeout=15)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    tabla = soup.find("table")
                    if tabla:
                        fechas_encabezado = [th.text.strip() for th in tabla.find_all("th")[1:]]
                        for fila in tabla.find_all("tr")[1:]:
                            hora_tag = fila.find("th")
                            if not hora_tag: continue
                            hora = hora_tag.text.strip()
                            
                            for i, td in enumerate(fila.find_all("td")):
                                if i < len(fechas_encabezado) and td.text.strip():
                                    datos.append({
                                        "fecha": fechas_encabezado[i],
                                        "hora": hora,
                                        "animalito": normalizar_animal(td.text.strip()),
                                        "loteria": "Lotto Activo"
                                    })
            except Exception as e:
                print(f"⚠️ Error en bloque {fecha_actual.date()}: {e}")
            
            fecha_actual += timedelta(days=7)
            await asyncio.sleep(0.5) # Pausa amigable para no ser bloqueados
            
    return datos
