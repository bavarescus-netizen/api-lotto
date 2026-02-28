import httpx
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta, date
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

URL_ULTIMO = "https://loteriadehoy.com/animalito/lottoactivo/resultados/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def normalizar_animal(texto):
    if not texto: return ""
    # Eliminamos números y caracteres especiales, dejamos solo el nombre
    limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', texto)
    return limpio.upper().strip()

# --- FUNCIÓN QUE USA EL SCHEDULER CADA HORA ---
async def procesar_ultimo_sorteo(db: AsyncSession):
    """Busca el último resultado en la web y lo guarda si no existe."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(URL_ULTIMO, headers=HEADERS, timeout=10)
            if r.status_code != 200: return False
            
            soup = BeautifulSoup(r.text, "html.parser")
            fila = soup.select_one("table tr:nth-of-type(2)") 
            if not fila: return False
            
            columnas = fila.find_all("td")
            if len(columnas) < 3: return False

            # Extraer datos
            fecha_str = columnas[0].text.strip() # Ej: 2026-02-28
            hora = columnas[1].text.strip().upper()
            animal = normalizar_animal(columnas[2].text.strip())

            if not animal: return False

            # Insertar en DB (Evita duplicados con ON CONFLICT)
            query = text("""
                INSERT INTO historico (fecha, hora, animalito)
                VALUES (:f, :h, :a)
                ON CONFLICT (fecha, hora) DO NOTHING
                RETURNING id;
            """)
            result = await db.execute(query, {"f": fecha_str, "h": hora, "a": animal})
            row = result.fetchone()
            
            if row:
                await db.commit()
                return True # Se insertó algo nuevo
            return False # Ya existía
            
    except Exception as e:
        logger.error(f"❌ Error en Scraper Automático: {e}")
        return False

# --- RUTA PARA CARGA MANUAL DESDE EL NAVEGADOR ---
@router.get("/cargar-ultimo")
async def api_cargar_ultimo(db: AsyncSession = Depends(get_db)):
    exito = await procesar_ultimo_sorteo(db)
    if exito:
        return {"status": "success", "message": "Nuevo resultado guardado."}
    return {"status": "no_change", "message": "No hay resultados nuevos o ya estaban cargados."}
