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

BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
LOTERIA = "Lotto Activo"

def normalizar_animal(texto):
    if not texto: return ""
    limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ\s]', '', texto)
    partes = limpio.strip().split()
    return partes[-1].lower() if partes else ""

def normalizar_hora(hora_texto):
    hora = hora_texto.strip().upper()
    partes = hora.split(':')
    if len(partes) >= 2:
        return f"{partes[0].zfill(2)}:{':'.join(partes[1:])}"
    return hora

async def obtener_resultados_hoy():
    url = f"{BASE_URL}/resultados/"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=HEADERS)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            fecha_hoy = date.today()
            resultados = []
            for leyenda in soup.find_all("div", class_="circle-legend"):
                h4 = leyenda.find("h4")
                h5 = leyenda.find("h5")
                if not h4 or not h5: continue
                animal = normalizar_animal(h4.text)
                match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', h5.text, re.IGNORECASE)
                if not match or not animal: continue
                hora = normalizar_hora(match.group(1))
                resultados.append({
                    "fecha": fecha_hoy,
                    "hora": hora,
                    "animalito": animal,
                    "loteria": LOTERIA
                })
            return resultados
    except Exception as e:
        logger.error(f"Error scraper hoy: {e}")
        return []

async def obtener_historico_semana(fecha_inicio: date, fecha_fin: date):
    url = f"{BASE_URL}/historico/{fecha_inicio}/{fecha_fin}/"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=HEADERS)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            tabla = soup.find("table")
            if not tabla: return []
            fechas = []
            for th in tabla.find_all("th")[1:]:
                try:
                    fechas.append(datetime.strptime(th.text.strip(), "%Y-%m-%d").date())
                except: pass
            resultados = []
            for fila in tabla.find_all("tr")[1:]:
                th_hora = fila.find("th")
                if not th_hora: continue
                hora = normalizar_hora(th_hora.text.strip())
                for i, celda in enumerate(fila.find_all("td")):
                    if i >= len(fechas): break
                    animal = normalizar_animal(celda.text.strip())
                    if animal:
                        resultados.append({
                            "fecha": fechas[i],
                            "hora": hora,
                            "animalito": animal,
                            "loteria": LOTERIA
                        })
            return resultados
    except Exception as e:
        logger.error(f"Error histórico: {e}")
        return []

# --- FUNCIÓN DE GUARDADO SEGURA ---
async def guardar_resultados(db: AsyncSession, resultados: list) -> int:
    if not resultados: return 0
    insertados = 0
    for r in resultados:
        try:
            # IMPORTANTE: Eliminado RETURNING id para evitar error en Neon
            await db.execute(text("""
                INSERT INTO historico (fecha, hora, animalito, loteria)
                VALUES (:fecha, :hora, :animalito, :loteria)
                ON CONFLICT (fecha, hora) DO NOTHING
            """), r)
            insertados += 1
        except Exception as e:
            # Rollback individual para no bloquear la conexión
            await db.rollback()
            logger.warning(f"Salto de registro por error: {e}")
            continue 
            
    if insertados > 0:
        await db.commit()
    return insertados

# --- RUTAS ---

@router.get("/cargar-ultimo")
async def api_cargar_ultimo(db: AsyncSession = Depends(get_db)):
    resultados = await obtener_resultados_hoy()
    insertados = await guardar_resultados(db, resultados)
    return {"status": "success", "nuevos": insertados}

@router.get("/cargar-rango")
async def api_cargar_rango(desde: str, hasta: str, db: AsyncSession = Depends(get_db)):
    try:
        fecha_inicio = datetime.strptime(desde, "%Y-%m-%d").date()
        fecha_fin = datetime.strptime(hasta, "%Y-%m-%d").date()
    except:
        return {"status": "error", "message": "Formato YYYY-MM-DD requerido"}
    
    total = 0
    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
        fin_bloque = min(fecha_actual + timedelta(days=6), fecha_fin)
        resultados = await obtener_historico_semana(fecha_actual, fin_bloque)
        total += await guardar_resultados(db, resultados)
        fecha_actual += timedelta(days=7)
    return {"status": "success", "total_cargado": total}
