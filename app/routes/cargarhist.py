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

# ── FUENTE CAMBIADA: lotoven.com (loteriadehoy.com devuelve 403) ──
BASE_URL = "https://lotoven.com/animalito/lottoactivo"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.8",
}
LOTERIA = "Lotto Activo"

HORAS_VALIDAS = {
    "08:00 AM", "09:00 AM", "10:00 AM", "11:00 AM",
    "12:00 PM", "01:00 PM", "02:00 PM", "03:00 PM",
    "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM",
}


def normalizar_animal(texto: str) -> str:
    if not texto:
        return ""
    limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ\s]', '', texto)
    partes = limpio.strip().split()
    return partes[-1].lower() if partes else ""


def normalizar_hora(hora_texto: str) -> str:
    """Convierte '8:00 AM', '8:00AM', '08:00 AM' → '08:00 AM'"""
    hora = hora_texto.strip().upper().replace("\xa0", " ")
    hora = re.sub(r'\s+', ' ', hora)
    # Insertar espacio antes de AM/PM si no lo hay
    hora = re.sub(r'(\d)(AM|PM)', r'\1 \2', hora)
    partes = hora.split(':')
    if len(partes) >= 2:
        hora_num = partes[0].zfill(2)
        resto = ':'.join(partes[1:])
        return f"{hora_num}:{resto}"
    return hora


async def obtener_resultados_hoy() -> list:
    """
    Scrapea los resultados de hoy desde lotoven.com/animalito/lottoactivo/resultados/
    """
    url = f"{BASE_URL}/resultados/"
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(url, headers=HEADERS)
            logger.info(f"Scraper hoy — status: {r.status_code} url: {url}")
            if r.status_code != 200:
                logger.error(f"Scraper hoy falló: HTTP {r.status_code}")
                return []

            soup = BeautifulSoup(r.text, "html.parser")
            fecha_hoy = date.today()
            resultados = []

            # Lotoven usa tabla con filas de hora + animal
            # Buscar patrones de hora + animal en el HTML
            # Patrón 1: divs con clase circle-legend o similar
            for leyenda in soup.find_all(["div", "li"], class_=re.compile(r'circle|result|sorteo|animal', re.I)):
                texto = leyenda.get_text(" ", strip=True)
                match_hora = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', texto, re.IGNORECASE)
                match_animal = re.search(r'\b([A-Za-záéíóúñ]{4,})\b', texto)
                if match_hora and match_animal:
                    hora = normalizar_hora(match_hora.group(1))
                    animal = normalizar_animal(match_animal.group(1))
                    if hora in HORAS_VALIDAS and animal:
                        resultados.append({
                            "fecha": fecha_hoy,
                            "hora": hora,
                            "animalito": animal,
                            "loteria": LOTERIA
                        })

            # Patrón 2: texto plano con hora + número + animal (formato lotoven)
            if not resultados:
                texto_completo = soup.get_text(" ", strip=True)
                patron = re.finditer(
                    r'(\d{1,2}:\d{2}\s*[AP]M)\s*[·\-]?\s*\d{0,2}\s*([A-Za-záéíóúñ]{4,})',
                    texto_completo, re.IGNORECASE
                )
                for m in patron:
                    hora = normalizar_hora(m.group(1))
                    animal = normalizar_animal(m.group(2))
                    if hora in HORAS_VALIDAS and animal:
                        resultados.append({
                            "fecha": fecha_hoy,
                            "hora": hora,
                            "animalito": animal,
                            "loteria": LOTERIA
                        })

            # Deduplicar por hora (quedarse con el primero)
            vistos = set()
            unicos = []
            for res in resultados:
                if res["hora"] not in vistos:
                    vistos.add(res["hora"])
                    unicos.append(res)

            logger.info(f"Scraper hoy: {len(unicos)} sorteos encontrados")
            return unicos

    except Exception as e:
        logger.error(f"Error scraper hoy: {e}")
        return []


async def obtener_historico_semana(fecha_inicio: date, fecha_fin: date) -> list:
    """
    Scrapea el histórico semanal desde lotoven.com
    URL: /animalito/lottoactivo/historico/YYYY-MM-DD/YYYY-MM-DD/
    """
    url = f"{BASE_URL}/historico/{fecha_inicio}/{fecha_fin}/"
    try:
        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            r = await client.get(url, headers=HEADERS)
            logger.info(f"Scraper historico — status: {r.status_code} url: {url}")
            if r.status_code != 200:
                logger.error(f"Historico falló: HTTP {r.status_code}")
                return []

            soup = BeautifulSoup(r.text, "html.parser")
            tabla = soup.find("table")
            if not tabla:
                logger.warning(f"No se encontró tabla en {url}")
                return []

            # Extraer fechas de los encabezados
            fechas = []
            for th in tabla.find_all("th")[1:]:
                texto = th.text.strip()
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
                    try:
                        fechas.append(datetime.strptime(texto, fmt).date())
                        break
                    except:
                        pass

            resultados = []
            for fila in tabla.find_all("tr")[1:]:
                th_hora = fila.find("th")
                if not th_hora:
                    continue
                hora = normalizar_hora(th_hora.text.strip())
                if hora not in HORAS_VALIDAS:
                    continue
                for i, celda in enumerate(fila.find_all("td")):
                    if i >= len(fechas):
                        break
                    animal = normalizar_animal(celda.text.strip())
                    if animal:
                        resultados.append({
                            "fecha": fechas[i],
                            "hora": hora,
                            "animalito": animal,
                            "loteria": LOTERIA
                        })

            logger.info(f"Historico {fecha_inicio}→{fecha_fin}: {len(resultados)} registros")
            return resultados

    except Exception as e:
        logger.error(f"Error historico semana: {e}")
        return []


async def guardar_resultados(db: AsyncSession, resultados: list) -> int:
    if not resultados:
        return 0
    insertados = 0
    for r in resultados:
        try:
            res = await db.execute(text("""
                INSERT INTO historico (fecha, hora, animalito, loteria)
                VALUES (:fecha, :hora, :animalito, :loteria)
                ON CONFLICT (fecha, hora) DO NOTHING
            """), r)
            await db.commit()
            if res.rowcount > 0:
                insertados += 1
        except Exception as e:
            await db.rollback()
            logger.warning(f"Error insertando {r.get('fecha')} {r.get('hora')}: {e}")
    return insertados


# ═══════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.get("/cargar-ultimo")
async def api_cargar_ultimo(db: AsyncSession = Depends(get_db)):
    """Jala los resultados de hoy desde lotoven.com e inserta los nuevos."""
    resultados = await obtener_resultados_hoy()
    insertados = await guardar_resultados(db, resultados)
    return {
        "status": "success",
        "encontrados": len(resultados),
        "nuevos": insertados,
        "detalle": resultados,
        "message": f"Hoy: {len(resultados)} encontrados, {insertados} nuevos guardados."
    }


@router.get("/cargar-semana")
async def api_cargar_semana(db: AsyncSession = Depends(get_db)):
    """Jala las últimas 2 semanas de resultados."""
    hoy = date.today()
    total = 0
    for offset in range(0, 14, 7):
        fecha_fin = hoy - timedelta(days=offset)
        fecha_inicio = fecha_fin - timedelta(days=6)
        resultados = await obtener_historico_semana(fecha_inicio, fecha_fin)
        total += await guardar_resultados(db, resultados)
    return {
        "status": "success",
        "nuevos_registros": total,
        "message": f"{total} registros nuevos en los últimos 14 días."
    }


@router.get("/cargar-rango")
async def api_cargar_rango(
    desde: str,
    hasta: str,
    db: AsyncSession = Depends(get_db)
):
    """Jala resultados de un rango de fechas. Formato: YYYY-MM-DD"""
    try:
        fecha_inicio = datetime.strptime(desde, "%Y-%m-%d").date()
        fecha_fin = datetime.strptime(hasta, "%Y-%m-%d").date()
    except Exception:
        return {"status": "error", "message": "Formato inválido. Use YYYY-MM-DD"}

    total = 0
    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
        fin_bloque = min(fecha_actual + timedelta(days=6), fecha_fin)
        resultados = await obtener_historico_semana(fecha_actual, fin_bloque)
        total += await guardar_resultados(db, resultados)
        fecha_actual += timedelta(days=7)

    return {
        "status": "success",
        "rango": f"{desde} al {hasta}",
        "nuevos_registros": total,
        "message": f"{total} registros nuevos."
    }


@router.get("/test-scraper")
async def test_scraper():
    """Endpoint de diagnóstico — muestra lo que el scraper encuentra sin guardar nada."""
    resultados = await obtener_resultados_hoy()
    return {
        "status": "ok" if resultados else "sin_datos",
        "encontrados": len(resultados),
        "detalle": resultados,
        "fuente": f"{BASE_URL}/resultados/",
    }
