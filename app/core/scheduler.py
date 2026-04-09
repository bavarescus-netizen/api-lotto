import asyncio
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx
from sqlalchemy import text
from db import AsyncSessionLocal
from app.services.motor_v10 import MAPA_ANIMALES

logger = logging.getLogger(__name__)

TIMEZONE_VE = ZoneInfo('America/Caracas')
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
NUM_A_ANIMAL = MAPA_ANIMALES


async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    if not animal_real:
        return
    animal_real = animal_real.lower().strip()
    try:
        await db.execute(text("""
            UPDATE auditoria_ia 
            SET resultado_real = :real,
                acierto = (LOWER(TRIM(prediccion_1)) = :real)
            WHERE fecha = :fecha AND hora = :hora AND loteria = 'Lotto Activo'
        """), {"real": animal_real, "fecha": fecha, "hora": hora})
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error auditoría: {e}")


async def capturar_y_procesar(db):
    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": "https://www.google.com/"
    }

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            r = await client.get(BASE_URL)
            if r.status_code == 200:
                html = r.text
                patron = r"(\d{2}):00.*?(\d{1,2})\s*[-–]\s*([a-zA-Záéíóúñ]+)"
                matches = re.findall(patron, html, re.DOTALL)

                for hora_str, num, nombre in matches:
                    nombre_normalizado = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())
                    await db.execute(text("""
                        INSERT INTO historico (fecha, hora, animalito, loteria)
                        VALUES (:f, :h, :a, 'Lotto Activo')
                        ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    """), {"f": fecha_hoy, "h": hora_str, "a": nombre_normalizado})
                    await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_str, nombre_normalizado)

                await db.commit()
            else:
                logger.warning(f"⚠️ Web bloqueada o caída (Status {r.status_code})")
    except Exception as e:
        logger.error(f"❌ Error en capturar_y_procesar: {e}")


async def ciclo_infinito():
    logger.info("🚀 [LottoAI PRO] Scraper V6.1 en ejecución")
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            if 8 <= ahora.hour <= 20:
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                espera = 300
            else:
                espera = 1800
            await asyncio.sleep(espera)
        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
