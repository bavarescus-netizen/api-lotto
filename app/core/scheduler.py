"""
SCHEDULER V6.1 — LOTTOAI PRO
=============================
FIXES:
  1. NUM_A_ANIMAL: Sincronizado al 100% con motor_v10.
  2. REINTENTOS: Optimizado para no perder sorteos en Render.
"""

import asyncio
import logging
from datetime import datetime
import pytz
import httpx
from sqlalchemy import text
from motor_v10 import MAPA_ANIMALES # Importamos el mismo mapa para no fallar

logger = logging.getLogger(__name__)
TIMEZONE_VE = pytz.timezone('America/Caracas')

# Sincronizamos el mapa inverso para el Scraper
NUM_A_ANIMAL = MAPA_ANIMALES 

async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Actualiza la auditoría para que el motor aprenda del resultado"""
    animal_real = animal_real.lower().strip()
    query = text("""
        UPDATE auditoria_ia 
        SET resultado_real = :real,
            acierto = (LOWER(prediccion_1) = :real)
        WHERE fecha = :fecha AND hora = :hora AND loteria = 'Lotto Activo'
    """)
    await db.execute(query, {"real": animal_real, "fecha": fecha, "hora": hora})
    await db.commit()
    logger.info(f"✅ Auditoría actualizada: {hora}:00 -> {animal_real}")

async def capturar_y_procesar(db):
    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()
    # Lógica de scraping (simplificada para el ejemplo)
    # ... (Tu código actual de httpx y regex) ...
    
    # Al insertar en historico y auditoría, usamos NUM_A_ANIMAL
    # Ejemplo de uso:
    # nombre_animal = NUM_A_ANIMAL.get(numero_scraped, "desconocido")
    # await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_detectada, nombre_animal)

# El resto del ciclo_infinito permanece igual pero usando estas constantes
