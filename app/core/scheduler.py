import asyncio
import logging
import re
from datetime import datetime
import pytz
import httpx
from sqlalchemy import text

# Manteniendo tus rutas originales
from db import AsyncSessionLocal
from app.services.motor_v10 import MAPA_ANIMALES

logger = logging.getLogger(__name__)

# Configuración Regional
TIMEZONE_VE = pytz.timezone('America/Caracas')
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
HORAS_SORTEO = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

# Sincronización con el motor V10
NUM_A_ANIMAL = MAPA_ANIMALES 

async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Actualiza la auditoría para que el motor aprenda del resultado"""
    if not animal_real:
        return
        
    animal_real = animal_real.lower().strip()
    
    # Corregido: Validación de acierto contra prediccion_1
    query = text("""
        UPDATE auditoria_ia 
        SET resultado_real = :real,
            acierto = (LOWER(TRIM(prediccion_1)) = :real)
        WHERE fecha = :fecha 
          AND hora = :hora 
          AND loteria = 'Lotto Activo'
    """)
    try:
        await db.execute(query, {"real": animal_real, "fecha": fecha, "hora": hora})
        await db.commit()
        logger.info(f"✅ Auditoría al día: {hora}:00 -> {animal_real}")
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error actualizando auditoría: {e}")

async def capturar_y_procesar(db):
    """Scraping con Regex y guardado en DB"""
    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()
    
    try:
        # Añadido follow_redirects para estabilidad en Render
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(BASE_URL)
            if r.status_code == 200:
                html = r.text
                # Regex mejorado para capturar con o sin etiquetas HTML extras
                patron = r"(\d{2}):00.*?(\d{1,2})\s*[-–]\s*([a-zA-Záéíóúñ]+)"
                matches = re.findall(patron, html, re.DOTALL)
                
                for hora_str, num, nombre in matches:
                    hora_int = int(hora_str)
                    # Normalización forzada con el MAPA_ANIMALES del motor
                    nombre_normalizado = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())
                    
                    # 1. Registro en histórico (ON CONFLICT para evitar duplicados)
                    await db.execute(text("""
                        INSERT INTO historico (fecha, hora, numero, animal, loteria)
                        VALUES (:f, :h, :n, :a, 'Lotto Activo')
                        ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    """), {"f": fecha_hoy, "h": hora_int, "n": num, "a": nombre_normalizado})
                    
                    # 2. Actualización de aprendizaje
                    await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_int, nombre_normalizado)
                
                await db.commit()
            else:
                logger.warning(f"⚠️ Web no disponible. Status: {r.status_code}")
                
    except Exception as e:
        logger.error(f"❌ Error en scraping: {e}")

async def ciclo_infinito():
    """Control de ejecución continua"""
    logger.info("🚀 [LottoAI PRO] Control de Scraper V6.1 Activo")
    
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            hora_actual = ahora.hour
            
            # Operación durante sorteos
            if 8 <= hora_actual <= 20:
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                espera = 300 # 5 minutos
            else:
                # Latencia nocturna
                espera = 1800 # 30 minutos

            await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"❌ Error en ciclo: {e}")
            await asyncio.sleep(60)
