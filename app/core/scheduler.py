import asyncio
import logging
import re
from datetime import datetime
import pytz
import httpx
from sqlalchemy import text

# Importaciones locales
from db import AsyncSessionLocal
from app.services.motor_v10 import MAPA_ANIMALES 

logger = logging.getLogger(__name__)

# Configuración Regional
TIMEZONE_VE = pytz.timezone('America/Caracas')
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
HORAS_SORTEO = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

# Sincronización automática con el motor
NUM_A_ANIMAL = MAPA_ANIMALES 

async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Actualiza la auditoría para que el motor aprenda del resultado"""
    if not animal_real:
        return
        
    animal_real = animal_real.lower().strip()
    # Actualizamos el acierto comparando con las 3 predicciones del motor_v10
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
        logger.info(f"✅ Auditoría sincronizada: {hora}:00 -> {animal_real}")
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error en actualización de auditoría: {e}")

async def capturar_y_procesar(db):
    """Scraping con Regex robusto y guardado en DB"""
    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()
    
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(BASE_URL)
            if r.status_code == 200:
                html = r.text
                # Regex mejorado: captura hora, número y nombre ignorando posibles etiquetas HTML intermedias
                patron = r"(\d{2}):00.*?(\d{1,2})\s*[-–]\s*([a-zA-Záéíóúñ]+)"
                matches = re.findall(patron, html, re.DOTALL)
                
                if not matches:
                    logger.warning("⚠️ No se detectaron resultados en el HTML actual.")
                    return

                for hora_str, num, nombre in matches:
                    hora_int = int(hora_str)
                    # Priorizamos el nombre del MAPA_ANIMALES para evitar inconsistencias (ej. Camello vs Camello )
                    nombre_normalizado = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())
                    
                    # 1. Insertar en histórico
                    await db.execute(text("""
                        INSERT INTO historico (fecha, hora, numero, animal, loteria)
                        VALUES (:f, :h, :n, :a, 'Lotto Activo')
                        ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    """), {"f": fecha_hoy, "h": hora_int, "n": num, "a": nombre_normalizado})
                    
                    # 2. Sincronizar con la tabla de auditoría
                    await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_int, nombre_normalizado)
                
                await db.commit()
            else:
                logger.warning(f"⚠️ Error de conexión Web: {r.status_code}")
                
    except Exception as e:
        logger.error(f"❌ Error en capturar_y_procesar: {e}")

async def ciclo_infinito():
    """Mantiene el bot activo respetando los límites de Render y la zona horaria"""
    logger.info("🚀 [LottoAI PRO] Ciclo V6.1 Iniciado")
    
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            hora_actual = ahora.hour
            
            # Ventana operativa (incluye margen para resultados tardíos)
            if 8 <= hora_actual <= 20:
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                
                # Durante el día, revisamos cada 5 min
                espera = 300 
            else:
                # En la noche, revisamos cada 45 min para ahorrar recursos
                if hora_actual == 23: logger.info("🌙 Modo latencia nocturna activado.")
                espera = 2700 

            await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"❌ Error crítico en ciclo: {e}")
            await asyncio.sleep(60)
