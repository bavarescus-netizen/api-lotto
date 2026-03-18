"""
SCHEDULER V6.1 — LOTTOAI PRO
=============================
FIXES:
  1. ModuleNotFoundError: Importación corregida para Render.
  2. ImportError: Función ciclo_infinito definida explícitamente.
  3. NUM_A_ANIMAL: Sincronizado al 100% con motor_v10.
  4. REINTENTOS: Optimizado para no perder sorteos en Render.
"""

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

# Sincronizamos el mapa inverso para el Scraper
NUM_A_ANIMAL = MAPA_ANIMALES 

async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Actualiza la auditoría para que el motor aprenda del resultado"""
    if not animal_real:
        return
        
    animal_real = animal_real.lower().strip()
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
        logger.info(f"✅ Auditoría actualizada: {hora}:00 -> {animal_real}")
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error actualizando auditoría: {e}")

async def capturar_y_procesar(db):
    """Lógica de scraping y actualización de base de datos"""
    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()
    
    logger.info(f"🔍 Iniciando captura de resultados para {fecha_hoy}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(BASE_URL)
            if r.status_code == 200:
                html = r.text
                # Regex robusto para capturar número y animal
                # Ajustado según la estructura típica de loteriadehoy
                patron = r"(\d{2}):00.*?(\d{1,2})\s*-\s*([a-zA-Záéíóúñ]+)"
                matches = re.findall(patron, html, re.DOTALL)
                
                for hora_str, num, nombre in matches:
                    hora_int = int(hora_str)
                    nombre_normalizado = NUM_A_ANIMAL.get(str(int(num)), nombre.lower())
                    
                    # 1. Insertar en histórico si no existe
                    await db.execute(text("""
                        INSERT INTO historico (fecha, hora, numero, animal, loteria)
                        VALUES (:f, :h, :n, :a, 'Lotto Activo')
                        ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    """), {"f": fecha_hoy, "h": hora_int, "n": num, "a": nombre_normalizado})
                    
                    # 2. Actualizar Auditoría de la IA
                    await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_int, nombre_normalizado)
                
                await db.commit()
            else:
                logger.warning(f"⚠️ No se pudo acceder a la web. Status: {r.status_code}")
                
    except Exception as e:
        logger.error(f"❌ Error en capturar_y_procesar: {e}")

async def ciclo_infinito():
    """
    FUNCIÓN PRINCIPAL: Invocada por main.py para mantener el bot activo.
    """
    logger.info("🚀 [LottoAI PRO] Ciclo Infinito de Control Iniciado")
    
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            hora_actual = ahora.hour
            minuto_actual = ahora.minute

            # Ventana de operación: 8 AM a 8 PM (incluye cierre de resultados)
            if 8 <= hora_actual <= 20:
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                
                # Esperar 5 minutos entre chequeos durante el día
                espera = 300 
            else:
                # Modo ahorro de energía en la madrugada
                logger.info("🌙 Fuera de horario. Sistema en modo latencia.")
                espera = 1800 # 30 minutos

            await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"❌ Error crítico en ciclo_infinito: {e}")
            await asyncio.sleep(60)
