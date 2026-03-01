"""
SCHEDULER V2 — LOTTOAI PRO
Ciclo automático: capturar → calibrar → entrenar → predecir
Reemplaza: scheduler.py (versión anterior)
"""

import asyncio
from datetime import datetime
import logging
import pytz
from sqlalchemy import text
from db import get_db
from app.routes.cargarhist import procesar_ultimo_sorteo
from app.services.motor_v5 import entrenar_modelo, calibrar_predicciones, generar_prediccion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TZ = pytz.timezone('America/Caracas')

# Horarios de sorteo (horas en Venezuela)
HORAS_SORTEO = [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]


async def ciclo_infinito():
    logger.info("🚀 LOTTOAI PRO — Sistema de vigilancia activo")

    while True:
        try:
            ahora = datetime.now(TZ)
            hora = ahora.hour
            minuto = ahora.minute

            # Solo operar en horario de sorteos
            if hora not in HORAS_SORTEO:
                logger.info(f"🌙 [{ahora.strftime('%H:%M')}] Fuera de horario. Durmiendo 30 min.")
                await asyncio.sleep(1800)
                continue

            # ─────────────────────────────────────────────
            # VENTANA DE CAPTURA: minutos 3 al 20
            # El sorteo ocurre a la hora en punto,
            # esperamos unos minutos para que aparezca en la web
            # ─────────────────────────────────────────────
            if 3 <= minuto <= 20:
                logger.info(f"🔍 [{ahora.strftime('%H:%M')}] Buscando resultado del sorteo de las {hora}:00...")

                async for db in get_db():
                    try:
                        # PASO 1: Capturar resultado nuevo
                        nuevo = await procesar_ultimo_sorteo(db)

                        if nuevo:
                            logger.info(f"✅ Nuevo resultado capturado — {ahora.strftime('%H:%M')}")

                            # PASO 2: Calibrar predicciones pendientes
                            cal = await calibrar_predicciones(db)
                            logger.info(f"🎯 Calibración: {cal.get('calibradas', 0)} predicciones validadas")

                            # PASO 3: Re-entrenar con el nuevo dato
                            ent = await entrenar_modelo(db)
                            logger.info(f"🧠 {ent.get('message', 'Entrenamiento completado')}")

                            # PASO 4: Generar predicción para el próximo sorteo
                            pred = await generar_prediccion(db)
                            if pred.get("top3"):
                                top1 = pred["top3"][0]
                                logger.info(f"🔮 Próxima predicción: {top1['animal']} ({top1['porcentaje']})")

                            # Dormir hasta 3 minutos después del próximo sorteo
                            espera = (60 - minuto + 3) * 60
                            logger.info(f"⏰ Próxima revisión en {(espera//60)} minutos")

                        else:
                            # Resultado no disponible aún, reintentar en 3 minutos
                            logger.info(f"⏳ Resultado aún no disponible. Reintento en 3 min.")
                            espera = 180

                    except Exception as e:
                        logger.error(f"⚠️ Error en ciclo principal: {e}")
                        await db.rollback()
                        espera = 180

                    break  # Salir del async for

                await asyncio.sleep(espera)

            else:
                # Calcular cuánto falta para el minuto 3
                if minuto < 3:
                    espera = (3 - minuto) * 60
                else:
                    # Ya pasó el minuto 20, esperar al próximo sorteo
                    minutos_restantes = (60 - minuto + 3)
                    espera = minutos_restantes * 60

                logger.info(f"⏰ [{ahora.strftime('%H:%M')}] Esperando ventana de captura. {espera//60} min restantes.")
                await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"💥 Error crítico en ciclo: {e}")
            await asyncio.sleep(60)
