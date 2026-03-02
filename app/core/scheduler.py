"""
SCHEDULER V3 — LOTTOAI PRO
Ciclo automático: capturar → calibrar → entrenar → predecir
Al arrancar: carga los últimos 14 días para rellenar huecos
"""

import asyncio
from datetime import datetime, timedelta, date
import logging
import pytz
from db import get_db
from app.routes.cargarhist import obtener_historico_semana, guardar_resultados, obtener_resultados_hoy
from app.services.motor_v5 import entrenar_modelo, calibrar_predicciones, generar_prediccion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TZ = pytz.timezone('America/Caracas')
HORAS_SORTEO = [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]


async def carga_inicial():
    """Al arrancar: rellena los últimos 14 días automáticamente"""
    logger.info("📦 Carga inicial — sincronizando últimos 14 días...")
    try:
        async for db in get_db():
            hoy = date.today()
            total = 0
            for offset in range(0, 14, 7):
                fecha_fin = hoy - timedelta(days=offset)
                fecha_inicio = fecha_fin - timedelta(days=6)
                resultados = await obtener_historico_semana(fecha_inicio, fecha_fin)
                total += await guardar_resultados(db, resultados)
            # También carga hoy
            hoy_resultados = await obtener_resultados_hoy()
            total += await guardar_resultados(db, hoy_resultados)
            logger.info(f"✅ Carga inicial: {total} registros nuevos sincronizados")
            # Entrenar con los datos frescos
            ent = await entrenar_modelo(db)
            logger.info(f"🧠 {ent.get('message', 'Entrenamiento inicial OK')}")
            # Calibrar predicciones pendientes
            cal = await calibrar_predicciones(db)
            logger.info(f"🎯 Calibración inicial: {cal.get('calibradas', 0)} validadas")
            break
    except Exception as e:
        logger.error(f"⚠️ Error en carga inicial: {e}")


async def ciclo_infinito():
    logger.info("🚀 LOTTOAI PRO — Sistema de vigilancia activo")

    # Carga inicial al arrancar
    await carga_inicial()

    while True:
        try:
            ahora = datetime.now(TZ)
            hora = ahora.hour
            minuto = ahora.minute

            if hora not in HORAS_SORTEO:
                logger.info(f"🌙 [{ahora.strftime('%H:%M')}] Fuera de horario. Durmiendo 30 min.")
                await asyncio.sleep(1800)
                continue

            if 3 <= minuto <= 20:
                logger.info(f"🔍 [{ahora.strftime('%H:%M')}] Buscando resultado sorteo {hora}:00...")
                espera = 180

                async for db in get_db():
                    try:
                        # PASO 1: Capturar resultado nuevo
                        hoy_resultados = await obtener_resultados_hoy()
                        nuevos = await guardar_resultados(db, hoy_resultados)

                        if nuevos > 0:
                            logger.info(f"✅ {nuevos} nuevo(s) resultado(s) capturado(s)")

                            # PASO 2: Calibrar predicciones pendientes
                            cal = await calibrar_predicciones(db)
                            logger.info(f"🎯 Calibración: {cal.get('calibradas', 0)} validadas")

                            # PASO 3: Re-entrenar
                            ent = await entrenar_modelo(db)
                            logger.info(f"🧠 {ent.get('message', 'OK')}")

                            # PASO 4: Generar predicción próximo sorteo
                            pred = await generar_prediccion(db)
                            if pred.get("top3"):
                                top1 = pred["top3"][0]
                                logger.info(f"🔮 Predicción: {top1['animal']} ({top1['porcentaje']})")

                            espera = (60 - minuto + 3) * 60
                            logger.info(f"⏰ Próxima revisión en {espera//60} min")
                        else:
                            logger.info("⏳ Sin resultado nuevo. Reintento en 3 min.")
                            espera = 180

                    except Exception as e:
                        logger.error(f"⚠️ Error en ciclo: {e}")
                        await db.rollback()
                        espera = 180
                    break

                await asyncio.sleep(espera)

            else:
                if minuto < 3:
                    espera = (3 - minuto) * 60
                else:
                    espera = (60 - minuto + 3) * 60
                logger.info(f"⏰ [{ahora.strftime('%H:%M')}] Esperando ventana. {espera//60} min restantes.")
                await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"💥 Error crítico: {e}")
            await asyncio.sleep(60)
