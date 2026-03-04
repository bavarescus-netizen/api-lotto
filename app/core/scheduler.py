"""
SCHEDULER V4 — LOTTOAI PRO
Fixes:
  1. Al arrancar: carga TODOS los sorteos del día actual (no solo histórico semanal)
  2. Al entrar en ventana horaria: si hay sorteos pasados sin capturar, los captura todos
  3. Retry inteligente: si no encuentra resultado, reintenta cada 3 min hasta 5 veces
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
# Sorteos: 8AM a 7PM = horas 8..19
HORAS_SORTEO = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]


async def carga_inicial():
    """
    Al arrancar:
    1. Rellena los últimos 14 días históricos
    2. Captura TODOS los sorteos del día actual (para no perder nada si el servidor se reinició)
    3. Entrena y calibra
    """
    logger.info("📦 Carga inicial — sincronizando últimos 14 días...")
    try:
        async for db in get_db():
            hoy = date.today()
            total = 0

            # Histórico últimas 2 semanas
            for offset in range(0, 14, 7):
                fecha_fin = hoy - timedelta(days=offset)
                fecha_inicio = fecha_fin - timedelta(days=6)
                resultados = await obtener_historico_semana(fecha_inicio, fecha_fin)
                total += await guardar_resultados(db, resultados)

            # ── FIX CRÍTICO: capturar resultados de HOY completo ──
            # Esto evita que un reinicio del servidor pierda sorteos del día
            hoy_resultados = await obtener_resultados_hoy()
            nuevos_hoy = await guardar_resultados(db, hoy_resultados)
            total += nuevos_hoy
            logger.info(f"✅ Carga inicial: {total} registros nuevos ({nuevos_hoy} de hoy)")

            # Entrenar con datos frescos
            ent = await entrenar_modelo(db)
            logger.info(f"🧠 {ent.get('message', 'Entrenamiento OK')}")

            # Calibrar predicciones pendientes
            cal = await calibrar_predicciones(db)
            logger.info(f"🎯 Calibración inicial: {cal.get('calibradas', 0)} validadas")

            # Generar predicción para la hora actual
            ahora = datetime.now(TZ)
            if ahora.hour in HORAS_SORTEO:
                pred = await generar_prediccion(db)
                if pred.get("top3"):
                    top1 = pred["top3"][0]
                    logger.info(f"🔮 Predicción inicial: {top1['animal']} ({top1['porcentaje']}) — Confianza: {pred.get('confianza_idx', 0)}/100")
            break

    except Exception as e:
        logger.error(f"⚠️ Error en carga inicial: {e}")
        import traceback; traceback.print_exc()


async def capturar_y_procesar(db):
    """Captura resultado, calibra, entrena y predice. Retorna True si hubo nuevos."""
    hoy_resultados = await obtener_resultados_hoy()
    nuevos = await guardar_resultados(db, hoy_resultados)

    if nuevos > 0:
        logger.info(f"✅ {nuevos} nuevo(s) resultado(s) capturado(s)")

        cal = await calibrar_predicciones(db)
        logger.info(f"🎯 Calibración: {cal.get('calibradas', 0)} validadas")

        ent = await entrenar_modelo(db)
        logger.info(f"🧠 {ent.get('message', 'OK')}")

        pred = await generar_prediccion(db)
        if pred.get("top3"):
            top1 = pred["top3"][0]
            conf = pred.get("confianza_idx", 0)
            señal = pred.get("señal_texto", "")
            logger.info(f"🔮 Predicción: {top1['animal']} ({top1['porcentaje']}) | Conf: {conf}/100 | {señal}")

        return True
    return False


async def ciclo_infinito():
    logger.info("🚀 LOTTOAI PRO — Sistema de vigilancia activo")
    await carga_inicial()

    intentos_fallidos = 0  # Cuenta reintentos si no llega resultado

    while True:
        try:
            ahora = datetime.now(TZ)
            hora = ahora.hour
            minuto = ahora.minute

            # ── Fuera de horario de sorteos ──
            if hora not in HORAS_SORTEO:
                # Si ya pasaron todos los sorteos del día (después de 7PM)
                # hacer una captura final para asegurar que nada quedó pendiente
                if hora == 20 and minuto < 5:
                    logger.info("🌙 Captura final del día...")
                    async for db in get_db():
                        try:
                            await capturar_y_procesar(db)
                        except Exception as e:
                            logger.error(f"Error captura final: {e}")
                            await db.rollback()
                        break

                logger.info(f"🌙 [{ahora.strftime('%H:%M')}] Fuera de horario. Durmiendo 30 min.")
                await asyncio.sleep(1800)
                continue

            # ── Ventana de captura: minutos 3-25 de cada hora ──
            # (ampliamos a 25 para dar más margen si el resultado tarda)
            if 3 <= minuto <= 25:
                logger.info(f"🔍 [{ahora.strftime('%H:%M')}] Buscando resultado sorteo {hora}:00...")

                async for db in get_db():
                    try:
                        encontrado = await capturar_y_procesar(db)

                        if encontrado:
                            intentos_fallidos = 0
                            # Esperar hasta 3 minutos después de la próxima hora
                            minutos_restantes = 60 - minuto + 3
                            logger.info(f"⏰ Próxima revisión en {minutos_restantes} min")
                            await asyncio.sleep(minutos_restantes * 60)
                        else:
                            intentos_fallidos += 1
                            if intentos_fallidos >= 5:
                                # Después de 5 intentos, asumir que no hay más sorteos
                                # y saltar a la próxima hora
                                logger.warning(f"⚠️ {intentos_fallidos} intentos sin resultado. Saltando a próxima hora.")
                                intentos_fallidos = 0
                                minutos_restantes = 60 - minuto + 3
                                await asyncio.sleep(minutos_restantes * 60)
                            else:
                                logger.info(f"⏳ Sin resultado nuevo (intento {intentos_fallidos}/5). Reintento en 3 min.")
                                await asyncio.sleep(180)

                    except Exception as e:
                        logger.error(f"⚠️ Error en ciclo: {e}")
                        import traceback; traceback.print_exc()
                        await db.rollback()
                        await asyncio.sleep(180)
                    break

            # ── Esperando ventana ──
            else:
                if minuto < 3:
                    espera = (3 - minuto) * 60
                    logger.info(f"⏰ [{ahora.strftime('%H:%M')}] Esperando ventana. {3-minuto} min restantes.")
                else:
                    # minuto > 25: esperar hasta minuto 3 de la próxima hora
                    espera = (60 - minuto + 3) * 60
                    logger.info(f"⏰ [{ahora.strftime('%H:%M')}] Esperando ventana. {(espera)//60} min restantes.")
                await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"💥 Error crítico en ciclo: {e}")
            import traceback; traceback.print_exc()
            await asyncio.sleep(60)
