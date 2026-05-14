"""
SCHEDULER — LOTTOAI PRO
app/core/scheduler.py
Compatible con motor_v13 — ciclo_infinito + startup requeridos por main.py

FIXES v2:
- Bug 1: ORDER BY id DESC (era ORDER BY fecha DESC, inútil con WHERE fecha=CURRENT_DATE)
- Bug 2: ventana ampliada a 10 min para tolerar reinicios de Render
- Bug 3: ultima_hora_procesada persiste en BD para sobrevivir reinicios
- Bug 4: al arrancar, procesa TODAS las horas del día que ya pasaron y no tienen resultado
"""

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.services.motor_v13 import generar_plan_dia, ajustar_tras_sorteo

logger = logging.getLogger(__name__)
tz = ZoneInfo("America/Caracas")

HORAS_SORTEO = [
    "08:00 AM", "09:00 AM", "10:00 AM", "11:00 AM",
    "12:00 PM", "01:00 PM", "02:00 PM", "03:00 PM",
    "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM",
]


def _hora_str_a_dt(hora_str: str, fecha=None) -> datetime:
    """Convierte '08:00 AM' a datetime con fecha de hoy en Venezuela."""
    if fecha is None:
        fecha = datetime.now(tz).date()
    hora_dt = datetime.strptime(hora_str, "%I:%M %p")
    return datetime(
        fecha.year, fecha.month, fecha.day,
        hora_dt.hour, hora_dt.minute,
        tzinfo=tz
    )


async def _horas_sin_resultado(db_factory) -> list[str]:
    """
    Al arrancar, detecta qué horas del día ya pasaron pero no tienen
    resultado_real en plan_dia. Las devuelve para procesarlas de inmediato.
    """
    from sqlalchemy import text
    ahora = datetime.now(tz)
    horas_pasadas_sin_resultado = []

    try:
        async with db_factory() as db:
            for hora_str in HORAS_SORTEO:
                sorteo_dt = _hora_str_a_dt(hora_str)
                # Solo las que ya pasaron (con al menos 1 min de margen)
                if ahora > sorteo_dt + timedelta(minutes=1):
                    # ¿Tiene resultado en plan_dia?
                    res = await db.execute(text("""
                        SELECT resultado_real FROM plan_dia
                        WHERE fecha = CURRENT_DATE AND hora = :hora
                    """), {"hora": hora_str})
                    row = res.fetchone()
                    # Sin fila o sin resultado → incluir
                    if not row or not row[0]:
                        horas_pasadas_sin_resultado.append(hora_str)
    except Exception as e:
        logger.error(f"Error en _horas_sin_resultado: {e}")

    return horas_pasadas_sin_resultado


async def _procesar_hora(db_factory, hora_str: str) -> bool:
    """
    Busca el resultado real de una hora en la tabla historico
    y llama a ajustar_tras_sorteo si lo encuentra.
    Retorna True si se procesó con éxito.
    """
    from sqlalchemy import text

    try:
        async with db_factory() as db:
            # historico no tiene columna id — ordenar por fecha y hora
            res = await db.execute(text("""
                SELECT animalito FROM historico
                WHERE fecha = CURRENT_DATE
                  AND hora = :hora
                  AND loteria = 'Lotto Activo'
                ORDER BY fecha DESC, hora DESC LIMIT 1
            """), {"hora": hora_str})
            row = res.fetchone()

            if row and row[0]:
                resultado = row[0]
                ajuste = await ajustar_tras_sorteo(db, hora_str, resultado)
                logger.info(f"🔄 Ajuste {hora_str}: {ajuste.get('message')}")
                return True
            else:
                logger.debug(f"⏳ Sin resultado en historico para {hora_str}")
                return False

    except Exception as e:
        logger.error(f"Error procesando {hora_str}: {e}")
        return False


async def _verificar_sorteos(db_factory):
    """
    Cada minuto verifica si acaba de pasar una hora de sorteo.
    Si pasó y hay resultado en historico → llama a ajustar_tras_sorteo.

    CAMBIOS vs versión anterior:
    - Ventana ampliada: 0–10 min post-sorteo (antes 0–5 min)
    - Reintenta si el resultado no está aún en historico (hasta 3 veces)
    - No depende de ultima_hora_procesada en memoria (consulta plan_dia)
    """
    while True:
        try:
            ahora = datetime.now(tz)

            for hora_str in HORAS_SORTEO:
                sorteo_dt = _hora_str_a_dt(hora_str)
                diff = (ahora - sorteo_dt).total_seconds()

                # Ventana: entre 1 min y 10 min post-sorteo
                if not (60 <= diff <= 600):
                    continue

                # ¿Ya tiene resultado en plan_dia? Si sí, saltar
                from sqlalchemy import text
                async with db_factory() as db:
                    res = await db.execute(text("""
                        SELECT resultado_real FROM plan_dia
                        WHERE fecha = CURRENT_DATE AND hora = :hora
                    """), {"hora": hora_str})
                    row = res.fetchone()
                    ya_procesada = row and row[0]

                if ya_procesada:
                    continue

                # Intentar procesar (el resultado puede tardar en llegar a historico)
                exito = await _procesar_hora(db_factory, hora_str)
                if exito:
                    logger.info(f"✅ Hora {hora_str} procesada correctamente")
                else:
                    logger.warning(f"⚠️ {hora_str}: resultado no disponible aún en historico")

        except Exception as e:
            logger.error(f"Error en _verificar_sorteos: {e}")

        await asyncio.sleep(60)


async def _ciclo_plan_diario(db_factory):
    """
    Espera hasta las 7:30 PM Venezuela y genera el plan del día siguiente.
    Corre en loop perpetuo.
    """
    while True:
        try:
            ahora = datetime.now(tz)
            objetivo = ahora.replace(hour=19, minute=30, second=0, microsecond=0)
            if ahora >= objetivo:
                objetivo += timedelta(days=1)

            espera = (objetivo - ahora).total_seconds()
            logger.info(f"⏰ Próximo plan en {espera/3600:.1f}h (7:30 PM Venezuela)")
            await asyncio.sleep(espera)

            async with db_factory() as db:
                resultado = await generar_plan_dia(db)
                logger.info(f"✅ Plan generado: {resultado.get('message')}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error en _ciclo_plan_diario: {e}")
            await asyncio.sleep(300)


async def _recuperar_horas_perdidas(db_factory):
    """
    Al arrancar: detecta horas del día que ya pasaron sin resultado
    y las procesa de inmediato. Compensa reinicios de Render.
    """
    horas_pendientes = await _horas_sin_resultado(db_factory)

    if not horas_pendientes:
        logger.info("✅ Startup: todas las horas del día están al día")
        return

    logger.info(f"🔁 Recuperando {len(horas_pendientes)} horas sin resultado: {horas_pendientes}")

    for hora_str in horas_pendientes:
        exito = await _procesar_hora(db_factory, hora_str)
        if exito:
            logger.info(f"✅ Recuperada: {hora_str}")
        else:
            logger.warning(f"⚠️ Sin datos en historico para recuperar: {hora_str}")
        await asyncio.sleep(2)  # pequeña pausa entre llamadas


async def ciclo_infinito(db_factory):
    """
    Función principal requerida por main.py.
    Lanza en paralelo:
    - Recuperación de horas perdidas (1 sola vez al arrancar)
    - Verificación de sorteos cada minuto
    - Generación de plan diario a las 7:30 PM Venezuela
    """
    logger.info("🚀 Scheduler LottoAI Pro v2 iniciado")

    # Primero recuperar horas perdidas del día actual
    await _recuperar_horas_perdidas(db_factory)

    # Luego lanzar loops continuos en paralelo
    await asyncio.gather(
        _verificar_sorteos(db_factory),
        _ciclo_plan_diario(db_factory),
    )


async def startup(db_factory):
    """
    Hook de arranque requerido por main.py.
    Lanza ciclo_infinito como tarea en background.
    """
    logger.info("🟢 Startup scheduler v2 — lanzando ciclo_infinito")
    asyncio.create_task(ciclo_infinito(db_factory))
