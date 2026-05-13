"""
SCHEDULER — LOTTOAI PRO
app/core/scheduler.py
Compatible con motor_v13 — ciclo_infinito + startup requeridos por main.py
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


async def _verificar_sorteos(db_factory):
    """
    Cada minuto verifica si acaba de pasar una hora de sorteo.
    Si pasó, llama a ajustar_tras_sorteo con el resultado real de la BD.
    """
    from sqlalchemy import text

    ultima_hora_procesada = None

    while True:
        try:
            ahora = datetime.now(tz)
            hora_actual_str = None

            for hora_str in HORAS_SORTEO:
                sorteo_dt = _hora_str_a_dt(hora_str)
                diff = (ahora - sorteo_dt).total_seconds()
                if 0 <= diff <= 300:  # ventana de 5 minutos post-sorteo
                    hora_actual_str = hora_str
                    break

            if hora_actual_str and hora_actual_str != ultima_hora_procesada:
                async with db_factory() as db:
                    res = await db.execute(text("""
                        SELECT animalito FROM historico
                        WHERE fecha = CURRENT_DATE
                          AND hora = :hora
                          AND loteria = 'Lotto Activo'
                        ORDER BY fecha DESC LIMIT 1
                    """), {"hora": hora_actual_str})
                    row = res.fetchone()

                    if row and row[0]:
                        resultado = row[0]
                        ajuste = await ajustar_tras_sorteo(db, hora_actual_str, resultado)
                        logger.info(f"🔄 Ajuste {hora_actual_str}: {ajuste.get('message')}")
                        ultima_hora_procesada = hora_actual_str

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


async def ciclo_infinito(db_factory):
    """
    Función principal requerida por main.py.
    Lanza en paralelo:
    - Verificación de sorteos cada minuto
    - Generación de plan diario a las 7:30 PM Venezuela
    """
    logger.info("🚀 Scheduler LottoAI Pro iniciado")
    await asyncio.gather(
        _verificar_sorteos(db_factory),
        _ciclo_plan_diario(db_factory),
    )


async def startup(db_factory):
    """
    Hook de arranque requerido por main.py.
    Lanza ciclo_infinito como tarea en background.
    """
    logger.info("🟢 Startup scheduler — lanzando ciclo_infinito")
    asyncio.create_task(ciclo_infinito(db_factory))
