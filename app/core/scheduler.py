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

# ── Contador para saber cuándo recalcular rentabilidad ──
_sorteos_desde_ultimo_recalculo = 0
_RECALCULO_CADA_N = 12  # una vez por hora aprox (12 sorteos/día)


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


async def recalcular_rentabilidad_automatico(db):
    """
    Recalcula la tabla rentabilidad_hora desde auditoria_ia.
    Se llama automáticamente cada N sorteos para que el motor
    siempre use las horas más rentables según datos reales.
    """
    try:
        from app.services.motor_v10 import (
            calcular_rentabilidad_horas,
            actualizar_tabla_rentabilidad,
        )
        rentabilidad = await calcular_rentabilidad_horas(db)
        await actualizar_tabla_rentabilidad(db, rentabilidad)
        horas_top = sorted(
            [(h, d.get("efectividad_top3", 0)) for h, d in rentabilidad.items()],
            key=lambda x: x[1], reverse=True
        )[:3]
        top_str = " | ".join([f"{h}={ef:.1f}%" for h, ef in horas_top])
        logger.info(f"📊 Rentabilidad actualizada — Top3 horas: {top_str}")
    except Exception as e:
        logger.warning(f"⚠️ Error recalculando rentabilidad: {e}")


async def capturar_y_procesar(db):
    global _sorteos_desde_ultimo_recalculo

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

                nuevos_insertados = []

                for hora_str, num, nombre in matches:
                    nombre_normalizado = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())

                    # Insertar en histórico solo si es nuevo
                    res = await db.execute(text("""
                        INSERT INTO historico (fecha, hora, animalito, loteria)
                        VALUES (:f, :h, :a, 'Lotto Activo')
                        ON CONFLICT (fecha, hora, loteria) DO NOTHING
                        RETURNING hora
                    """), {"f": fecha_hoy, "h": hora_str, "a": nombre_normalizado})

                    insertado = res.fetchone()
                    if insertado:
                        nuevos_insertados.append((hora_str, nombre_normalizado))

                    await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_str, nombre_normalizado)

                await db.commit()

                # ── PASO 2: Micro-aprendizaje solo para sorteos NUEVOS ──
                if nuevos_insertados:
                    try:
                        from app.services.motor_v10 import aprender_sorteo
                        for hora_ap, nombre_ap in nuevos_insertados:
                            resultado = await aprender_sorteo(db, fecha_hoy, hora_ap, nombre_ap)
                            if resultado.get("status") == "success":
                                señal = resultado.get("señal_dominante", "?")
                                acerto = "TOP1 ✅" if resultado.get("acerto_top1") else "TOP3 ✅" if resultado.get("acerto_top3") else "FALLO ❌"
                                logger.info(f"🧠 Aprendizaje {hora_ap}: {acerto} | señal={señal}")
                        _sorteos_desde_ultimo_recalculo += len(nuevos_insertados)
                    except Exception as e_ap:
                        logger.warning(f"⚠️ Micro-aprendizaje: {e_ap}")

                    # ── Recalcular rentabilidad cada N sorteos nuevos ──
                    if _sorteos_desde_ultimo_recalculo >= _RECALCULO_CADA_N:
                        await recalcular_rentabilidad_automatico(db)
                        _sorteos_desde_ultimo_recalculo = 0

            else:
                logger.warning(f"⚠️ Web bloqueada o caída (Status {r.status_code})")

    except Exception as e:
        logger.error(f"❌ Error en capturar_y_procesar: {e}")


async def ciclo_infinito():
    logger.info("🚀 [LottoAI PRO] Scheduler V7 — Aprendizaje + Rentabilidad automáticos")
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
