import asyncio
import logging
import re
from datetime import datetime, date
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
_RECALCULO_CADA_N = 12
_ultimo_dia_predicciones = None  # Para generar predicciones solo una vez por día


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


async def generar_predicciones_dia(db, fecha_hoy):
    """
    Genera predicciones V10 para todas las horas del día si no existen.
    Se llama una vez al inicio de cada día.
    """
    horas = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
             "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM","06:00 PM","07:00 PM"]
    generadas = 0
    try:
        from app.services.motor_v10 import generar_prediccion
        for hora in horas:
            existe = (await db.execute(text("""
                SELECT 1 FROM auditoria_ia
                WHERE fecha = :fecha AND hora = :hora
                AND prediccion_1 IS NOT NULL
            """), {"fecha": fecha_hoy, "hora": hora})).fetchone()
            if existe:
                continue
            try:
                pred = await generar_prediccion(db, hora)
                if pred and pred.get("prediccion_1"):
                    await db.execute(text("""
                        INSERT INTO auditoria_ia
                            (fecha, hora, animal_predicho, prediccion_1, prediccion_2,
                             prediccion_3, confianza_pct, confianza_hora, es_hora_rentable)
                        VALUES
                            (:fecha, :hora, :p1, :p1, :p2, :p3, :conf, :conf_hora, :rentable)
                        ON CONFLICT (fecha, hora) DO NOTHING
                    """), {
                        "fecha":     fecha_hoy,
                        "hora":      hora,
                        "p1":        pred.get("prediccion_1"),
                        "p2":        pred.get("prediccion_2"),
                        "p3":        pred.get("prediccion_3"),
                        "conf":      pred.get("confianza_pct", 0),
                        "conf_hora": pred.get("confianza_hora", 0),
                        "rentable":  pred.get("es_hora_rentable", False),
                    })
                    generadas += 1
            except Exception as e_h:
                logger.warning(f"⚠️ Error generando pred {hora}: {e_h}")
        await db.commit()
        logger.info(f"🎯 Predicciones generadas para {fecha_hoy}: {generadas} nuevas")
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error generando predicciones del día: {e}")


async def capturar_y_procesar(db):
    global _sorteos_desde_ultimo_recalculo, _ultimo_dia_predicciones

    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()

    # ── Generar predicciones V10 una vez al inicio de cada día ──
    if _ultimo_dia_predicciones != fecha_hoy:
        await generar_predicciones_dia(db, fecha_hoy)
        _ultimo_dia_predicciones = fecha_hoy

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

                # ── Micro-aprendizaje solo para sorteos NUEVOS ──
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
    logger.info("🚀 [LottoAI PRO] Scheduler V8 — Predicciones diarias + Aprendizaje + Rentabilidad")
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
