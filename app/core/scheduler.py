"""
scheduler_v11_final.py — LottoAI PRO V12
=========================================
Cambios vs versión anterior:
  ✅ FIX CRÍTICO: hora del scraper "08" → "08:00 AM" (HORA_NUM_A_LABEL)
  ✅ FIX: hora_siguiente() no rompe con ValueError silencioso
  ✅ NUEVO: columnas pred_tentativa_1/2/3 + origen en auditoria_ia
  ✅ NUEVO: dashboard puede comparar TENTATIVO vs INTRADAY vs REAL
  ✅ Ciclo nocturno 7PM revisado cada 2 min (antes del bloque general)
"""

import asyncio
import logging
import re
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import httpx
from sqlalchemy import text
from db import AsyncSessionLocal
from app.services.motor_v10 import MAPA_ANIMALES

logger = logging.getLogger(__name__)
TIMEZONE_VE = ZoneInfo("America/Caracas")
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo/resultados/"
NUM_A_ANIMAL = MAPA_ANIMALES

_sorteos_desde_ultimo_recalculo = 0
_RECALCULO_CADA_N = 12

# Evita regenerar el tentativo más de una vez por noche
_tentativo_manana_generado: date | None = None

# Horas en formato exacto de auditoria_ia
HORAS_SORTEO = [
    "08:00 AM", "09:00 AM", "10:00 AM", "11:00 AM", "12:00 PM",
    "01:00 PM", "02:00 PM", "03:00 PM", "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM"
]

# ✅ FIX CRÍTICO: el HTML devuelve "08", "13", etc. → convertir al formato correcto
HORA_NUM_A_LABEL = {
    "08": "08:00 AM",
    "09": "09:00 AM",
    "10": "10:00 AM",
    "11": "11:00 AM",
    "12": "12:00 PM",
    "13": "01:00 PM",
    "14": "02:00 PM",
    "15": "03:00 PM",
    "16": "04:00 PM",
    "17": "05:00 PM",
    "18": "06:00 PM",
    "19": "07:00 PM",
}


# ─────────────────────────────────────────────────────────────
# MIGRACIÓN: agregar columnas tentativo si no existen
# ─────────────────────────────────────────────────────────────

async def migrar_columnas_tentativo(db):
    """
    Agrega pred_tentativa_1/2/3 y origen a auditoria_ia si no existen.
    Llamar desde el startup de main.py una vez.
    """
    try:
        await db.execute(text("""
            ALTER TABLE auditoria_ia
                ADD COLUMN IF NOT EXISTS pred_tentativa_1 VARCHAR,
                ADD COLUMN IF NOT EXISTS pred_tentativa_2 VARCHAR,
                ADD COLUMN IF NOT EXISTS pred_tentativa_3 VARCHAR,
                ADD COLUMN IF NOT EXISTS origen VARCHAR DEFAULT 'INICIAL'
        """))
        await db.commit()
        logger.info("✅ Migración columnas tentativo: OK")
    except Exception as e:
        await db.rollback()
        logger.warning(f"⚠️ Migración columnas tentativo (puede ser normal si ya existen): {e}")


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def hora_siguiente(hora_actual: str) -> str | None:
    """Dada una hora en formato HORAS_SORTEO, devuelve la siguiente del día."""
    try:
        idx = HORAS_SORTEO.index(hora_actual)
        if idx + 1 < len(HORAS_SORTEO):
            return HORAS_SORTEO[idx + 1]
    except ValueError:
        logger.warning(f"⚠️ hora_siguiente: '{hora_actual}' no está en HORAS_SORTEO")
    return None


# ─────────────────────────────────────────────────────────────
# AUDITORIA: guardar resultado real post-sorteo
# ─────────────────────────────────────────────────────────────

async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Guarda resultado_real, acierto TOP1 y TOP3, y actualiza origen."""
    if not animal_real:
        return
    animal_real_upper = animal_real.upper().strip()
    animal_real_lower = animal_real.lower().strip()
    try:
        await db.execute(text("""
            UPDATE auditoria_ia
            SET resultado_real = :real_upper,
                acierto = (
                    LOWER(TRIM(prediccion_1)) = :real_lower
                    OR LOWER(TRIM(prediccion_2)) = :real_lower
                    OR LOWER(TRIM(prediccion_3)) = :real_lower
                ),
                origen = 'INTRADAY'
            WHERE fecha = :fecha AND hora = :hora
        """), {
            "real_upper": animal_real_upper,
            "real_lower": animal_real_lower,
            "fecha": fecha,
            "hora": hora
        })
        await db.commit()
        logger.info(f"✅ Resultado guardado: {hora} → {animal_real_upper}")
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error auditoría post-sorteo {hora}: {e}")


# ─────────────────────────────────────────────────────────────
# PREDICCIONES: guardar (helper unificado)
# ─────────────────────────────────────────────────────────────

async def guardar_prediccion(db, fecha, hora, pred, *, forzar: bool = False, origen: str = "INICIAL"):
    """
    Guarda predicción en auditoria_ia.

    forzar=True  → DO UPDATE siempre (tentativo nocturno, correcciones intraday)
    forzar=False → DO UPDATE solo si resultado_real está vacío (predicción inicial)

    origen="TENTATIVO-DD/MM" → guarda también en pred_tentativa_* para comparar
    origen="INTRADAY"        → actualiza prediccion_1/2/3 pero NO toca pred_tentativa_*
    """
    es_tentativo = "TENTATIVO" in origen.upper()

    if forzar:
        conflict_clause = """
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_predicho  = EXCLUDED.animal_predicho,
                prediccion_1     = EXCLUDED.prediccion_1,
                prediccion_2     = EXCLUDED.prediccion_2,
                prediccion_3     = EXCLUDED.prediccion_3,
                confianza_pct    = EXCLUDED.confianza_pct,
                confianza_hora   = EXCLUDED.confianza_hora,
                es_hora_rentable = EXCLUDED.es_hora_rentable,
                origen           = EXCLUDED.origen
        """
    else:
        conflict_clause = """
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_predicho  = EXCLUDED.animal_predicho,
                prediccion_1     = EXCLUDED.prediccion_1,
                prediccion_2     = EXCLUDED.prediccion_2,
                prediccion_3     = EXCLUDED.prediccion_3,
                confianza_pct    = EXCLUDED.confianza_pct,
                confianza_hora   = EXCLUDED.confianza_hora,
                es_hora_rentable = EXCLUDED.es_hora_rentable,
                origen           = EXCLUDED.origen
            WHERE auditoria_ia.resultado_real IS NULL
               OR auditoria_ia.resultado_real IN ('PENDIENTE', '', 'pendiente')
        """

    try:
        await db.execute(text(f"""
            INSERT INTO auditoria_ia
                (fecha, hora, animal_predicho, prediccion_1, prediccion_2,
                 prediccion_3, confianza_pct, confianza_hora, es_hora_rentable, origen)
            VALUES
                (:fecha, :hora, :p1, :p1, :p2, :p3, :conf, :conf_hora, :rentable, :origen)
            {conflict_clause}
        """), {
            "fecha":     fecha,
            "hora":      hora,
            "p1":        pred.get("prediccion_1"),
            "p2":        pred.get("prediccion_2"),
            "p3":        pred.get("prediccion_3"),
            "conf":      pred.get("confianza_pct", 0),
            "conf_hora": pred.get("confianza_hora", 0),
            "rentable":  pred.get("es_hora_rentable", False),
            "origen":    origen,
        })
        await db.commit()

        # ✅ NUEVO: si es tentativo, guardar también en pred_tentativa_* (COALESCE = no sobreescribir)
        # Así aunque INTRADAY actualice prediccion_1, el tentativo original queda para comparar
        if es_tentativo:
            await db.execute(text("""
                UPDATE auditoria_ia SET
                    pred_tentativa_1 = COALESCE(pred_tentativa_1, :p1),
                    pred_tentativa_2 = COALESCE(pred_tentativa_2, :p2),
                    pred_tentativa_3 = COALESCE(pred_tentativa_3, :p3)
                WHERE fecha = :fecha AND hora = :hora
            """), {
                "p1": pred.get("prediccion_1"),
                "p2": pred.get("prediccion_2"),
                "p3": pred.get("prediccion_3"),
                "fecha": fecha,
                "hora": hora,
            })
            await db.commit()

        tag = f"[{origen}] " if origen else ""
        logger.info(
            f"💾 {tag}{fecha} {hora}: "
            f"{pred.get('prediccion_1','?').upper()} / "
            f"{pred.get('prediccion_2','?').upper()} / "
            f"{pred.get('prediccion_3','?').upper()} "
            f"| conf={pred.get('confianza_pct', 0)}"
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error guardar_prediccion {fecha} {hora} [{origen}]: {e}")


# ─────────────────────────────────────────────────────────────
# CORRECCIÓN INTRADAY
# ─────────────────────────────────────────────────────────────

async def recalcular_prediccion_siguiente(db, fecha, hora_resultado, animal_resultado):
    """
    Tras conocer el resultado de una hora, recalcula la predicción
    de la SIGUIENTE con contexto actualizado (origen=INTRADAY).
    El tentativo queda preservado en pred_tentativa_* para comparar.
    """
    hora_prox = hora_siguiente(hora_resultado)
    if not hora_prox:
        logger.info(f"🏁 {hora_resultado} fue el último sorteo del día")
        return

    try:
        from app.services.motor_v10 import generar_prediccion
        logger.info(f"🔄 Recalculando {hora_prox} tras {hora_resultado}={animal_resultado.upper()}")

        pred = await generar_prediccion(db, hora_prox)
        if not pred or not pred.get("prediccion_1"):
            logger.warning(f"⚠️ Motor no devolvió predicción para {hora_prox}")
            return

        await guardar_prediccion(
            db, fecha, hora_prox, pred,
            forzar=True,
            origen="INTRADAY"
        )

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error recalculando predicción {hora_prox}: {e}")


# ─────────────────────────────────────────────────────────────
# PREDICCIÓN INICIAL (cuando no hay tentativo nocturno)
# ─────────────────────────────────────────────────────────────

async def generar_prediccion_inicial(db, fecha, hora):
    """
    Genera predicción para una hora si no existe ninguna para esa fecha.
    Respeta el tentativo nocturno si ya existe — no lo toca.
    """
    try:
        fila = (await db.execute(text("""
            SELECT prediccion_1 FROM auditoria_ia
            WHERE fecha = :fecha AND hora = :hora
        """), {"fecha": fecha, "hora": hora})).fetchone()

        if fila and fila[0]:
            return  # Ya existe → no tocar

        from app.services.motor_v10 import generar_prediccion
        pred = await generar_prediccion(db, hora)
        if pred and pred.get("prediccion_1"):
            await guardar_prediccion(
                db, fecha, hora, pred,
                forzar=False,
                origen="INICIAL"
            )
            logger.info(f"🌱 Predicción inicial: {fecha} {hora}")
    except Exception as e:
        await db.rollback()
        logger.warning(f"⚠️ Error predicción inicial {hora}: {e}")


# ─────────────────────────────────────────────────────────────
# TENTATIVO NOCTURNO (tras el último sorteo: 07PM)
# ─────────────────────────────────────────────────────────────

async def generar_tentativo_manana(db, fecha_hoy: date, animal_ultimo: str):
    """
    Genera predicciones tentativas para TODAS las horas de mañana
    justo después de conocer el resultado de 07:00 PM.

    - Markov ya fue actualizado con ese resultado antes de llamar esto
    - origen = TENTATIVO-DD/MM → guarda también en pred_tentativa_*
    - El dashboard mostrará: TENTATIVO | INTRADAY | REAL para cada hora
    """
    global _tentativo_manana_generado

    if _tentativo_manana_generado == fecha_hoy:
        logger.info("⏭️ Tentativo de mañana ya generado esta noche, omitiendo")
        return

    fecha_manana = fecha_hoy + timedelta(days=1)
    tag_origen = f"TENTATIVO-{fecha_hoy.strftime('%d/%m')}"

    logger.info(
        f"🌅 Último sorteo ({animal_ultimo.upper()}) procesado. "
        f"Generando TENTATIVO para mañana {fecha_manana}..."
    )

    try:
        from app.services.motor_v10 import generar_prediccion
        generadas = 0

        for hora in HORAS_SORTEO:
            try:
                pred = await generar_prediccion(db, hora)
                if pred and pred.get("prediccion_1"):
                    await guardar_prediccion(
                        db, fecha_manana, hora, pred,
                        forzar=True,
                        origen=tag_origen
                    )
                    generadas += 1
            except Exception as e_hora:
                logger.warning(f"⚠️ Error tentativo {hora}: {e_hora}")
                continue

        _tentativo_manana_generado = fecha_hoy
        logger.info(
            f"✅ Tentativo completo: {generadas}/{len(HORAS_SORTEO)} horas "
            f"para {fecha_manana}. Markov post-{animal_ultimo.upper()}."
        )
        logger.info(
            "📋 Mañana: INTRADAY corregirá cada hora. "
            "pred_tentativa_* guardado para comparar en dashboard."
        )

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error generando tentativo de mañana: {e}")


# ─────────────────────────────────────────────────────────────
# RENTABILIDAD AUTOMÁTICA
# ─────────────────────────────────────────────────────────────

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
        logger.info(f"📊 Rentabilidad recalculada — Top3: {top_str}")
    except Exception as e:
        logger.warning(f"⚠️ Error recalculando rentabilidad: {e}")


# ─────────────────────────────────────────────────────────────
# CAPTURA PRINCIPAL — scraper + procesamiento completo
# ─────────────────────────────────────────────────────────────

async def capturar_y_procesar(db):
    global _sorteos_desde_ultimo_recalculo

    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": "https://www.google.com/",
    }

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            r = await client.get(BASE_URL)
            if r.status_code != 200:
                logger.warning(f"⚠️ Web bloqueada o caída (Status {r.status_code})")
                return

            html = r.text

            # Extrae: hora_raw="08"/"13", num="12", nombre="Caballo"
            patron = r"(\d{2}):00.*?(\d{1,2})\s*[-–]\s*([a-zA-Záéíóúñ]+)"
            matches = re.findall(patron, html, re.DOTALL)
            nuevos_insertados = []

            for hora_raw, num, nombre in matches:

                # ✅ FIX: "08" → "08:00 AM", "13" → "01:00 PM", etc.
                hora_str = HORA_NUM_A_LABEL.get(hora_raw)
                if not hora_str:
                    logger.warning(f"⚠️ Hora no reconocida en HTML: '{hora_raw}' — omitiendo")
                    continue

                nombre_norm = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())

                # Insertar en histórico
                res = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, 'Lotto Activo')
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    RETURNING hora
                """), {"f": fecha_hoy, "h": hora_str, "a": nombre_norm})

                insertado = res.fetchone()
                if insertado:
                    nuevos_insertados.append((hora_str, nombre_norm))
                    logger.info(f"📥 Nuevo: {hora_str} → {nombre_norm.upper()}")

                # Actualizar resultado real en auditoria_ia
                await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_str, nombre_norm)

            await db.commit()

            # ── Procesar cada sorteo NUEVO ─────────────────────────────────────
            if nuevos_insertados:
                for hora_nuevo, animal_nuevo in nuevos_insertados:

                    # 1. Micro-aprendizaje (Markov aprende)
                    try:
                        from app.services.motor_v10 import aprender_sorteo
                        resultado = await aprender_sorteo(db, fecha_hoy, hora_nuevo, animal_nuevo)
                        if resultado.get("status") == "success":
                            acerto = (
                                "TOP1 ✅" if resultado.get("acerto_top1")
                                else "TOP3 ✅" if resultado.get("acerto_top3")
                                else "FALLO ❌"
                            )
                            logger.info(f"🧠 Aprendizaje {hora_nuevo}: {acerto}")
                    except Exception as e_ap:
                        logger.warning(f"⚠️ Micro-aprendizaje {hora_nuevo}: {e_ap}")

                    # 2. Corrección INTRADAY de la siguiente hora
                    await recalcular_prediccion_siguiente(db, fecha_hoy, hora_nuevo, animal_nuevo)

                    # 3. Si es el último sorteo → TENTATIVO de mañana
                    if hora_nuevo == "07:00 PM":
                        await generar_tentativo_manana(db, fecha_hoy, animal_nuevo)

                _sorteos_desde_ultimo_recalculo += len(nuevos_insertados)

                if _sorteos_desde_ultimo_recalculo >= _RECALCULO_CADA_N:
                    await recalcular_rentabilidad_automatico(db)
                    _sorteos_desde_ultimo_recalculo = 0

            # ── Generar predicción inicial para la próxima hora si no existe ──
            hora_prox = None
            h_actual = ahora.hour
            for h_slot, h_lbl in zip(
                [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
                HORAS_SORTEO
            ):
                if h_slot > h_actual:
                    hora_prox = h_lbl
                    break

            if hora_prox:
                await generar_prediccion_inicial(db, fecha_hoy, hora_prox)

    except Exception as e:
        logger.error(f"❌ Error en capturar_y_procesar: {e}")


# ─────────────────────────────────────────────────────────────
# CICLO PRINCIPAL
# ─────────────────────────────────────────────────────────────

async def _asegurar_prediccion_hora_actual(db, ahora):
    """Genera predicción para la próxima hora si no existe en BD — independiente del scraper."""
    fecha_hoy = ahora.date()
    h_actual = ahora.hour
    horas_slots = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

    for h_slot, h_lbl in zip(horas_slots, HORAS_SORTEO):
        if h_slot >= h_actual:
            try:
                await generar_prediccion_inicial(db, fecha_hoy, h_lbl)
                logger.info(f"🔮 Predicción asegurada para: {h_lbl}")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo generar predicción {h_lbl}: {e}")
            break  # Solo la próxima hora pendiente


async def ciclo_infinito():
    logger.info("🚀 [LottoAI PRO] Scheduler V11 FINAL — Tentativo + Intraday + Comparación")
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            hora_ve = ahora.hour

            # 7PM-8:30PM: cada 2 min — capturar 7PM y disparar tentativo ASAP
            if hora_ve == 19 or (hora_ve == 20 and ahora.minute < 30):
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                    # ✅ Predicción independiente del scraper
                    await _asegurar_prediccion_hora_actual(db, ahora)
                espera = 120

            # Horario sorteos 8AM-8PM: cada 5 min
            elif 8 <= hora_ve <= 20:
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                    # ✅ Predicción independiente del scraper
                    await _asegurar_prediccion_hora_actual(db, ahora)
                espera = 300

            # Noche/madrugada: sin procesar
            else:
                espera = 1800

            await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
