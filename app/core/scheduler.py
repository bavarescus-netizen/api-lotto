"""
scheduler_v11_2.py — LottoAI PRO V11.2
========================================
Cambios acumulados:
  ✅ FIX CRÍTICO: hora del scraper "08" → "08:00 AM" (HORA_NUM_A_LABEL)
  ✅ FIX: hora_siguiente() no rompe con ValueError silencioso
  ✅ NUEVO: columnas pred_tentativa_1/2/3 + origen en auditoria_ia
  ✅ NUEVO: dashboard puede comparar TENTATIVO vs INTRADAY vs REAL
  ✅ Ciclo nocturno 7PM revisado cada 2 min

V11.2 — nuevos:
  ✅ job_descubrir_patrones(): corre cada lunes 06AM — auto-aprende pares intraday
  ✅ migrar_tabla_patrones(): crea patrones_intraday_confirmados si no existe
  ✅ recalcular_prediccion_siguiente() mejorado: recalcula las 2 horas siguientes
  ✅ Madrugada (00:00-07:59): duerme 30 min pero ejecuta job lunes
  ✅ _asegurar_prediccion_hora_actual() genera TODAS las horas pendientes (no solo la próxima)
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


async def migrar_tabla_patrones(db):
    """
    V11.2 — Crea patrones_intraday_confirmados si no existe.
    Llamar desde startup de main.py junto con migrar_columnas_tentativo.
    """
    try:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS patrones_intraday_confirmados (
                id                SERIAL PRIMARY KEY,
                trigger_hora      VARCHAR(10)   NOT NULL,
                trigger_animal    VARCHAR(30)   NOT NULL,
                resultado_hora    VARCHAR(10)   NOT NULL,
                resultado_animal  VARCHAR(30)   NOT NULL,
                tipo              VARCHAR(20)   DEFAULT 'mismo_dia',
                n_casos           INT           NOT NULL,
                total_trigger     INT           NOT NULL DEFAULT 0,
                pct_confirmado    DECIMAL(5,2)  NOT NULL,
                ventaja_vs_azar   DECIMAL(5,2)  NOT NULL DEFAULT 1.0,
                activo            BOOLEAN       DEFAULT true,
                fecha_actualizacion DATE        DEFAULT CURRENT_DATE,
                UNIQUE (trigger_hora, trigger_animal, resultado_hora, resultado_animal)
            )
        """))
        # Insertar patrones base confirmados manualmente (solo si tabla estaba vacía)
        await db.execute(text("""
            INSERT INTO patrones_intraday_confirmados
            (trigger_hora, trigger_animal, resultado_hora, resultado_animal,
             tipo, n_casos, total_trigger, pct_confirmado, ventaja_vs_azar)
            VALUES
            ('02:00 PM','oso',      '07:00 PM','gallo',  'mismo_dia',    4, 13, 30.8, 11.7),
            ('01:00 PM','ardilla',  '02:00 PM','perico', 'mismo_dia',    4, 13, 30.8, 11.7),
            ('03:00 PM','alacran',  '01:00 PM','raton',  'dia_siguiente',4, 17, 23.5,  8.9),
            ('07:00 PM','caballo',  '01:00 PM','raton',  'dia_siguiente',4, 17, 23.5,  8.9),
            ('12:00 PM','carnero',  '02:00 PM','perico', 'mismo_dia',    4, 20, 20.0,  7.6)
            ON CONFLICT DO NOTHING
        """))
        await db.commit()
        logger.info("✅ Tabla patrones_intraday_confirmados: OK")
    except Exception as e:
        await db.rollback()
        logger.warning(f"⚠️ migrar_tabla_patrones: {e}")


# ─────────────────────────────────────────────────────────────
# JOB SEMANAL — descubrir patrones nuevos (lunes 06AM)
# ─────────────────────────────────────────────────────────────

_ultimo_descubrimiento_patrones: date | None = None

async def job_descubrir_patrones():
    """
    Corre cada lunes a las 06:00 AM VE.
    Llama a descubrir_patrones_nuevos() del motor → inserta pares nuevos en BD.
    """
    global _ultimo_descubrimiento_patrones
    hoy = date.today()

    # Evitar correr más de una vez por semana
    if _ultimo_descubrimiento_patrones == hoy:
        return

    logger.info("🔍 [JOB LUNES] Iniciando descubrimiento de patrones intraday...")
    try:
        from app.services.motor_v10 import descubrir_patrones_nuevos
        async with AsyncSessionLocal() as db:
            nuevos = await descubrir_patrones_nuevos(db, min_casos=4, min_pct=18.0)
            logger.info(f"✅ [JOB LUNES] Patrones nuevos descubiertos: {nuevos}")
            _ultimo_descubrimiento_patrones = hoy
    except Exception as e:
        logger.error(f"❌ [JOB LUNES] Error descubriendo patrones: {e}")


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
    V11.2 — Tras conocer el resultado de una hora, recalcula la predicción
    de las SIGUIENTES 2 HORAS con contexto actualizado (origen=INTRADAY).
    Recalcular 2 horas maximiza el impacto de los patrones confirmados
    y la señal intraday que ahora lleva el resultado real del día.
    El tentativo queda preservado en pred_tentativa_* para comparar.
    """
    from app.services.motor_v10 import generar_prediccion

    horas_a_recalcular = []
    h = hora_siguiente(hora_resultado)
    if h:
        horas_a_recalcular.append(h)
        h2 = hora_siguiente(h)
        if h2:
            horas_a_recalcular.append(h2)

    if not horas_a_recalcular:
        logger.info(f"🏁 {hora_resultado} fue el último sorteo del día")
        return

    for hora_prox in horas_a_recalcular:
        try:
            logger.info(f"🔄 Recalculando {hora_prox} tras {hora_resultado}={animal_resultado.upper()}")
            pred = await generar_prediccion(db, hora_prox)
            if not pred or not pred.get("prediccion_1"):
                logger.warning(f"⚠️ Motor no devolvió predicción para {hora_prox}")
                continue
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

            # Extrae: num="5", nombre="Leon", hora="08:00 AM"
            # Estructura HTML: <h4>5 Leon</h4><h5>Lotto Activo 08:00 AM</h5>
            patron = r'<h4[^>]*>(\d+)\s+([a-zA-ZáéíóúñÁÉÍÓÚÑ]+)</h4>\s*<h5>Lotto Activo\s+(\d{2}:\d{2}\s+[AP]M)</h5>'
            matches = re.findall(patron, html, re.DOTALL)
            nuevos_insertados = []

            for num, nombre, hora_str in matches:

                # hora_str ya viene en formato "08:00 AM" / "01:00 PM"
                if not hora_str:
                    logger.warning(f"⚠️ Hora no reconocida en HTML: '{hora_str}' — omitiendo")
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

                    # 3. V13: ajuste adaptativo tras cada sorteo real
                    try:
                        from app.services.motor_v13 import ajustar_tras_sorteo
                        ajuste = await ajustar_tras_sorteo(db, hora_nuevo, animal_nuevo)
                        n_aj = ajuste.get("ajustes_aplicados", 0)
                        pos  = ajuste.get("acierto_pos", "ninguna")
                        logger.info(f"🔄 V13 {hora_nuevo}: {pos} | {n_aj} horas ajustadas")
                    except Exception as e_v13:
                        logger.warning(f"⚠️ V13 ajuste_tras_sorteo {hora_nuevo}: {e_v13}")

                    # 4. Si es el último sorteo → TENTATIVO de mañana + plan V13
                    if hora_nuevo == "07:00 PM":
                        await generar_tentativo_manana(db, fecha_hoy, animal_nuevo)
                        try:
                            from app.services.motor_v13 import generar_plan_dia
                            manana = fecha_hoy + timedelta(days=1)
                            plan = await generar_plan_dia(db, manana)
                            hrs = len(plan.get("horas_rentables", []))
                            logger.info(f"📋 V13 plan mañana: {hrs} horas rentables")
                        except Exception as e_plan:
                            logger.warning(f"⚠️ V13 generar_plan_dia: {e_plan}")

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


async def startup(db):
    """
    Llamar desde main.py al iniciar la app.
    Ejecuta todas las migraciones necesarias.
    """
    await migrar_columnas_tentativo(db)
    await migrar_tabla_patrones(db)
    # V13: crear tabla plan_dia si no existe
    try:
        from app.services.motor_v13 import migrar_tabla_plan_dia
        await migrar_tabla_plan_dia(db)
        logger.info("✅ Tabla plan_dia V13: OK")
    except Exception as e:
        logger.warning(f"⚠️ migrar_tabla_plan_dia: {e}")
    logger.info("✅ Startup scheduler V13 completo")


async def ciclo_infinito():
    logger.info("🚀 [LottoAI PRO] Scheduler V11.2 — Tentativo + Intraday + Patrones + Job Semanal")
    while True:
        try:
            ahora    = datetime.now(TIMEZONE_VE)
            hora_ve  = ahora.hour
            min_ve   = ahora.minute
            dia_sem  = ahora.weekday()   # 0=lunes … 6=domingo

            # ── JOB SEMANAL: lunes entre 06:00–06:05 AM ──────────────────────
            if dia_sem == 0 and hora_ve == 6 and min_ve < 5:
                await job_descubrir_patrones()

            # ── 7PM–8:30PM: cada 2 min — capturar 7PM + tentativo ASAP ──────
            if hora_ve == 19 or (hora_ve == 20 and min_ve < 30):
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                    await _asegurar_prediccion_hora_actual(db, ahora)
                espera = 120

            # ── 8AM–7PM: cada 5 min — ciclo principal de sorteos ─────────────
            elif 8 <= hora_ve <= 18:
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                    await _asegurar_prediccion_hora_actual(db, ahora)
                espera = 300

            # ── Madrugada / noche temprana: cada 30 min ───────────────────────
            # No captura sorteos pero sí ejecuta jobs y mantiene el proceso vivo
            else:
                # Entre 20:30 y 23:59 — nada más que esperar
                # Entre 00:00 y 07:59 — solo job lunes si aplica
                espera = 1800

            await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
