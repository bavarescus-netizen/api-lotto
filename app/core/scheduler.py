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
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
NUM_A_ANIMAL = MAPA_ANIMALES

_sorteos_desde_ultimo_recalculo = 0
_RECALCULO_CADA_N = 12

# Flag para evitar regenerar el tentativo de mañana más de una vez por noche
_tentativo_manana_generado: date | None = None

# Horas de sorteo en formato exacto usado en auditoria_ia
HORAS_SORTEO = [
    "08:00 AM", "09:00 AM", "10:00 AM", "11:00 AM", "12:00 PM",
    "01:00 PM", "02:00 PM", "03:00 PM", "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM"
]

# ✅ FIX PRINCIPAL: mapa de hora numérica del HTML → formato HORAS_SORTEO
# El scraper extrae "08", "09"... pero auditoria_ia usa "08:00 AM", "09:00 AM"...
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


def hora_siguiente(hora_actual: str) -> str | None:
    """Dada una hora en formato HORAS_SORTEO, devuelve la siguiente del día."""
    try:
        idx = HORAS_SORTEO.index(hora_actual)
        if idx + 1 < len(HORAS_SORTEO):
            return HORAS_SORTEO[idx + 1]
    except ValueError:
        logger.warning(f"⚠️ hora_siguiente: '{hora_actual}' no está en HORAS_SORTEO")
    return None


# ─────────────────────────────────────────────
# AUDITORIA: guardar resultado real
# ─────────────────────────────────────────────

async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Actualiza resultado_real y acierto en auditoria_ia cuando sale un sorteo."""
    if not animal_real:
        return
    animal_real = animal_real.lower().strip()
    try:
        await db.execute(text("""
            UPDATE auditoria_ia
            SET resultado_real = :real,
                acierto = (LOWER(TRIM(prediccion_1)) = :real)
            WHERE fecha = :fecha AND hora = :hora
        """), {"real": animal_real, "fecha": fecha, "hora": hora})
        await db.commit()
        logger.info(f"✅ Resultado guardado: {hora} → {animal_real}")
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error auditoría post-sorteo: {e}")


# ─────────────────────────────────────────────
# PREDICCIONES: guardar / recalcular
# ─────────────────────────────────────────────

async def guardar_prediccion(db, fecha, hora, pred, *, forzar: bool = False, origen: str = ""):
    """
    Helper unificado para guardar predicciones en auditoria_ia.

    forzar=True  → DO UPDATE siempre (tentativo nocturno, correcciones intraday)
    forzar=False → DO UPDATE solo si aún no hay resultado real (predicción inicial)
    origen       → etiqueta para el log ("TENTATIVO", "CORRECCIÓN", "INICIAL", etc.)
    """
    if forzar:
        conflict_clause = """
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_predicho  = EXCLUDED.animal_predicho,
                prediccion_1     = EXCLUDED.prediccion_1,
                prediccion_2     = EXCLUDED.prediccion_2,
                prediccion_3     = EXCLUDED.prediccion_3,
                confianza_pct    = EXCLUDED.confianza_pct,
                confianza_hora   = EXCLUDED.confianza_hora,
                es_hora_rentable = EXCLUDED.es_hora_rentable
        """
    else:
        # Solo actualiza si no hay resultado real aún (respeta sorteos ya registrados)
        conflict_clause = """
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_predicho  = EXCLUDED.animal_predicho,
                prediccion_1     = EXCLUDED.prediccion_1,
                prediccion_2     = EXCLUDED.prediccion_2,
                prediccion_3     = EXCLUDED.prediccion_3,
                confianza_pct    = EXCLUDED.confianza_pct,
                confianza_hora   = EXCLUDED.confianza_hora,
                es_hora_rentable = EXCLUDED.es_hora_rentable
            WHERE auditoria_ia.resultado_real IS NULL
               OR auditoria_ia.resultado_real IN ('PENDIENTE', '', 'pendiente')
        """

    try:
        await db.execute(text(f"""
            INSERT INTO auditoria_ia
                (fecha, hora, animal_predicho, prediccion_1, prediccion_2,
                 prediccion_3, confianza_pct, confianza_hora, es_hora_rentable)
            VALUES
                (:fecha, :hora, :p1, :p1, :p2, :p3, :conf, :conf_hora, :rentable)
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
        logger.error(f"❌ Error guardar_prediccion {fecha} {hora}: {e}")


async def recalcular_prediccion_siguiente(db, fecha, hora_resultado, animal_resultado):
    """
    Tras conocer el resultado de una hora, recalcula la predicción
    de la SIGUIENTE hora con el contexto actualizado (corrección intraday).
    Siempre fuerza la actualización — reemplaza el tentativo de anoche.
    """
    hora_prox = hora_siguiente(hora_resultado)
    if not hora_prox:
        logger.info(f"🏁 {hora_resultado} fue el último sorteo del día")
        return

    try:
        from app.services.motor_v10 import generar_prediccion
        logger.info(f"🔄 Recalculando {hora_prox} tras {hora_resultado}={animal_resultado}")

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


async def generar_prediccion_inicial(db, fecha, hora):
    """
    Genera predicción para una hora si no existe para esa fecha.
    Respeta el tentativo nocturno si ya existe.
    Solo actúa cuando no hay ninguna predicción para fecha+hora.
    """
    try:
        fila = (await db.execute(text("""
            SELECT prediccion_1 FROM auditoria_ia
            WHERE fecha = :fecha AND hora = :hora
        """), {"fecha": fecha, "hora": hora})).fetchone()

        if fila and fila[0]:
            return  # Ya existe predicción → no tocar

        from app.services.motor_v10 import generar_prediccion
        pred = await generar_prediccion(db, hora)
        if pred and pred.get("prediccion_1"):
            await guardar_prediccion(
                db, fecha, hora, pred,
                forzar=False,
                origen="INICIAL"
            )
            logger.info(f"🌱 Predicción inicial generada: {fecha} {hora}")
    except Exception as e:
        await db.rollback()
        logger.warning(f"⚠️ Error predicción inicial {hora}: {e}")


# ─────────────────────────────────────────────
# TENTATIVO NOCTURNO
# ─────────────────────────────────────────────

async def generar_tentativo_manana(db, fecha_hoy: date, animal_ultimo: str):
    """
    Genera predicciones tentativas para todas las horas de mañana
    justo después del último sorteo del día (07:00 PM).
    Markov ya fue actualizado con el resultado de 7PM antes de llamar esto.
    """
    global _tentativo_manana_generado

    if _tentativo_manana_generado == fecha_hoy:
        logger.info("⏭️ Tentativo de mañana ya generado esta noche, omitiendo")
        return

    fecha_manana = fecha_hoy + timedelta(days=1)
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
                        origen=f"TENTATIVO-{fecha_hoy.strftime('%d/%m')}"
                    )
                    generadas += 1
            except Exception as e_hora:
                logger.warning(f"⚠️ Error tentativo {hora}: {e_hora}")
                continue

        _tentativo_manana_generado = fecha_hoy
        logger.info(
            f"✅ Tentativo completo: {generadas}/{len(HORAS_SORTEO)} horas "
            f"para {fecha_manana}. Markov fresco post-{animal_ultimo.upper()}."
        )

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error generando tentativo de mañana: {e}")


# ─────────────────────────────────────────────
# RENTABILIDAD
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# CAPTURA PRINCIPAL — scraper + procesamiento
# ─────────────────────────────────────────────

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

            # ✅ FIX: el patrón extrae hora como "08", "09", "13"...
            # Se convierte a formato "08:00 AM", "01:00 PM"... con HORA_NUM_A_LABEL
            patron = r"(\d{2}):00.*?(\d{1,2})\s*[-–]\s*([a-zA-Záéíóúñ]+)"
            matches = re.findall(patron, html, re.DOTALL)
            nuevos_insertados = []

            for hora_raw, num, nombre in matches:

                # ✅ FIX: convertir "08" → "08:00 AM", "13" → "01:00 PM", etc.
                hora_str = HORA_NUM_A_LABEL.get(hora_raw)
                if not hora_str:
                    logger.warning(f"⚠️ Hora no reconocida en HTML: '{hora_raw}' — omitiendo")
                    continue

                nombre_norm = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())

                # Insertar en histórico (tabla de resultados reales)
                res = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, 'Lotto Activo')
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    RETURNING hora
                """), {"f": fecha_hoy, "h": hora_str, "a": nombre_norm})

                insertado = res.fetchone()
                if insertado:
                    nuevos_insertados.append((hora_str, nombre_norm))
                    logger.info(f"📥 Nuevo sorteo: {hora_str} → {nombre_norm.upper()}")

                # Actualizar resultado real en auditoria_ia (hora_str ya en formato correcto)
                await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_str, nombre_norm)

            await db.commit()

            # ── Procesar cada sorteo NUEVO ──
            if nuevos_insertados:
                for hora_nuevo, animal_nuevo in nuevos_insertados:

                    # 1. Micro-aprendizaje — Markov aprende el resultado real
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

                    # 2. Recalcular predicción de la siguiente hora (corrección intraday)
                    #    hora_siguiente("07:00 PM") → None → no hace nada extra
                    await recalcular_prediccion_siguiente(db, fecha_hoy, hora_nuevo, animal_nuevo)

                    # 3. Si es el último sorteo del día → generar TENTATIVO de mañana
                    if hora_nuevo == "07:00 PM":
                        await generar_tentativo_manana(db, fecha_hoy, animal_nuevo)

                _sorteos_desde_ultimo_recalculo += len(nuevos_insertados)

                # Recalcular rentabilidad cada N sorteos
                if _sorteos_desde_ultimo_recalculo >= _RECALCULO_CADA_N:
                    await recalcular_rentabilidad_automatico(db)
                    _sorteos_desde_ultimo_recalculo = 0

            # ── Generar predicción inicial para la próxima hora si no existe ──
            # (el tentativo nocturno ya la debería tener, esta función lo respeta)
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


# ─────────────────────────────────────────────
# CICLO PRINCIPAL
# ─────────────────────────────────────────────

async def ciclo_infinito():
    logger.info("🚀 [LottoAI PRO] Scheduler V11 FINAL — Tentativo nocturno + correcciones intraday")
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            hora_ve = ahora.hour

            if hora_ve == 19 or (hora_ve == 20 and ahora.minute < 30):
                # 7PM-8:30PM — revisar cada 2 min para no perder el resultado de 7PM
                # y disparar el tentativo nocturno lo antes posible
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                espera = 120

            elif 8 <= hora_ve <= 20:
                # Horario de sorteos — revisar cada 5 min
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                espera = 300

            else:
                # Noche/madrugada — no procesar, esperar 30 min
                espera = 1800

            await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
