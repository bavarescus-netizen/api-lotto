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
TIMEZONE_VE = ZoneInfo('America/Caracas')
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
NUM_A_ANIMAL = MAPA_ANIMALES

_sorteos_desde_ultimo_recalculo = 0
_RECALCULO_CADA_N = 12

# Flag para saber si ya generamos el tentativo de mañana esta noche
# Se resetea cada día a las 7PM cuando sale el último sorteo
_tentativo_manana_generado: date | None = None

HORAS_SORTEO = [
    "08:00 AM", "09:00 AM", "10:00 AM", "11:00 AM", "12:00 PM",
    "01:00 PM", "02:00 PM", "03:00 PM", "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM"
]


def hora_siguiente(hora_actual: str) -> str | None:
    """Dada una hora, devuelve la siguiente hora de sorteo del mismo día."""
    try:
        idx = HORAS_SORTEO.index(hora_actual)
        if idx + 1 < len(HORAS_SORTEO):
            return HORAS_SORTEO[idx + 1]
    except ValueError:
        pass
    return None


async def actualizar_auditoria_post_sorteo(db, fecha, hora, animal_real):
    """Actualiza el resultado real en auditoria_ia cuando sale un sorteo."""
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
        logger.error(f"❌ Error auditoría: {e}")


async def recalcular_prediccion_siguiente(db, fecha, hora_resultado, animal_resultado):
    """
    Después de conocer el resultado de una hora, recalcula la predicción
    de la SIGUIENTE hora usando el motor con el contexto actualizado.
    Núcleo del sistema reactivo intraday.
    """
    hora_prox = hora_siguiente(hora_resultado)
    if not hora_prox:
        logger.info(f"🏁 {hora_resultado} fue el último sorteo del día")
        return

    try:
        from app.services.motor_v10 import generar_prediccion
        logger.info(f"🔄 Recalculando predicción {hora_prox} tras conocer {hora_resultado}={animal_resultado}")

        pred = await generar_prediccion(db, hora_prox)
        if not pred or not pred.get("prediccion_1"):
            logger.warning(f"⚠️ Motor no devolvió predicción para {hora_prox}")
            return

        await db.execute(text("""
            INSERT INTO auditoria_ia
                (fecha, hora, animal_predicho, prediccion_1, prediccion_2,
                 prediccion_3, confianza_pct, confianza_hora, es_hora_rentable)
            VALUES
                (:fecha, :hora, :p1, :p1, :p2, :p3, :conf, :conf_hora, :rentable)
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
        """), {
            "fecha":     fecha,
            "hora":      hora_prox,
            "p1":        pred.get("prediccion_1"),
            "p2":        pred.get("prediccion_2"),
            "p3":        pred.get("prediccion_3"),
            "conf":      pred.get("confianza_pct", 0),
            "conf_hora": pred.get("confianza_hora", 0),
            "rentable":  pred.get("es_hora_rentable", False),
        })
        await db.commit()

        logger.info(
            f"🎯 Nueva pred {hora_prox}: "
            f"{pred.get('prediccion_1','?').upper()} / "
            f"{pred.get('prediccion_2','?').upper()} / "
            f"{pred.get('prediccion_3','?').upper()} "
            f"| conf={pred.get('confianza_pct', 0)}"
        )

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error recalculando predicción {hora_prox}: {e}")


async def guardar_prediccion(db, fecha, hora, pred, *, forzar: bool = False, origen: str = ""):
    """
    Helper unificado para guardar predicciones en auditoria_ia.

    forzar=True  → DO UPDATE siempre (tentativo nocturno, correcciones intraday)
    forzar=False → DO UPDATE solo si aún no hay resultado real (predicción inicial)
    origen       → etiqueta para el log ("TENTATIVO", "CORRECCIÓN", "INICIAL", etc.)
    """
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
    if not forzar:
        conflict_clause += """
            WHERE auditoria_ia.resultado_real IS NULL
               OR auditoria_ia.resultado_real IN ('PENDIENTE', '', 'pendiente')
        """

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


async def generar_tentativo_manana(db, fecha_hoy: date, animal_ultimo: str):
    """
    Genera predicciones TENTATIVAS para todas las horas de mañana
    justo después de conocer el último sorteo del día (07:00 PM).

    - Markov ya fue actualizado con el resultado de 07PM
    - Se guarda con forzar=True → reemplaza cualquier predicción previa de ayer
    - El dashboard puede mostrar "TENTATIVO" vs "CONFIRMADO" para comparación

    Al día siguiente, cada sorteo nuevo recalcula y corrige estas predicciones
    con el contexto real acumulado.
    """
    global _tentativo_manana_generado

    fecha_manana = fecha_hoy + timedelta(days=1)

    # Evitar regenerar si ya lo hicimos hoy
    if _tentativo_manana_generado == fecha_hoy:
        logger.info("⏭️ Tentativo de mañana ya generado esta noche, omitiendo")
        return

    logger.info(
        f"🌅 Último sorteo del día ({animal_ultimo.upper()}) procesado. "
        f"Generando TENTATIVO completo para mañana {fecha_manana}..."
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
                logger.warning(f"⚠️ Error generando tentativo {hora}: {e_hora}")
                continue

        _tentativo_manana_generado = fecha_hoy
        logger.info(
            f"✅ Tentativo completo generado: {generadas}/{len(HORAS_SORTEO)} horas "
            f"para {fecha_manana}. Markov fresco post-{animal_ultimo.upper()}."
        )
        logger.info(
            "📋 Mañana: cada sorteo real corregirá su siguiente predicción. "
            "El tentativo sirve como baseline de comparación."
        )

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error generando tentativo de mañana: {e}")


async def generar_prediccion_inicial(db, fecha, hora):
    """
    Genera predicción para una hora si no existe para ESA FECHA.
    Fix del bug de duplicados: compara fecha exacta, no solo hora.
    """
    # Verificar si ya existe predicción para esta fecha+hora específica
    fila = (await db.execute(text("""
        SELECT 1 FROM auditoria_ia
        WHERE fecha = :fecha AND hora = :hora
        AND prediccion_1 IS NOT NULL
    """), {"fecha": fecha, "hora": hora})).fetchone()

    if fila:
        return  # Ya existe para esta fecha exacta — no tocar

    try:
        from app.services.motor_v10 import generar_prediccion
        pred = await generar_prediccion(db, hora)
        if pred and pred.get("prediccion_1"):
            await guardar_prediccion(
                db, fecha, hora, pred,
                forzar=False,
                origen="INICIAL"
            )
    except Exception as e:
        await db.rollback()
        logger.warning(f"⚠️ Error predicción inicial {hora}: {e}")


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


async def capturar_y_procesar(db):
    global _sorteos_desde_ultimo_recalculo

    ahora = datetime.now(TIMEZONE_VE)
    fecha_hoy = ahora.date()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": "https://www.google.com/"
    }

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            r = await client.get(BASE_URL)
            if r.status_code != 200:
                logger.warning(f"⚠️ Web bloqueada o caída (Status {r.status_code})")
                return

            html = r.text
            patron = r"(\d{2}):00.*?(\d{1,2})\s*[-–]\s*([a-zA-Záéíóúñ]+)"
            matches = re.findall(patron, html, re.DOTALL)
            nuevos_insertados = []

            for hora_str, num, nombre in matches:
                nombre_norm = NUM_A_ANIMAL.get(str(int(num)), nombre.lower().strip())

                res = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, 'Lotto Activo')
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                    RETURNING hora
                """), {"f": fecha_hoy, "h": hora_str, "a": nombre_norm})

                insertado = res.fetchone()
                if insertado:
                    nuevos_insertados.append((hora_str, nombre_norm))

                await actualizar_auditoria_post_sorteo(db, fecha_hoy, hora_str, nombre_norm)

            await db.commit()

            # ── Para cada sorteo NUEVO: aprender → corregir siguiente → tentativo si es 7PM ──
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

                    # 2. Recalcular predicción siguiente hora (corrección intraday)
                    #    Si es 07PM → hora_siguiente() devuelve None y no hace nada aquí
                    await recalcular_prediccion_siguiente(db, fecha_hoy, hora_nuevo, animal_nuevo)

                    # 3. Si es el último sorteo del día → generar TENTATIVO de mañana
                    #    Markov ya procesó el resultado de 07PM en el paso 1
                    if hora_nuevo == "07:00 PM":
                        await generar_tentativo_manana(db, fecha_hoy, animal_nuevo)

                _sorteos_desde_ultimo_recalculo += len(nuevos_insertados)

                # Recalcular rentabilidad cada N sorteos
                if _sorteos_desde_ultimo_recalculo >= _RECALCULO_CADA_N:
                    await recalcular_rentabilidad_automatico(db)
                    _sorteos_desde_ultimo_recalculo = 0

            # ── Durante el día: generar predicción inicial si no existe para esta hora ──
            # (cubre el caso de que el tentativo nocturno ya la puso → no la toca)
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
                # generar_prediccion_inicial ya verifica fecha exacta → no toca el tentativo
                await generar_prediccion_inicial(db, fecha_hoy, hora_prox)

    except Exception as e:
        logger.error(f"❌ Error en capturar_y_procesar: {e}")


async def ciclo_infinito():
    logger.info("🚀 [LottoAI PRO] Scheduler V11 — Tentativo nocturno + correcciones intraday")
    while True:
        try:
            ahora = datetime.now(TIMEZONE_VE)
            hora_ve = ahora.hour

            if 8 <= hora_ve <= 20:
                # Horario de sorteos — revisar cada 5 min
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                espera = 300

            elif hora_ve == 19 or (hora_ve == 20 and ahora.minute < 30):
                # Justo después del cierre (7PM-8:30PM) — revisar más seguido
                # para no perder el resultado de 07PM y generar el tentativo pronto
                async with AsyncSessionLocal() as db:
                    await capturar_y_procesar(db)
                espera = 120  # cada 2 min para no perder el 7PM

            else:
                # Noche/madrugada — esperar sin procesar
                espera = 1800

            await asyncio.sleep(espera)
        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
