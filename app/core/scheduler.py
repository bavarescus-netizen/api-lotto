"""
scheduler.py — Ciclo automático LOTTOAI PRO
=============================================
Flujo por sorteo (12 veces al día, cada hora de 8AM a 7PM VET):

  T-5 min  → generar_prediccion() y guardarla en auditoria_ia
  T+3 min  → capturar resultado real del scraper
  T+3 min  → aprender_tras_sorteo() AUTOMÁTICO ← NUEVO
  Sábado   → recalcular_todos_los_pesos() con historia ponderada ← NUEVO

El aprendizaje ocurre 12 veces al día.
Los pesos se recalibran con historia completa 1 vez/semana.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db import AsyncSessionLocal

logger = logging.getLogger(__name__)

VET = ZoneInfo("America/Caracas")

# Horarios de sorteo (hora entera en Venezuela)
HORAS_SORTEO = {
    8: "08:00 AM", 9: "09:00 AM", 10: "10:00 AM",
    11: "11:00 AM", 12: "12:00 PM", 13: "01:00 PM",
    14: "02:00 PM", 15: "03:00 PM", 16: "04:00 PM",
    17: "05:00 PM", 18: "06:00 PM", 19: "07:00 PM",
}

# ─── Migración de columnas tentativo (V11) ───────────────────────────────────
async def migrar_columnas_tentativo(db: AsyncSession):
    """Agrega columnas de predicción tentativa si no existen."""
    cols = [
        "pred_tentativa_1 VARCHAR(80)",
        "pred_tentativa_2 VARCHAR(80)",
        "pred_tentativa_3 VARCHAR(80)",
        "origen VARCHAR(30) DEFAULT 'INICIAL'",
    ]
    for col_def in cols:
        col_name = col_def.split()[0]
        try:
            await db.execute(text(
                f"ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS {col_def}"
            ))
            await db.commit()
        except Exception:
            await db.rollback()

    # Migrar tabla rentabilidad_hora si no existe
    try:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS rentabilidad_hora (
                hora              VARCHAR(20) PRIMARY KEY,
                total_sorteos     INT DEFAULT 0,
                aciertos_top1     INT DEFAULT 0,
                aciertos_top3     INT DEFAULT 0,
                efectividad_top1  FLOAT DEFAULT 0,
                efectividad_top3  FLOAT DEFAULT 0,
                es_rentable       BOOLEAN DEFAULT FALSE,
                updated_at        TIMESTAMP DEFAULT NOW()
            )
        """))
        # Insertar horas si no existen
        for hora_lbl in HORAS_SORTEO.values():
            await db.execute(text(
                "INSERT INTO rentabilidad_hora (hora) VALUES (:h) ON CONFLICT DO NOTHING"
            ), {"h": hora_lbl})
        await db.commit()
    except Exception:
        await db.rollback()


# ─── Startup ─────────────────────────────────────────────────────────────────
async def startup(db: AsyncSession):
    """Inicialización: migraciones y carga de patrones base."""
    await migrar_columnas_tentativo(db)

    # Migrar tabla patrones si no existe
    try:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS patrones_confirmados (
                id          SERIAL PRIMARY KEY,
                hora        VARCHAR(20) NOT NULL,
                patron_tipo VARCHAR(50),
                descripcion TEXT,
                fuerza      FLOAT DEFAULT 0,
                activo      BOOLEAN DEFAULT TRUE,
                creado      TIMESTAMP DEFAULT NOW()
            )
        """))
        await db.commit()
    except Exception:
        await db.rollback()

    logger.info("✅ Startup scheduler completado")


# ─── Captura de resultado real ────────────────────────────────────────────────
async def _capturar_resultado(db: AsyncSession, hora_label: str) -> str | None:
    """
    Intenta obtener el resultado real del sorteo de esta hora.
    Busca en la tabla historico si ya fue capturado por el scraper.
    Reintenta hasta 3 veces con 1 min de espera.
    """
    hoy = date.today()
    for intento in range(3):
        row = (await db.execute(text("""
            SELECT animalito FROM historico
            WHERE loteria = 'Lotto Activo'
              AND fecha = :hoy
              AND hora = :hora
            ORDER BY fecha DESC LIMIT 1
        """), {"hoy": hoy, "hora": hora_label})).fetchone()

        if row and row[0]:
            return row[0]

        if intento < 2:
            logger.info(f"⏳ Esperando resultado {hora_label} (intento {intento+1}/3)...")
            await asyncio.sleep(60)  # esperar 1 minuto y reintentar

    logger.warning(f"⚠️ Sin resultado real para {hora_label} después de 3 intentos")
    return None


# ─── Ciclo de un sorteo completo ─────────────────────────────────────────────
async def _procesar_sorteo(hora_int: int):
    """
    Flujo completo para un sorteo:
    1. Generar predicción (T-5 min, ya debería estar guardada)
    2. Esperar 3 min después del sorteo
    3. Capturar resultado real
    4. Aprender automáticamente
    """
    hora_label = HORAS_SORTEO[hora_int]
    ahora = datetime.now(VET)
    hoy = ahora.date()

    logger.info(f"🎯 Procesando sorteo {hora_label} — {hoy}")

    async with AsyncSessionLocal() as db:
        try:
            # ── Asegurar que la predicción existe ─────────────────────────────
            existe_pred = (await db.execute(text("""
                SELECT 1 FROM auditoria_ia
                WHERE fecha = :hoy AND hora = :hora
                  AND prediccion_1 IS NOT NULL
            """), {"hoy": hoy, "hora": hora_label})).fetchone()

            if not existe_pred:
                logger.warning(f"⚠️ No hay predicción guardada para {hora_label} — generando ahora")
                try:
                    from app.services.motor_v10 import generar_prediccion  # path CORRECTO
                    pred = await generar_prediccion(db, hora_label)
                    if pred and pred.get("prediccion_1"):
                        await db.execute(text("""
                            INSERT INTO auditoria_ia
                                (fecha, hora, animal_predicho, prediccion_1,
                                 prediccion_2, prediccion_3, confianza_pct,
                                 confianza_hora, es_hora_rentable)
                            VALUES
                                (:fecha, :hora, :p1, :p1, :p2, :p3,
                                 :conf, :conf_h, :rent)
                            ON CONFLICT (fecha, hora) DO NOTHING
                        """), {
                            "fecha": hoy, "hora": hora_label,
                            "p1": pred.get("prediccion_1"),
                            "p2": pred.get("prediccion_2"),
                            "p3": pred.get("prediccion_3"),
                            "conf": pred.get("confianza_pct", 0),
                            "conf_h": pred.get("confianza_hora", 0),
                            "rent": pred.get("es_hora_rentable", False),
                        })
                        await db.commit()
                except Exception as e_pred:
                    logger.error(f"❌ Error generando predicción {hora_label}: {e_pred}")

        except Exception as e:
            logger.error(f"❌ Error verificando predicción {hora_label}: {e}")

    # ── Esperar 3 minutos después del sorteo para que el scraper capture ──────
    logger.info(f"⏳ Esperando 3 min para resultado de {hora_label}...")
    await asyncio.sleep(180)

    # ── Capturar resultado y aprender ─────────────────────────────────────────
    async with AsyncSessionLocal() as db:
        try:
            animal_real = await _capturar_resultado(db, hora_label)

            if animal_real:
                # ✅ APRENDIZAJE AUTOMÁTICO POST-SORTEO
                from app.services.motor_aprendizaje import aprender_tras_sorteo
                resultado = await aprender_tras_sorteo(db, hoy, hora_label, animal_real)
                logger.info(
                    f"🧠 Aprendizaje completado {hora_label}: "
                    f"top1={resultado['acierto_top1']} "
                    f"top3={resultado['acierto_top3']} "
                    f"λ={resultado['lambda_nuevo']}"
                )
            else:
                logger.warning(f"⚠️ Sin resultado para {hora_label} — aprendizaje omitido")

        except Exception as e:
            logger.error(f"❌ Error en aprendizaje post-sorteo {hora_label}: {e}")


# ─── Recalibración semanal ────────────────────────────────────────────────────
async def _recalibrar_semanal():
    """
    Cada sábado a las 8PM recalcula pesos con historia completa ponderada.
    Los datos recientes pesan hasta 10x más que los de 2018-2022.
    """
    logger.info("📊 Iniciando recalibración semanal con pesos temporales...")
    async with AsyncSessionLocal() as db:
        try:
            from app.services.motor_aprendizaje import recalcular_todos_los_pesos
            resultado = await recalcular_todos_los_pesos(db)
            logger.info(
                f"✅ Recalibración semanal completada: "
                f"{resultado['ok']}/{resultado['total']} horas actualizadas"
            )
        except Exception as e:
            logger.error(f"❌ Error en recalibración semanal: {e}")


# ─── Ciclo principal ──────────────────────────────────────────────────────────
async def ciclo_infinito():
    """
    Ciclo principal del scheduler. Corre indefinidamente.
    Se activa en los minutos correctos para cada sorteo.

    Lógica de tiempo:
    - A T-5 min antes de cada sorteo: verificar/generar predicción
    - A T+0 (hora exacta): activar proceso de captura + aprendizaje
    - Sábado 20:00 VET: recalibración semanal de pesos
    """
    logger.info("🚀 Scheduler LOTTOAI iniciado — aprendizaje automático activo (12x/día)")

    while True:
        try:
            ahora = datetime.now(VET)
            hora_int = ahora.hour
            minuto = ahora.minute
            dia_semana = ahora.weekday()  # 5 = sábado

            # ── Recalibración semanal (sábado 20:00) ──────────────────────────
            if dia_semana == 5 and hora_int == 20 and minuto == 0:
                asyncio.create_task(_recalibrar_semanal())
                await asyncio.sleep(60)
                continue

            # ── Sorteos: activar proceso en el minuto exacto ──────────────────
            if hora_int in HORAS_SORTEO and minuto == 0:
                asyncio.create_task(_procesar_sorteo(hora_int))
                # Esperar al menos 50 min antes de verificar de nuevo
                # (evitar doble disparo si el sleep es impreciso)
                await asyncio.sleep(50 * 60)
                continue

            # ── Generar predicciones 5 min antes del sorteo ──────────────────
            hora_siguiente = hora_int + 1
            if hora_siguiente in HORAS_SORTEO and minuto == 55:
                hora_label = HORAS_SORTEO[hora_siguiente]
                hoy = ahora.date()
                async with AsyncSessionLocal() as db:
                    existe = (await db.execute(text("""
                        SELECT 1 FROM auditoria_ia
                        WHERE fecha = :hoy AND hora = :hora
                          AND prediccion_1 IS NOT NULL
                    """), {"hoy": hoy, "hora": hora_label})).fetchone()

                    if not existe:
                        try:
                            from app.services.motor_v10 import generar_prediccion
                            pred = await generar_prediccion(db, hora_label)
                            if pred and pred.get("prediccion_1"):
                                await db.execute(text("""
                                    INSERT INTO auditoria_ia
                                        (fecha, hora, animal_predicho, prediccion_1,
                                         prediccion_2, prediccion_3, confianza_pct,
                                         confianza_hora, es_hora_rentable,
                                         pred_tentativa_1, pred_tentativa_2, pred_tentativa_3,
                                         origen)
                                    VALUES
                                        (:fecha, :hora, :p1, :p1, :p2, :p3,
                                         :conf, :conf_h, :rent,
                                         :p1, :p2, :p3, 'TENTATIVA')
                                    ON CONFLICT (fecha, hora) DO NOTHING
                                """), {
                                    "fecha": hoy, "hora": hora_label,
                                    "p1": pred.get("prediccion_1"),
                                    "p2": pred.get("prediccion_2"),
                                    "p3": pred.get("prediccion_3"),
                                    "conf": pred.get("confianza_pct", 0),
                                    "conf_h": pred.get("confianza_hora", 0),
                                    "rent": pred.get("es_hora_rentable", False),
                                })
                                await db.commit()
                                logger.info(f"📌 Predicción tentativa guardada: {hora_label}")
                        except Exception as e:
                            logger.error(f"❌ Error predicción tentativa {hora_label}: {e}")

            # Dormir 30 segundos y revisar de nuevo
            await asyncio.sleep(30)

        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
