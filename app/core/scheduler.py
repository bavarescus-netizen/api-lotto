"""
scheduler.py — Ciclo automático LOTTOAI PRO (CORREGIDO)
=======================================================
FIXES aplicados:
  FIX-1: _capturar_resultado() ahora hace 10 reintentos de 2 min (total 20 min)
          antes hacía 3 × 1 min = 3 min → insuficiente para el scraper real
  FIX-2: Importa aprender_sorteo() desde motor_v10 directamente
          (motor_aprendizaje era redundante y causaba imports circulares)
  FIX-3: _procesar_sorteo() espera 8 min (no 3) antes de buscar resultado
          para darle margen al scraper
  FIX-4: ciclo_infinito() genera predicción T-5 min con importación local
          (evita error si el módulo no cargó al arrancar)
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

HORAS_SORTEO = {
    8:  "08:00 AM", 9:  "09:00 AM", 10: "10:00 AM",
    11: "11:00 AM", 12: "12:00 PM", 13: "01:00 PM",
    14: "02:00 PM", 15: "03:00 PM", 16: "04:00 PM",
    17: "05:00 PM", 18: "06:00 PM", 19: "07:00 PM",
}


# ─── Migración de columnas tentativo ─────────────────────────────────────────
async def migrar_columnas_tentativo(db: AsyncSession):
    cols = [
        "pred_tentativa_1 VARCHAR(80)",
        "pred_tentativa_2 VARCHAR(80)",
        "pred_tentativa_3 VARCHAR(80)",
        "origen VARCHAR(30) DEFAULT 'INICIAL'",
    ]
    for col_def in cols:
        try:
            await db.execute(text(
                f"ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS {col_def}"
            ))
            await db.commit()
        except Exception:
            await db.rollback()

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
        for hora_lbl in HORAS_SORTEO.values():
            await db.execute(text(
                "INSERT INTO rentabilidad_hora (hora) VALUES (:h) ON CONFLICT DO NOTHING"
            ), {"h": hora_lbl})
        await db.commit()
    except Exception:
        await db.rollback()


# ─── Startup ─────────────────────────────────────────────────────────────────
async def startup(db: AsyncSession):
    await migrar_columnas_tentativo(db)
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


# ─── FIX-1: Captura con 10 reintentos de 2 min ──────────────────────────────
async def _capturar_resultado(db: AsyncSession, hora_label: str) -> str | None:
    """
    Busca el resultado real en historico.
    10 reintentos × 2 min = hasta 20 min de margen para el scraper.
    Antes: 3 × 1 min = solo 3 min → siempre fallaba.
    """
    hoy = date.today()
    MAX_INTENTOS = 10
    ESPERA_SEGUNDOS = 120  # 2 minutos

    for intento in range(MAX_INTENTOS):
        try:
            row = (await db.execute(text("""
                SELECT animalito FROM historico
                WHERE loteria = 'Lotto Activo'
                  AND fecha = :hoy
                  AND TRIM(hora) = TRIM(:hora)
                ORDER BY fecha DESC LIMIT 1
            """), {"hoy": hoy, "hora": hora_label})).fetchone()

            if row and row[0]:
                logger.info(f"✅ Resultado capturado {hora_label}: {row[0]} (intento {intento+1})")
                return row[0]
        except Exception as e:
            logger.warning(f"⚠️ Error consultando resultado {hora_label}: {e}")
            await db.rollback()

        if intento < MAX_INTENTOS - 1:
            logger.info(
                f"⏳ Sin resultado para {hora_label} "
                f"(intento {intento+1}/{MAX_INTENTOS}), "
                f"reintentando en {ESPERA_SEGUNDOS//60} min..."
            )
            await asyncio.sleep(ESPERA_SEGUNDOS)

    logger.warning(
        f"⚠️ Sin resultado real para {hora_label} "
        f"después de {MAX_INTENTOS} intentos ({MAX_INTENTOS * ESPERA_SEGUNDOS // 60} min)"
    )
    return None


# ─── FIX-2 + FIX-3: Ciclo de un sorteo ──────────────────────────────────────
async def _procesar_sorteo(hora_int: int):
    """
    Flujo completo para un sorteo:
    1. Verificar/generar predicción
    2. Esperar 8 min para que el scraper capture (era 3 min → insuficiente)
    3. Buscar resultado con 10 reintentos × 2 min
    4. FIX-2: Aprender usando aprender_sorteo() de motor_v10 (no motor_aprendizaje)
    """
    hora_label = HORAS_SORTEO[hora_int]
    ahora = datetime.now(VET)
    hoy = ahora.date()

    logger.info(f"🎯 Procesando sorteo {hora_label} — {hoy}")

    # ── Verificar/generar predicción ──────────────────────────────────────────
    async with AsyncSessionLocal() as db:
        try:
            existe_pred = (await db.execute(text("""
                SELECT 1 FROM auditoria_ia
                WHERE fecha = :hoy AND hora = :hora
                  AND prediccion_1 IS NOT NULL
            """), {"hoy": hoy, "hora": hora_label})).fetchone()

            if not existe_pred:
                logger.warning(f"⚠️ No hay predicción para {hora_label} — generando ahora")
                try:
                    from app.services.motor_v10 import generar_prediccion
                    pred = await generar_prediccion(db, hora_label)
                    if pred and pred.get("top3"):
                        top3 = pred["top3"]
                        p1 = top3[0]["animal"].lower() if len(top3) > 0 else None
                        p2 = top3[1]["animal"].lower() if len(top3) > 1 else None
                        p3 = top3[2]["animal"].lower() if len(top3) > 2 else None
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
                            "p1": p1, "p2": p2, "p3": p3,
                            "conf": float(pred.get("confianza_idx", 0)),
                            "conf_h": float(pred.get("efectividad_hora_top3", 0)),
                            "rent": bool(pred.get("hora_premium", False)),
                        })
                        await db.commit()
                        logger.info(f"📌 Predicción generada en el momento: {hora_label} → {p1}/{p2}/{p3}")
                except Exception as e_pred:
                    logger.error(f"❌ Error generando predicción {hora_label}: {e_pred}")
                    await db.rollback()
        except Exception as e:
            logger.error(f"❌ Error verificando predicción {hora_label}: {e}")
            await db.rollback()

    # ── FIX-3: Esperar 8 min (antes 3 min era insuficiente) ──────────────────
    logger.info(f"⏳ Esperando 8 min para que el scraper capture {hora_label}...")
    await asyncio.sleep(480)

    # ── Capturar resultado y aprender ─────────────────────────────────────────
    async with AsyncSessionLocal() as db:
        try:
            animal_real = await _capturar_resultado(db, hora_label)

            if animal_real:
                # FIX-2: Usar aprender_sorteo de motor_v10 directamente
                # motor_aprendizaje era redundante y causaba imports circulares
                from app.services.motor_v10 import aprender_sorteo
                resultado = await aprender_sorteo(db, hoy, hora_label, animal_real)
                logger.info(
                    f"🧠 Aprendizaje {hora_label}: "
                    f"real={animal_real} "
                    f"top1={resultado.get('acerto_top1')} "
                    f"top3={resultado.get('acerto_top3')} "
                    f"señal={resultado.get('señal_dominante')}"
                )

                # Actualizar rentabilidad_hora con el resultado de hoy
                try:
                    ac1 = 1 if resultado.get("acerto_top1") else 0
                    ac3 = 1 if resultado.get("acerto_top3") else 0
                    await db.execute(text("""
                        INSERT INTO rentabilidad_hora
                            (hora, total_sorteos, aciertos_top1, aciertos_top3,
                             efectividad_top1, efectividad_top3, es_rentable)
                        VALUES (:hora, 1, :ac1, :ac3, 0, 0, FALSE)
                        ON CONFLICT (hora) DO UPDATE SET
                            total_sorteos  = rentabilidad_hora.total_sorteos + 1,
                            aciertos_top1  = rentabilidad_hora.aciertos_top1 + :ac1,
                            aciertos_top3  = rentabilidad_hora.aciertos_top3 + :ac3,
                            efectividad_top1 = ROUND(
                                (rentabilidad_hora.aciertos_top1 + :ac1)::numeric /
                                NULLIF(rentabilidad_hora.total_sorteos + 1, 0) * 100, 2
                            ),
                            efectividad_top3 = ROUND(
                                (rentabilidad_hora.aciertos_top3 + :ac3)::numeric /
                                NULLIF(rentabilidad_hora.total_sorteos + 1, 0) * 100, 2
                            ),
                            es_rentable = (
                                (rentabilidad_hora.aciertos_top3 + :ac3)::float /
                                NULLIF(rentabilidad_hora.total_sorteos + 1, 0) * 100
                            ) >= 10.0,
                            updated_at = NOW()
                    """), {"hora": hora_label, "ac1": ac1, "ac3": ac3})
                    await db.commit()
                except Exception as e_rent:
                    logger.warning(f"⚠️ Error actualizando rentabilidad_hora {hora_label}: {e_rent}")
                    await db.rollback()
            else:
                logger.warning(
                    f"⚠️ Sin resultado para {hora_label} después de todos los reintentos — "
                    f"aprendizaje omitido. El scraper puede estar caído."
                )
        except Exception as e:
            logger.error(f"❌ Error en aprendizaje post-sorteo {hora_label}: {e}")
            await db.rollback()


# ─── Recalibración semanal ────────────────────────────────────────────────────
async def _recalibrar_semanal():
    """Cada sábado a las 8PM recalcula pesos con historia completa ponderada."""
    logger.info("📊 Iniciando recalibración semanal...")
    async with AsyncSessionLocal() as db:
        try:
            # Usa aprender_desde_historico de motor_v10 para recalibrar
            from app.services.motor_v10 import aprender_desde_historico
            from datetime import date as _date
            fecha_inicio = _date.today() - timedelta(days=365)
            resultado = await aprender_desde_historico(db, fecha_inicio, dias_por_generacion=30)
            logger.info(
                f"✅ Recalibración semanal completada: "
                f"ef_top1={resultado.get('efectividad_top1')}% "
                f"ef_top3={resultado.get('efectividad_top3')}%"
            )
        except Exception as e:
            logger.error(f"❌ Error en recalibración semanal: {e}")


# ─── Ciclo principal ──────────────────────────────────────────────────────────
async def ciclo_infinito():
    """
    Ciclo principal del scheduler.
    - T-5 min antes de cada sorteo: generar predicción tentativa
    - T+0 (hora exacta): activar proceso completo (captura + aprendizaje)
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
                # Dormir 50 min para evitar doble disparo
                await asyncio.sleep(50 * 60)
                continue

            # ── FIX-4: Generar predicción tentativa T-5 min ──────────────────
            hora_siguiente = hora_int + 1
            if hora_siguiente in HORAS_SORTEO and minuto == 55:
                hora_label = HORAS_SORTEO[hora_siguiente]
                hoy = ahora.date()
                async with AsyncSessionLocal() as db:
                    try:
                        existe = (await db.execute(text("""
                            SELECT 1 FROM auditoria_ia
                            WHERE fecha = :hoy AND hora = :hora
                              AND prediccion_1 IS NOT NULL
                        """), {"hoy": hoy, "hora": hora_label})).fetchone()

                        if not existe:
                            from app.services.motor_v10 import generar_prediccion
                            pred = await generar_prediccion(db, hora_label)
                            if pred and pred.get("top3"):
                                top3 = pred["top3"]
                                p1 = top3[0]["animal"].lower() if len(top3) > 0 else None
                                p2 = top3[1]["animal"].lower() if len(top3) > 1 else None
                                p3 = top3[2]["animal"].lower() if len(top3) > 2 else None
                                await db.execute(text("""
                                    INSERT INTO auditoria_ia
                                        (fecha, hora, animal_predicho, prediccion_1,
                                         prediccion_2, prediccion_3, confianza_pct,
                                         confianza_hora, es_hora_rentable,
                                         pred_tentativa_1, pred_tentativa_2,
                                         pred_tentativa_3, origen)
                                    VALUES
                                        (:fecha, :hora, :p1, :p1, :p2, :p3,
                                         :conf, :conf_h, :rent,
                                         :p1, :p2, :p3, 'TENTATIVA')
                                    ON CONFLICT (fecha, hora) DO NOTHING
                                """), {
                                    "fecha": hoy, "hora": hora_label,
                                    "p1": p1, "p2": p2, "p3": p3,
                                    "conf": float(pred.get("confianza_idx", 0)),
                                    "conf_h": float(pred.get("efectividad_hora_top3", 0)),
                                    "rent": bool(pred.get("hora_premium", False)),
                                })
                                await db.commit()
                                logger.info(
                                    f"📌 Predicción tentativa guardada: "
                                    f"{hora_label} → {p1}/{p2}/{p3}"
                                )
                    except Exception as e:
                        logger.error(f"❌ Error predicción tentativa {hora_label}: {e}")
                        await db.rollback()

            await asyncio.sleep(30)

        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
