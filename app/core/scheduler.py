"""
scheduler.py — Ciclo automático LOTTOAI PRO
============================================
FIXES aplicados:
  FIX-1: _capturar_resultado() hace 10 reintentos × 2 min (20 min total)
  FIX-2: Importa aprender_sorteo() desde motor_v10 directamente
  FIX-3: _procesar_sorteo() espera 8 min antes de buscar resultado
  FIX-4: ciclo_infinito() genera predicción T-5 min con importación local
  FIX-5: Scraper integrado en _procesar_sorteo() — ya no depende de llamadas externas
  FIX-6: El scheduler se detiene solo después del último sorteo del día (19:00 VET)
         para no mantener Render ni Neon activos innecesariamente
"""

import asyncio
import logging
import httpx
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db import AsyncSessionLocal

logger = logging.getLogger(__name__)

VET = ZoneInfo("America/Caracas")

HORAS_SORTEO = {
    8:  "08:00 AM",
    9:  "09:00 AM",
    10: "10:00 AM",
    11: "11:00 AM",
    12: "12:00 PM",
    13: "01:00 PM",
    14: "02:00 PM",
    15: "03:00 PM",
    16: "04:00 PM",
    17: "05:00 PM",
    18: "06:00 PM",
    19: "07:00 PM",   # ← último sorteo del día
}

HORA_ULTIMO_SORTEO = 19   # 07:00 PM — después de procesar este, el ciclo duerme hasta el día siguiente


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
                es_rentable       BOOLEAN DEFAULT FALSE
            )
        """))
        # Migración: updated_at puede no existir en tablas ya creadas
        await db.execute(text(
            "ALTER TABLE rentabilidad_hora "
            "ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()"
        ))
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


# ─── FIX-5: Scraper integrado vía HTTP interno ───────────────────────────────
async def _ejecutar_scraper(contexto: str = "") -> int:
    """
    Llama al endpoint /cargar-ultimo vía HTTP localhost.
    Evita problemas de import entre módulos — el scheduler llama al
    scraper igual que un cliente externo, sin dependencia de paths.
    Retorna cantidad de registros nuevos insertados.
    """
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(
                "http://localhost:10000/cargar-ultimo",
                headers={"User-Agent": "scheduler-interno"}
            )
            if r.status_code == 200:
                data = r.json()
                insertados  = data.get("nuevos", 0)
                encontrados = data.get("encontrados", 0)
                logger.info(
                    f"🌐 Scraper [{contexto}]: "
                    f"{encontrados} encontrados, {insertados} nuevos en historico"
                )
                return insertados
            else:
                logger.warning(f"⚠️ Scraper [{contexto}]: HTTP {r.status_code}")
                return 0
    except Exception as e:
        logger.error(f"❌ Error en scraper [{contexto}]: {e}")
        return 0


# ─── FIX-1: Captura con 10 reintentos × 2 min + re-scraping en 2, 5, 8 ──────
async def _capturar_resultado(db: AsyncSession, hora_label: str) -> str | None:
    """
    Busca el resultado real en historico.
    - 10 reintentos × 2 min = hasta 20 min de margen.
    - En los intentos 2, 5 y 8 vuelve a llamar al scraper por si el dato
      llegó tarde a lotoven.com.
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
                logger.info(
                    f"✅ Resultado capturado {hora_label}: {row[0]} "
                    f"(intento {intento + 1})"
                )
                return row[0]
        except Exception as e:
            logger.warning(f"⚠️ Error consultando resultado {hora_label}: {e}")
            await db.rollback()

        # Re-scraping en intentos 2, 5 y 8 para refrescar historico
        if intento in (2, 5, 8):
            await _ejecutar_scraper(contexto=f"reintento-{intento}-{hora_label}")

        if intento < MAX_INTENTOS - 1:
            logger.info(
                f"⏳ Sin resultado para {hora_label} "
                f"(intento {intento + 1}/{MAX_INTENTOS}), "
                f"reintentando en {ESPERA_SEGUNDOS // 60} min..."
            )
            await asyncio.sleep(ESPERA_SEGUNDOS)

    logger.warning(
        f"⚠️ Sin resultado real para {hora_label} "
        f"después de {MAX_INTENTOS} intentos "
        f"({MAX_INTENTOS * ESPERA_SEGUNDOS // 60} min)"
    )
    return None


# ─── FIX-2 + FIX-3 + FIX-5: Ciclo de un sorteo ──────────────────────────────
async def _procesar_sorteo(hora_int: int):
    """
    Flujo completo para un sorteo:
    1. Verificar/generar predicción
    2. Llamar al scraper para poblar historico  ← FIX-5
    3. Esperar 8 min (margen para datos tardíos en lotoven)
    4. Buscar resultado con 10 reintentos × 2 min (re-scraping en 2, 5, 8)
    5. Aprender usando aprender_sorteo() de motor_v10
    """
    hora_label = HORAS_SORTEO[hora_int]
    ahora = datetime.now(VET)
    hoy = ahora.date()

    logger.info(f"🎯 Procesando sorteo {hora_label} — {hoy}")

    # ── 1. Verificar/generar predicción ──────────────────────────────────────
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
                        logger.info(
                            f"📌 Predicción generada: {hora_label} → {p1}/{p2}/{p3}"
                        )
                except Exception as e_pred:
                    logger.error(f"❌ Error generando predicción {hora_label}: {e_pred}")
                    await db.rollback()
        except Exception as e:
            logger.error(f"❌ Error verificando predicción {hora_label}: {e}")
            await db.rollback()

    # ── 2. Esperar 2 min para que lotoven publique el resultado ──────────────
    logger.info(f"⏳ Esperando 2 min para que lotoven publique {hora_label}...")
    await asyncio.sleep(120)

    # ── 3. FIX-5: Scraper tras 2 min — lotoven ya debería tener el dato ──────
    await _ejecutar_scraper(contexto=f"inicial-{hora_label}")

    # ── 4. Esperar 6 min más por si el dato llegó tarde ───────────────────────
    logger.info(f"⏳ Esperando 6 min adicionales por si hay retraso — {hora_label}")
    await asyncio.sleep(360)

    # ── 4 + 5. Capturar resultado y aprender ─────────────────────────────────
    async with AsyncSessionLocal() as db:
        try:
            animal_real = await _capturar_resultado(db, hora_label)

            if animal_real:
                # FIX-2: aprender_sorteo de motor_v10 directamente
                from app.services.motor_v10 import aprender_sorteo
                resultado = await aprender_sorteo(db, hoy, hora_label, animal_real)
                logger.info(
                    f"🧠 Aprendizaje {hora_label}: "
                    f"real={animal_real} "
                    f"top1={resultado.get('acerto_top1')} "
                    f"top3={resultado.get('acerto_top3')} "
                    f"señal={resultado.get('señal_dominante')}"
                )

                # Actualizar rentabilidad_hora
                try:
                    ac1 = 1 if resultado.get("acerto_top1") else 0
                    ac3 = 1 if resultado.get("acerto_top3") else 0
                    await db.execute(text("""
                        INSERT INTO rentabilidad_hora
                            (hora, total_sorteos, aciertos_top1, aciertos_top3,
                             efectividad_top1, efectividad_top3, es_rentable)
                        VALUES (:hora, 1, :ac1, :ac3, 0, 0, FALSE)
                        ON CONFLICT (hora) DO UPDATE SET
                            total_sorteos    = rentabilidad_hora.total_sorteos + 1,
                            aciertos_top1    = rentabilidad_hora.aciertos_top1 + :ac1,
                            aciertos_top3    = rentabilidad_hora.aciertos_top3 + :ac3,
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
                    """), {"hora": hora_label, "ac1": ac1, "ac3": ac3})
                    await db.commit()
                except Exception as e_rent:
                    logger.warning(
                        f"⚠️ Error actualizando rentabilidad_hora {hora_label}: {e_rent}"
                    )
                    await db.rollback()
            else:
                logger.warning(
                    f"⚠️ Sin resultado para {hora_label} tras todos los reintentos — "
                    f"aprendizaje omitido."
                )
        except Exception as e:
            logger.error(f"❌ Error en aprendizaje post-sorteo {hora_label}: {e}")
            await db.rollback()


# ─── Recalibración semanal ────────────────────────────────────────────────────
async def _recalibrar_semanal():
    """Cada sábado a las 20:00 VET recalcula pesos con historia completa."""
    logger.info("📊 Iniciando recalibración semanal...")
    async with AsyncSessionLocal() as db:
        try:
            from app.services.motor_v10 import aprender_desde_historico
            from datetime import date as _date
            fecha_inicio = _date.today() - timedelta(days=365)
            resultado = await aprender_desde_historico(
                db, fecha_inicio, dias_por_generacion=30
            )
            logger.info(
                f"✅ Recalibración semanal: "
                f"ef_top1={resultado.get('efectividad_top1')}% "
                f"ef_top3={resultado.get('efectividad_top3')}%"
            )
        except Exception as e:
            logger.error(f"❌ Error en recalibración semanal: {e}")


# ─── FIX-6: Calcular segundos hasta el próximo día 07:55 AM VET ──────────────
def _segundos_hasta_manana_755() -> float:
    """
    Calcula cuántos segundos faltan para las 07:55 AM VET del día siguiente.
    A esa hora el ciclo se reactiva para generar la predicción tentativa
    del primer sorteo (08:00 AM).
    """
    ahora = datetime.now(VET)
    manana = ahora.date() + timedelta(days=1)
    despertar = datetime(
        manana.year, manana.month, manana.day,
        7, 55, 0,
        tzinfo=VET
    )
    delta = (despertar - ahora).total_seconds()
    return max(delta, 60)   # mínimo 1 min por seguridad


# ─── Ciclo principal ──────────────────────────────────────────────────────────
async def ciclo_infinito():
    """
    Ciclo principal del scheduler.

    Flujo diario:
    - 07:55 AM    → predicción tentativa 08:00 AM
    - 08:00 AM    → _procesar_sorteo (scraper + captura + aprendizaje)
    - …repite cada hora hasta…
    - 07:00 PM    → _procesar_sorteo del último sorteo
    - ~07:30 PM+  → FIX-6: duerme hasta las 07:55 AM del día siguiente
                    (no consume Neon ni Render hasta entonces)

    Semanal:
    - Sábado 20:00 VET → recalibración de pesos
    """
    logger.info(
        "🚀 Scheduler LOTTOAI iniciado — "
        "aprendizaje automático + scraper integrado (12x/día)"
    )

    while True:
        try:
            ahora = datetime.now(VET)
            hora_int = ahora.hour
            minuto = ahora.minute
            dia_semana = ahora.weekday()  # 5 = sábado

            # ── Recalibración semanal (sábado 20:00 VET) ──────────────────────
            if dia_semana == 5 and hora_int == 20 and minuto == 0:
                asyncio.create_task(_recalibrar_semanal())
                await asyncio.sleep(60)
                continue

            # ── Sorteos: activar proceso en el minuto exacto ──────────────────
            if hora_int in HORAS_SORTEO and minuto == 0:
                asyncio.create_task(_procesar_sorteo(hora_int))

                if hora_int == HORA_ULTIMO_SORTEO:
                    # FIX-6: último sorteo del día procesado
                    # Dormir hasta 07:55 AM del día siguiente para no
                    # consumir Neon ni mantener Render activo toda la noche
                    segundos = _segundos_hasta_manana_755()
                    horas_restantes = round(segundos / 3600, 1)
                    logger.info(
                        f"🌙 Último sorteo del día ({hora_int}:00) lanzado. "
                        f"Durmiendo {horas_restantes}h hasta las 07:55 AM VET de mañana."
                    )
                    await asyncio.sleep(segundos)
                else:
                    # Dormir 50 min para evitar doble disparo en el mismo sorteo
                    await asyncio.sleep(50 * 60)

                continue

            # ── FIX-4: Predicción tentativa T-5 min ──────────────────────────
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
                                    f"📌 Predicción tentativa: "
                                    f"{hora_label} → {p1}/{p2}/{p3}"
                                )
                    except Exception as e:
                        logger.error(
                            f"❌ Error predicción tentativa {hora_label}: {e}"
                        )
                        await db.rollback()

            # Pulso normal cada 30 seg — solo activo entre 07:55 y ~20:30 VET
            await asyncio.sleep(30)

        except Exception as e:
            logger.error(f"❌ Error en ciclo_infinito: {e}")
            await asyncio.sleep(60)
