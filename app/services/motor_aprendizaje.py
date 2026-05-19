"""
motor_aprendizaje.py — Aprendizaje automático post-sorteo
===========================================================
Se ejecuta automáticamente tras CADA sorteo (12 veces al día).

Lógica de pesos temporales:
  - Datos 2018-2022  → peso base (histórico lejano)
  - Datos 2023-2024  → peso medio
  - Últimos 90 días  → peso ALTO (3x más peso que el histórico)
  - Últimos 14 días  → peso MUY ALTO (6x)
  - Últimos 3 días   → peso MÁXIMO (10x)

Esto implementa lo que pediste: "más peso los últimos días".
El λ de decay temporal se ajusta automáticamente por hora según
la volatilidad reciente de esa hora.
"""

import math
import logging
from datetime import date, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ─── Configuración de pesos temporales ───────────────────────────────────────
# Cuántas veces más vale un dato reciente vs uno de 2018
PESO_HISTORICO_LEJANO  = 1.0    # 2018–2022
PESO_HISTORICO_MEDIO   = 2.0    # 2023–2024
PESO_RECIENTE_90D      = 3.0    # últimos 90 días
PESO_RECIENTE_14D      = 6.0    # últimos 14 días
PESO_RECIENTE_3D       = 10.0   # últimos 3 días  ← MÁXIMO IMPACTO

# Tasa de aprendizaje base (qué tan rápido ajusta los pesos del motor)
TASA_BASE              = 0.04   # era 0.02 — aumentada para aprender más rápido
TASA_ACIERTO           = 0.06   # si acertó: refuerzo positivo mayor
TASA_ERROR             = 0.02   # si falló: ajuste suave (no sobrepenalizar)


def _peso_temporal(fecha_sorteo: date) -> float:
    """Retorna el multiplicador de peso según antigüedad del dato."""
    hoy = date.today()
    dias = (hoy - fecha_sorteo).days

    if dias <= 3:
        return PESO_RECIENTE_3D
    elif dias <= 14:
        return PESO_RECIENTE_14D
    elif dias <= 90:
        return PESO_RECIENTE_90D
    elif fecha_sorteo.year >= 2023:
        return PESO_HISTORICO_MEDIO
    else:
        return PESO_HISTORICO_LEJANO


async def aprender_tras_sorteo(
    db: AsyncSession,
    fecha: date,
    hora: str,
    animal_real: str
) -> dict:
    """
    Llamar inmediatamente tras capturar el resultado real de un sorteo.
    1. Actualiza auditoria_ia con acierto=True/False
    2. Ajusta motor_pesos_hora para esa hora
    3. Actualiza markov_transiciones con el nuevo par (previo → real)
    4. Recalcula λ óptimo para esa hora
    5. Guarda en aprendizaje_sorteo el registro del ciclo
    """
    resultado = {
        "fecha": str(fecha),
        "hora": hora,
        "animal_real": animal_real,
        "acierto_top1": False,
        "acierto_top3": False,
        "pesos_ajustados": {},
        "lambda_nuevo": 0.008,
        "markov_actualizado": False,
    }

    try:
        # ── 1. Leer predicción guardada para este sorteo ──────────────────────
        pred = (await db.execute(text("""
            SELECT prediccion_1, prediccion_2, prediccion_3,
                   confianza_pct, id
            FROM auditoria_ia
            WHERE fecha = :fecha AND hora = :hora
            ORDER BY id DESC LIMIT 1
        """), {"fecha": fecha, "hora": hora})).fetchone()

        if not pred:
            logger.warning(f"⚠️ No hay predicción para {fecha} {hora} — saltando aprendizaje")
            return resultado

        p1, p2, p3 = (pred[0] or "").lower(), (pred[1] or "").lower(), (pred[2] or "").lower()
        animal_norm = animal_real.lower().strip()
        audit_id = pred[4]

        acierto_top1 = (animal_norm == p1)
        acierto_top3 = (animal_norm in [p1, p2, p3])
        resultado["acierto_top1"] = acierto_top1
        resultado["acierto_top3"] = acierto_top3

        # ── 2. Actualizar auditoria_ia con resultado real ─────────────────────
        await db.execute(text("""
            UPDATE auditoria_ia
            SET resultado_real = :real,
                acierto = :ac
            WHERE id = :id
        """), {"real": animal_real, "ac": acierto_top1, "id": audit_id})

        # ── 3. Calcular λ adaptativo por hora ─────────────────────────────────
        # λ alto = olvidar rápido (hora volátil)
        # λ bajo = confiar en histórico (hora estable)
        volatilidad = (await db.execute(text("""
            SELECT
                STDDEV(CASE WHEN LOWER(TRIM(resultado_real)) = LOWER(TRIM(prediccion_1)) THEN 1.0 ELSE 0.0 END)
            FROM auditoria_ia
            WHERE hora = :hora
              AND resultado_real NOT IN ('PENDIENTE','')
              AND fecha >= CURRENT_DATE - INTERVAL '30 days'
        """), {"hora": hora})).scalar() or 0.0

        # λ entre 0.004 (estable) y 0.020 (muy volátil)
        lambda_nuevo = round(max(0.004, min(0.020, 0.004 + float(volatilidad) * 0.08)), 4)
        resultado["lambda_nuevo"] = lambda_nuevo

        # ── 4. Leer pesos actuales de esta hora ───────────────────────────────
        pesos_row = (await db.execute(text("""
            SELECT peso_decay, peso_markov, peso_gap, peso_reciente, efectividad, generacion
            FROM motor_pesos_hora
            WHERE hora = :hora
            ORDER BY generacion DESC LIMIT 1
        """), {"hora": hora})).fetchone()

        if pesos_row:
            pd_  = float(pesos_row[0] or 0.25)
            pm_  = float(pesos_row[1] or 0.25)
            pg_  = float(pesos_row[2] or 0.25)
            pr_  = float(pesos_row[3] or 0.25)
            ef_  = float(pesos_row[4] or 0.0)
            gen_ = int(pesos_row[5] or 1)
        else:
            pd_, pm_, pg_, pr_, ef_, gen_ = 0.25, 0.25, 0.25, 0.25, 0.0, 1

        # ── 5. Ajustar pesos según resultado (con peso temporal) ──────────────
        # El multiplicador temporal hace que el sorteo de hoy pese MÁS
        # que uno de hace 6 meses al calcular la dirección del ajuste
        peso_hoy = _peso_temporal(fecha)
        tasa = TASA_ACIERTO if acierto_top3 else TASA_ERROR
        tasa_efectiva = tasa * (peso_hoy / PESO_RECIENTE_3D)  # normalizar a [0..tasa]

        if acierto_top3:
            # Acertó: reforzar la señal que más contribuyó
            # (heurística: si hay muchas transiciones Markov → reforzar Markov)
            mk_count = (await db.execute(text(
                "SELECT COUNT(*) FROM markov_transiciones WHERE hora = :hora"
            ), {"hora": hora})).scalar() or 0

            if mk_count > 100:
                pm_ = min(0.45, pm_ + tasa_efectiva)
                pd_ = max(0.10, pd_ - tasa_efectiva * 0.5)
            else:
                pd_ = min(0.45, pd_ + tasa_efectiva)
                pm_ = max(0.10, pm_ - tasa_efectiva * 0.5)
        else:
            # Falló: reducir levemente todos, aumentar el de mayor brecha
            pd_ = max(0.10, pd_ - tasa_efectiva * 0.3)
            pm_ = max(0.10, pm_ - tasa_efectiva * 0.3)
            pg_ = max(0.10, pg_ - tasa_efectiva * 0.2)
            pr_ = max(0.10, pr_ - tasa_efectiva * 0.2)

        # Renormalizar a suma=1.0
        total = pd_ + pm_ + pg_ + pr_
        if total > 0:
            pd_ = round(pd_ / total, 4)
            pm_ = round(pm_ / total, 4)
            pg_ = round(pg_ / total, 4)
            pr_ = round(1.0 - pd_ - pm_ - pg_, 4)  # garantizar suma exacta

        # Efectividad ponderada con peso temporal (reciente vale más)
        peso_ef = min(1.0, peso_hoy / 10.0)
        nueva_ef = round(ef_ * (1 - peso_ef * 0.1) + (1.0 if acierto_top3 else 0.0) * peso_ef * 0.1, 4)

        resultado["pesos_ajustados"] = {
            "peso_decay": pd_, "peso_markov": pm_,
            "peso_gap": pg_, "peso_reciente": pr_,
            "efectividad": nueva_ef
        }

        # ── 6. Guardar pesos ajustados ─────────────────────────────────────────
        await db.execute(text("""
            INSERT INTO motor_pesos_hora
                (hora, generacion, peso_decay, peso_markov, peso_gap, peso_reciente, efectividad, fecha)
            VALUES
                (:hora, :gen, :pd, :pm, :pg, :pr, :ef, NOW())
            ON CONFLICT (hora, generacion) DO UPDATE SET
                peso_decay    = EXCLUDED.peso_decay,
                peso_markov   = EXCLUDED.peso_markov,
                peso_gap      = EXCLUDED.peso_gap,
                peso_reciente = EXCLUDED.peso_reciente,
                efectividad   = EXCLUDED.efectividad,
                fecha         = NOW()
        """), {
            "hora": hora, "gen": gen_,
            "pd": pd_, "pm": pm_, "pg": pg_, "pr": pr_, "ef": nueva_ef
        })

        # ── 7. Actualizar Markov: registrar nueva transición ──────────────────
        # Obtener animal anterior en esta hora (penúltimo real, no el de hoy)
        previo_row = (await db.execute(text("""
            SELECT animalito FROM historico
            WHERE loteria = 'Lotto Activo' AND hora = :hora
              AND fecha < :fecha
            ORDER BY fecha DESC LIMIT 1
        """), {"hora": hora, "fecha": fecha})).fetchone()

        if previo_row and previo_row[0]:
            animal_previo = previo_row[0].lower().strip()

            # UPSERT correcto: incrementar frecuencia, NO reemplazar
            await db.execute(text("""
                INSERT INTO markov_transiciones
                    (hora, animal_previo, animal_sig, frecuencia, probabilidad)
                VALUES
                    (:hora, :previo, :sig, 1, 0)
                ON CONFLICT (hora, animal_previo, animal_sig) DO UPDATE SET
                    frecuencia = markov_transiciones.frecuencia + 1
            """), {"hora": hora, "previo": animal_previo, "sig": animal_norm})

            # Recalcular probabilidades para este animal_previo+hora
            await db.execute(text("""
                UPDATE markov_transiciones m
                SET probabilidad = ROUND(
                    (m.frecuencia::FLOAT /
                     NULLIF(sub.total, 0) * 100)::numeric, 2
                )
                FROM (
                    SELECT SUM(frecuencia) AS total
                    FROM markov_transiciones
                    WHERE hora = :hora AND animal_previo = :previo
                ) sub
                WHERE m.hora = :hora AND m.animal_previo = :previo
            """), {"hora": hora, "previo": animal_previo})

            resultado["markov_actualizado"] = True

        # ── 8. Registrar en aprendizaje_sorteo ───────────────────────────────
        # Leer pesos antes del ajuste para historial
        await db.execute(text("""
            INSERT INTO aprendizaje_sorteo
                (fecha, hora, animal_real, animal_pred1, animal_pred2, animal_pred3,
                 acerto_top1, acerto_top3, señal_dominante,
                 peso_despues, delta_ef, tasa_aprendizaje, generacion)
            VALUES
                (:fecha, :hora, :real, :p1, :p2, :p3,
                 :ac1, :ac3, :señal,
                 :pesos::jsonb, :delta, :tasa, :gen)
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_real   = EXCLUDED.animal_real,
                acerto_top1   = EXCLUDED.acerto_top1,
                acerto_top3   = EXCLUDED.acerto_top3,
                peso_despues  = EXCLUDED.peso_despues,
                delta_ef      = EXCLUDED.delta_ef,
                tasa_aprendizaje = EXCLUDED.tasa_aprendizaje
        """), {
            "fecha": fecha, "hora": hora,
            "real": animal_real,
            "p1": pred[0], "p2": pred[1], "p3": pred[2],
            "ac1": acierto_top1, "ac3": acierto_top3,
            "señal": "markov" if pm_ == max(pd_, pm_, pg_, pr_) else "decay",
            "pesos": str(resultado["pesos_ajustados"]).replace("'", '"'),
            "delta": round(nueva_ef - ef_, 4),
            "tasa": tasa_efectiva,
            "gen": gen_
        })

        # ── 9. Actualizar rentabilidad_hora ───────────────────────────────────
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
                ) >= 10.0
        """), {"hora": hora, "ac1": 1 if acierto_top1 else 0, "ac3": 1 if acierto_top3 else 0})

        await db.commit()

        logger.info(
            f"✅ Aprendizaje {fecha} {hora}: real={animal_real} "
            f"top1={acierto_top1} top3={acierto_top3} "
            f"λ={lambda_nuevo} pesos={resultado['pesos_ajustados']}"
        )

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error en aprender_tras_sorteo {fecha} {hora}: {e}")
        resultado["error"] = str(e)

    return resultado


async def recalcular_pesos_con_historia(
    db: AsyncSession,
    hora: str,
    dias_lookback: int = 365
) -> dict:
    """
    Recalcula los pesos de una hora completa usando todos los datos históricos
    ponderados por antigüedad. Llamar 1 vez por semana (sábados desde scheduler).
    
    Aplica la estrategia de peso temporal:
    - Datos viejos: pesan poco
    - Datos recientes: pesan mucho más
    """
    try:
        fecha_desde = date.today() - timedelta(days=dias_lookback)

        rows = (await db.execute(text("""
            SELECT
                a.fecha,
                a.prediccion_1, a.prediccion_2, a.prediccion_3,
                a.resultado_real,
                CASE WHEN LOWER(TRIM(a.resultado_real)) = LOWER(TRIM(a.prediccion_1))
                     THEN 1 ELSE 0 END AS ac1,
                CASE WHEN LOWER(TRIM(a.resultado_real)) IN (
                         LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                         LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                         LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                     ) THEN 1 ELSE 0 END AS ac3
            FROM auditoria_ia a
            WHERE a.hora = :hora
              AND a.fecha >= :desde
              AND a.resultado_real NOT IN ('PENDIENTE', '')
              AND a.resultado_real IS NOT NULL
            ORDER BY a.fecha DESC
        """), {"hora": hora, "desde": fecha_desde})).fetchall()

        if not rows:
            return {"hora": hora, "status": "sin_datos"}

        # Calcular efectividad ponderada por peso temporal
        suma_pesos = 0.0
        suma_ac3_pond = 0.0
        suma_ac1_pond = 0.0

        for r in rows:
            try:
                f = r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0]))
            except Exception:
                continue
            w = _peso_temporal(f)
            suma_pesos += w
            suma_ac3_pond += r[6] * w
            suma_ac1_pond += r[5] * w

        ef_top3_pond = round((suma_ac3_pond / suma_pesos) * 100, 2) if suma_pesos > 0 else 0.0
        ef_top1_pond = round((suma_ac1_pond / suma_pesos) * 100, 2) if suma_pesos > 0 else 0.0

        # Calcular λ óptimo basado en volatilidad de los últimos 30 días
        recientes = [r for r in rows if _peso_temporal(
            r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0]))
        ) >= PESO_RECIENTE_90D]

        if len(recientes) >= 5:
            aciertos_rec = [r[6] for r in recientes]
            media = sum(aciertos_rec) / len(aciertos_rec)
            varianza = sum((x - media) ** 2 for x in aciertos_rec) / len(aciertos_rec)
            volatilidad = math.sqrt(varianza)
            lambda_opt = round(max(0.004, min(0.020, 0.004 + volatilidad * 0.08)), 4)
        else:
            lambda_opt = 0.008

        # Determinar señal dominante basada en datos recientes (últimos 30 días)
        # Comparar: ¿cuándo acertó, cuál señal predijo correcto?
        mk_count = (await db.execute(text(
            "SELECT COUNT(*) FROM markov_transiciones WHERE hora = :hora AND frecuencia >= 5"
        ), {"hora": hora})).scalar() or 0

        total_hist = len(rows)
        # Heurística de pesos óptimos basada en datos:
        # - Si Markov tiene muchas transiciones y alta efectividad reciente → más peso a Markov
        # - Si los datos son pocos → más peso a decay (frecuencia histórica)
        if mk_count > 200 and ef_top3_pond > 15:
            pw_d, pw_m, pw_g, pw_r = 0.20, 0.40, 0.20, 0.20
        elif mk_count > 100:
            pw_d, pw_m, pw_g, pw_r = 0.25, 0.35, 0.20, 0.20
        elif total_hist > 500:
            pw_d, pw_m, pw_g, pw_r = 0.35, 0.25, 0.20, 0.20
        else:
            pw_d, pw_m, pw_g, pw_r = 0.30, 0.25, 0.25, 0.20

        # Guardar pesos recalculados
        gen = (await db.execute(text(
            "SELECT COALESCE(MAX(generacion), 1) FROM motor_pesos_hora WHERE hora = :hora"
        ), {"hora": hora})).scalar() or 1

        await db.execute(text("""
            INSERT INTO motor_pesos_hora
                (hora, generacion, peso_decay, peso_markov, peso_gap, peso_reciente, efectividad, fecha)
            VALUES (:hora, :gen, :pd, :pm, :pg, :pr, :ef, NOW())
            ON CONFLICT (hora, generacion) DO UPDATE SET
                peso_decay    = EXCLUDED.peso_decay,
                peso_markov   = EXCLUDED.peso_markov,
                peso_gap      = EXCLUDED.peso_gap,
                peso_reciente = EXCLUDED.peso_reciente,
                efectividad   = EXCLUDED.efectividad,
                fecha         = NOW()
        """), {
            "hora": hora, "gen": gen,
            "pd": pw_d, "pm": pw_m, "pg": pw_g, "pr": pw_r,
            "ef": ef_top3_pond
        })

        await db.commit()

        return {
            "hora": hora,
            "status": "ok",
            "total_evaluados": total_hist,
            "ef_top3_ponderada": ef_top3_pond,
            "ef_top1_ponderada": ef_top1_pond,
            "lambda_optimo": lambda_opt,
            "pesos_nuevos": {"decay": pw_d, "markov": pw_m, "gap": pw_g, "reciente": pw_r},
            "markov_transiciones": mk_count,
        }

    except Exception as e:
        await db.rollback()
        logger.error(f"❌ recalcular_pesos_con_historia {hora}: {e}")
        return {"hora": hora, "status": "error", "error": str(e)}


async def recalcular_todos_los_pesos(db: AsyncSession) -> dict:
    """
    Recalcula pesos para todas las horas. Llamar cada sábado desde el scheduler.
    """
    horas = [
        "08:00 AM","09:00 AM","10:00 AM","11:00 AM",
        "12:00 PM","01:00 PM","02:00 PM","03:00 PM",
        "04:00 PM","05:00 PM","06:00 PM","07:00 PM"
    ]
    resultados = []
    for hora in horas:
        r = await recalcular_pesos_con_historia(db, hora)
        resultados.append(r)
        logger.info(f"  ✅ {hora}: ef={r.get('ef_top3_ponderada', 0)}% pesos={r.get('pesos_nuevos', {})}")

    ok = sum(1 for r in resultados if r.get("status") == "ok")
    return {"total": len(horas), "ok": ok, "resultados": resultados}
