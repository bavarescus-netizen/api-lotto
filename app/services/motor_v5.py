"""
MOTOR V6 — LOTTOAI PRO
Reemplaza motor_v5.py directamente (mismo nombre de funciones).
CORRECCIONES:
  1. calcular_deuda: LAG en subconsulta separada antes de AVG (GroupingError fix)
  2. entrenar_modelo: convierte hora texto -> integer para tabla probabilidades_hora
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
import pytz
import re

MAPA_ANIMALES = {
    "0": "delfin", "00": "ballena", "1": "carnero", "2": "toro",
    "3": "ciempies", "4": "alacran", "5": "leon", "6": "rana",
    "7": "perico", "8": "raton", "9": "aguila", "10": "tigre",
    "11": "gato", "12": "caballo", "13": "mono", "14": "paloma",
    "15": "zorro", "16": "oso", "17": "pavo", "18": "burro",
    "19": "chivo", "20": "cochino", "21": "gallo", "22": "camello",
    "23": "cebra", "24": "iguana", "25": "gallina", "26": "vaca",
    "27": "perro", "28": "zamuro", "29": "elefante", "30": "caiman",
    "31": "lapa", "32": "ardilla", "33": "pescado", "34": "venado",
    "35": "jirafa", "36": "culebra"
}
NUMERO_POR_ANIMAL = {v: k for k, v in MAPA_ANIMALES.items()}


# ══════════════════════════════════════════════════════
# SEÑAL 1 — ÍNDICE DE DEUDA (peso 0.35)
# FIX: LAG en CTE separado, luego AVG en otro CTE
# PostgreSQL no permite AVG(LAG()) en la misma query
# ══════════════════════════════════════════════════════
async def calcular_deuda(db: AsyncSession, hora_str: str, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()

    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fecha_anterior
            FROM historico
            WHERE hora = :hora AND fecha < :hoy
        ),
        gaps AS (
            SELECT animalito,
                (fecha - fecha_anterior) AS gap_dias
            FROM apariciones
            WHERE fecha_anterior IS NOT NULL
        ),
        ciclos AS (
            SELECT animalito,
                AVG(gap_dias) AS ciclo_prom,
                COUNT(*) AS apariciones
            FROM gaps
            GROUP BY animalito
            HAVING COUNT(*) >= 3
        ),
        ultima_vez AS (
            SELECT animalito,
                MAX(fecha) AS ultima,
                :hoy - MAX(fecha) AS dias_ausente
            FROM historico
            WHERE hora = :hora AND fecha < :hoy
            GROUP BY animalito
        )
        SELECT u.animalito,
            u.dias_ausente,
            ROUND(c.ciclo_prom::numeric, 1) AS ciclo_prom,
            ROUND((u.dias_ausente / NULLIF(c.ciclo_prom, 0) * 100)::numeric, 1) AS pct_deuda,
            c.apariciones
        FROM ultima_vez u
        JOIN ciclos c ON u.animalito = c.animalito
        ORDER BY pct_deuda DESC
    """), {"hora": hora_str, "hoy": fecha_limite})

    rows = res.fetchall()
    resultado = {}
    if rows:
        max_deuda = max(float(r[3]) for r in rows) or 1
        for r in rows:
            deuda_pct = float(r[3])
            score = min(deuda_pct / max_deuda, 1.0)
            if deuda_pct > 300:
                score = min(score * 1.4, 1.0)
            elif deuda_pct > 200:
                score = min(score * 1.2, 1.0)
            resultado[r[0]] = {
                "score": round(score, 4),
                "dias_ausente": int(r[1]),
                "ciclo_prom": float(r[2]),
                "pct_deuda": deuda_pct,
                "apariciones": int(r[4])
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 2 — FRECUENCIA RECIENTE 90 días (peso 0.20)
# ══════════════════════════════════════════════════════
async def calcular_frecuencia_reciente(db: AsyncSession, hora_str: str, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    fecha_90 = fecha_limite - timedelta(days=90)

    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces
        FROM historico
        WHERE hora = :hora AND fecha >= :desde AND fecha < :hasta
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora": hora_str, "desde": fecha_90, "hasta": fecha_limite})

    rows = res.fetchall()
    resultado = {}
    if rows:
        total = sum(r[1] for r in rows)
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {
                "score": r[1] / max_v,
                "veces": int(r[1]),
                "pct": round(r[1] / total * 100, 1)
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 3 — PATRÓN DÍA SEMANA + HORA (peso 0.15)
# ══════════════════════════════════════════════════════
async def calcular_patron_dia(db: AsyncSession, hora_str: str, dia_semana: int, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()

    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces
        FROM historico
        WHERE hora = :hora
          AND EXTRACT(DOW FROM fecha) = :dia
          AND fecha < :hoy
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora": hora_str, "dia": dia_semana, "hoy": fecha_limite})

    rows = res.fetchall()
    resultado = {}
    if rows:
        total = sum(r[1] for r in rows)
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {
                "score": r[1] / max_v,
                "veces": int(r[1]),
                "pct": round(r[1] / total * 100, 1)
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 4 — ANTI-RACHA (peso 0.20)
# ══════════════════════════════════════════════════════
async def calcular_anti_racha(db: AsyncSession, hora_str: str, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()

    res = await db.execute(text("""
        SELECT animalito, MAX(fecha) AS ultima, :hoy - MAX(fecha) AS dias
        FROM historico
        WHERE hora = :hora AND fecha < :hoy
        GROUP BY animalito
    """), {"hora": hora_str, "hoy": fecha_limite})

    rows = res.fetchall()
    resultado = {}
    for r in rows:
        dias = int(r[2])
        if dias <= 3:
            score = 0.05
        elif dias <= 7:
            score = 0.25
        elif dias <= 14:
            score = 0.55
        elif dias <= 30:
            score = 0.80
        else:
            score = 1.0
        resultado[r[0]] = {"score": score, "dias_desde_ultima": dias}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 5 — SECUENCIA (peso 0.10)
# ══════════════════════════════════════════════════════
async def calcular_secuencia(db: AsyncSession, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()

    res_ultimo = await db.execute(text("""
        SELECT animalito FROM historico
        WHERE fecha < :hoy
        ORDER BY fecha DESC, hora DESC LIMIT 1
    """), {"hoy": fecha_limite})
    ultimo = res_ultimo.scalar()
    if not ultimo:
        return {}

    res = await db.execute(text("""
        WITH seq AS (
            SELECT animalito,
                LEAD(animalito) OVER (ORDER BY fecha, hora) AS siguiente
            FROM historico WHERE fecha < :hoy
        )
        SELECT siguiente, COUNT(*) AS veces
        FROM seq
        WHERE animalito = :ultimo AND siguiente IS NOT NULL
        GROUP BY siguiente ORDER BY veces DESC LIMIT 15
    """), {"ultimo": ultimo, "hoy": fecha_limite})

    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score": r[1] / max_v, "veces": int(r[1])}
    return resultado


# ══════════════════════════════════════════════════════
# ÍNDICE DE CONFIANZA 0-100
# ══════════════════════════════════════════════════════
def calcular_indice_confianza(scores: dict) -> tuple:
    if not scores:
        return 0, "🔴 SIN DATOS"
    valores = sorted(scores.values(), reverse=True)
    if len(valores) < 3:
        return 20, "🔴 DATOS INSUFICIENTES"

    top1, top2 = valores[0], valores[1]
    promedio = sum(valores) / len(valores)
    separacion = top1 - top2
    dominio = top1 / promedio if promedio > 0 else 1

    confianza = min(100, int(
        (separacion * 50) +
        (min(dominio - 1, 1) * 35) +
        (min(top1, 1) * 15)
    ))

    if confianza >= 65:
        return confianza, "🟢 ALTA CONFIANZA — OPERAR"
    elif confianza >= 40:
        return confianza, "🟡 MEDIA CONFIANZA — OPERAR CON CAUTELA"
    else:
        return confianza, "🔴 BAJA CONFIANZA — NO OPERAR"


# ══════════════════════════════════════════════════════
# PREDICCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════
async def generar_prediccion(db: AsyncSession) -> dict:
    try:
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_str = ahora.strftime("%I:00 %p").upper()  # "08:00 AM"
        dia_semana = ahora.weekday()
        hoy = ahora.date()

        PESOS = {
            "deuda":     0.35,
            "reciente":  0.20,
            "patron":    0.15,
            "anti":      0.20,
            "secuencia": 0.10,
        }

        deuda     = await calcular_deuda(db, hora_str)
        reciente  = await calcular_frecuencia_reciente(db, hora_str)
        patron    = await calcular_patron_dia(db, hora_str, dia_semana)
        anti      = await calcular_anti_racha(db, hora_str)
        secuencia = await calcular_secuencia(db)

        todos = set(list(deuda) + list(reciente) + list(patron) + list(anti) + list(secuencia))
        scores = {}
        detalle = {}

        for animal in todos:
            s1 = deuda.get(animal, {}).get("score", 0)
            s2 = reciente.get(animal, {}).get("score", 0)
            s3 = patron.get(animal, {}).get("score", 0)
            s4 = anti.get(animal, {}).get("score", 0.5)
            s5 = secuencia.get(animal, {}).get("score", 0)

            total = (s1*PESOS["deuda"] + s2*PESOS["reciente"] +
                     s3*PESOS["patron"] + s4*PESOS["anti"] +
                     s5*PESOS["secuencia"])
            scores[animal] = total
            detalle[animal] = {
                "deuda": round(s1, 3), "reciente": round(s2, 3),
                "patron": round(s3, 3), "anti_racha": round(s4, 3),
                "secuencia": round(s5, 3), "total": round(total, 4)
            }

        confianza_idx, señal_texto = calcular_indice_confianza(scores)
        ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total_scores = sum(scores.values()) or 1

        top3 = []
        for animal, score in ranking[:3]:
            nombre = re.sub(r'[^a-z]', '', animal.lower())
            num = NUMERO_POR_ANIMAL.get(nombre, "--")
            pct = round(score / total_scores * 100, 1)
            info_d = deuda.get(animal, {})
            top3.append({
                "numero": num,
                "animal": nombre.upper(),
                "imagen": f"{nombre}.png",
                "porcentaje": f"{pct}%",
                "score_raw": round(score, 4),
                "dias_ausente": info_d.get("dias_ausente", 0),
                "pct_deuda": info_d.get("pct_deuda", 0),
                "señales": detalle.get(animal, {})
            })

        res_ultimo = await db.execute(text(
            "SELECT animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 1"
        ))
        ultimo = res_ultimo.scalar()

        if top3:
            try:
                await db.execute(text("""
                    INSERT INTO auditoria_ia
                        (fecha, hora, animal_predicho, confianza_pct, resultado_real)
                    VALUES (:f, :h, :a, :c, 'PENDIENTE')
                    ON CONFLICT (fecha, hora) DO UPDATE SET
                        animal_predicho = EXCLUDED.animal_predicho,
                        confianza_pct = EXCLUDED.confianza_pct
                """), {
                    "f": hoy, "h": hora_str,
                    "a": top3[0]["animal"].lower(),
                    "c": float(confianza_idx)
                })
                await db.commit()
            except Exception:
                await db.rollback()

        return {
            "top3": top3,
            "hora": hora_str,
            "ultimo_resultado": ultimo or "N/A",
            "confianza_idx": confianza_idx,
            "señal_texto": señal_texto,
            "analisis": f"Motor V6 | {hora_str} | Confianza: {confianza_idx}/100 | {señal_texto}"
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        return {"top3": [], "analisis": f"Error V6: {e}", "confianza_idx": 0, "señal_texto": "ERROR"}


# ══════════════════════════════════════════════════════
# BACKTESTING sin data leakage
# ══════════════════════════════════════════════════════
async def backtest(db: AsyncSession, fecha_desde: date, fecha_hasta: date) -> dict:
    try:
        res = await db.execute(text("""
            SELECT fecha, hora, animalito,
                EXTRACT(DOW FROM fecha)::int AS dia_semana
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta
            ORDER BY fecha, hora
        """), {"desde": fecha_desde, "hasta": fecha_hasta})
        sorteos = res.fetchall()

        if not sorteos:
            return {"error": "Sin datos en ese rango"}

        aciertos = 0
        total = 0
        alta_conf_total = 0
        alta_conf_aciertos = 0
        detalle = []

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s = sorteo
            dia_s = int(dia_s)

            d = await calcular_deuda(db, hora_s, fecha_s)
            r = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
            p = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
            a = await calcular_anti_racha(db, hora_s, fecha_s)
            s = await calcular_secuencia(db, fecha_s)

            PESOS = {"deuda":0.35,"reciente":0.20,"patron":0.15,"anti":0.20,"secuencia":0.10}
            todos = set(list(d)+list(r)+list(p)+list(a)+list(s))
            sc = {}
            for animal in todos:
                sc[animal] = (
                    d.get(animal,{}).get("score",0)*PESOS["deuda"] +
                    r.get(animal,{}).get("score",0)*PESOS["reciente"] +
                    p.get(animal,{}).get("score",0)*PESOS["patron"] +
                    a.get(animal,{}).get("score",0.5)*PESOS["anti"] +
                    s.get(animal,{}).get("score",0)*PESOS["secuencia"]
                )

            if not sc:
                continue

            confianza_idx, _ = calcular_indice_confianza(sc)
            predicho = max(sc, key=sc.get)
            acerto = predicho.lower() == real.lower()
            alta_conf = confianza_idx >= 60

            total += 1
            if acerto: aciertos += 1
            if alta_conf:
                alta_conf_total += 1
                if acerto: alta_conf_aciertos += 1

            detalle.append({
                "fecha": str(fecha_s),
                "hora": hora_s,
                "predicho": predicho,
                "real": real,
                "acierto": acerto,
                "confianza": confianza_idx,
                "alta_confianza": alta_conf,
                "pct_deuda": round(d.get(predicho, {}).get("pct_deuda", 0), 1)
            })

        ef_global = round(aciertos/total*100, 1) if total > 0 else 0
        ef_alta = round(alta_conf_aciertos/alta_conf_total*100, 1) if alta_conf_total > 0 else 0

        return {
            "fecha_desde": str(fecha_desde),
            "fecha_hasta": str(fecha_hasta),
            "total_sorteos": total,
            "aciertos_global": aciertos,
            "efectividad_global": ef_global,
            "alta_confianza_total": alta_conf_total,
            "alta_confianza_aciertos": alta_conf_aciertos,
            "efectividad_alta_confianza": ef_alta,
            "mensaje": f"Alta confianza: {ef_alta}% ({alta_conf_aciertos}/{alta_conf_total}) | Global: {ef_global}% ({aciertos}/{total})",
            "detalle": detalle[-100:]
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"error": str(e)}


# ══════════════════════════════════════════════════════
# ENTRENAR
# FIX: probabilidades_hora.hora es INTEGER
# Convierte "08:00 AM" → 8, "12:00 PM" → 12, etc.
# ══════════════════════════════════════════════════════
async def entrenar_modelo(db: AsyncSession) -> dict:
    try:
        await db.execute(text("DELETE FROM probabilidades_hora"))

        await db.execute(text("""
            INSERT INTO probabilidades_hora
                (hora, animalito, frecuencia, probabilidad, tendencia, ultima_actualizacion)
            WITH base AS (
                SELECT
                    CASE
                        WHEN hora LIKE '12:%AM' THEN 0
                        WHEN hora LIKE '12:%PM' THEN 12
                        WHEN hora LIKE '%PM'
                            THEN CAST(SPLIT_PART(hora, ':', 1) AS INT) + 12
                        ELSE CAST(SPLIT_PART(hora, ':', 1) AS INT)
                    END AS hora_int,
                    animalito,
                    COUNT(*) AS total_hist
                FROM historico
                GROUP BY 1, 2
            ),
            reciente AS (
                SELECT
                    CASE
                        WHEN hora LIKE '12:%AM' THEN 0
                        WHEN hora LIKE '12:%PM' THEN 12
                        WHEN hora LIKE '%PM'
                            THEN CAST(SPLIT_PART(hora, ':', 1) AS INT) + 12
                        ELSE CAST(SPLIT_PART(hora, ':', 1) AS INT)
                    END AS hora_int,
                    animalito,
                    COUNT(*) AS total_rec
                FROM historico
                WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY 1, 2
            ),
            totales AS (
                SELECT hora_int, SUM(total_hist) AS gran_total
                FROM base GROUP BY hora_int
            )
            SELECT b.hora_int, b.animalito, b.total_hist,
                ROUND((b.total_hist::FLOAT / NULLIF(t.gran_total,0) * 100)::numeric, 2),
                CASE WHEN COALESCE(r.total_rec,0) >= 2 THEN 'CALIENTE' ELSE 'FRIO' END,
                NOW()
            FROM base b
            JOIN totales t ON b.hora_int = t.hora_int
            LEFT JOIN reciente r ON b.hora_int = r.hora_int AND b.animalito = r.animalito
            WHERE b.hora_int BETWEEN 7 AND 19
        """))

        await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE')
        """))

        await db.execute(text("""
            UPDATE metricas SET
                total    = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL),
                aciertos = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto = TRUE),
                errores  = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto = FALSE),
                precision = (
                    SELECT CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE ROUND((COUNT(CASE WHEN acierto=TRUE THEN 1 END)::FLOAT
                            / COUNT(*) * 100)::numeric, 1)
                    END FROM auditoria_ia WHERE acierto IS NOT NULL
                ),
                fecha = NOW()
            WHERE id = 1
        """))

        res_hist = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_hist = res_hist.scalar() or 0
        res_cal = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL"))
        calibradas = res_cal.scalar() or 0
        res_ac = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto = TRUE"))
        ac = res_ac.scalar() or 0
        efectividad = round(ac / calibradas * 100, 1) if calibradas > 0 else 0

        await db.commit()
        return {
            "status": "success",
            "message": (f"✅ Motor V6 entrenado. {total_hist:,} registros. "
                       f"Efectividad: {efectividad}% ({ac}/{calibradas})."),
            "registros_analizados": total_hist,
            "efectividad": efectividad,
            "calibradas": calibradas,
            "aciertos": ac
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# CALIBRAR
# ══════════════════════════════════════════════════════
async def calibrar_predicciones(db: AsyncSession) -> dict:
    try:
        result = await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE')
        """))
        calibradas = result.rowcount
        await db.commit()
        return {"calibradas": calibradas}
    except Exception as e:
        await db.rollback()
        return {"calibradas": 0, "error": str(e)}


# ══════════════════════════════════════════════════════
# BITÁCORA
# ══════════════════════════════════════════════════════
async def obtener_bitacora(db: AsyncSession) -> list:
    try:
        res = await db.execute(text("""
            SELECT a.hora, a.animal_predicho,
                COALESCE(a.resultado_real,'PENDIENTE') AS resultado_real,
                a.acierto, a.confianza_pct
            FROM auditoria_ia a
            WHERE a.fecha = CURRENT_DATE
            ORDER BY a.hora DESC LIMIT 13
        """))
        bitacora = []
        for r in res.fetchall():
            pred = re.sub(r'[^a-z]', '', (r[1] or '').lower())
            real = re.sub(r'[^a-z]', '', (r[2] or '').lower())
            bitacora.append({
                "hora": r[0],
                "animal_predicho": pred.upper() if pred else "PENDIENTE",
                "resultado_real": real.upper() if real and real != 'pendiente' else "PENDIENTE",
                "acierto": r[3],
                "img_predicho": f"{pred}.png" if pred else "pendiente.png",
                "img_real": f"{real}.png" if real and real != 'pendiente' else "pendiente.png",
                "confianza": int(round(float(r[4] or 0)))
            })
        return bitacora
    except Exception as e:
        return []


# ══════════════════════════════════════════════════════
# ESTADÍSTICAS
# ══════════════════════════════════════════════════════
async def obtener_estadisticas(db: AsyncSession) -> dict:
    try:
        res_ef = await db.execute(text("""
            SELECT COUNT(*) AS total,
                COUNT(CASE WHEN acierto=TRUE THEN 1 END) AS aciertos,
                ROUND(
                    (COUNT(CASE WHEN acierto=TRUE THEN 1 END)::NUMERIC /
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)) * 100
                , 1) AS precision
            FROM auditoria_ia
        """))
        ef = res_ef.fetchone()

        res_hoy = await db.execute(text("""
            SELECT COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END)
            FROM auditoria_ia WHERE fecha = CURRENT_DATE
        """))
        hoy = res_hoy.fetchone()

        res_total = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_hist = res_total.scalar() or 0

        res_top = await db.execute(text("""
            SELECT animalito, COUNT(*) AS veces
            FROM historico WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY animalito ORDER BY veces DESC LIMIT 5
        """))
        top_animales = [{"animal": r[0], "veces": r[1]} for r in res_top.fetchall()]

        return {
            "efectividad_global": float(ef[2] or 0),
            "total_auditado": int(ef[0] or 0),
            "aciertos_total": int(ef[1] or 0),
            "aciertos_hoy": int(hoy[0] or 0),
            "sorteos_hoy": int(hoy[1] or 0),
            "top_animales": top_animales,
            "total_historico": total_hist
        }
    except Exception as e:
        return {"efectividad_global": 0, "aciertos_hoy": 0,
                "sorteos_hoy": 0, "total_historico": 0, "top_animales": []}
