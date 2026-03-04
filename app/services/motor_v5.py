"""
MOTOR V6 — LOTTOAI PRO (guardado como motor_v5.py)
NUEVO: 
  - backtest en chunks para evitar timeout de Render (30s)
  - función llenar_auditoria_retroactiva para los últimos N días
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
# SEÑAL 1 — DEUDA (peso 0.35) — FIX: LAG en CTE separado
# ══════════════════════════════════════════════════════
async def calcular_deuda(db, hora_str: str, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fecha_anterior
            FROM historico WHERE hora = :hora AND fecha < :hoy
        ),
        gaps AS (
            SELECT animalito, (fecha - fecha_anterior) AS gap_dias
            FROM apariciones WHERE fecha_anterior IS NOT NULL
        ),
        ciclos AS (
            SELECT animalito, AVG(gap_dias) AS ciclo_prom, COUNT(*) AS apariciones
            FROM gaps GROUP BY animalito HAVING COUNT(*) >= 3
        ),
        ultima_vez AS (
            SELECT animalito, MAX(fecha) AS ultima,
                :hoy - MAX(fecha) AS dias_ausente
            FROM historico WHERE hora = :hora AND fecha < :hoy GROUP BY animalito
        )
        SELECT u.animalito, u.dias_ausente,
            ROUND(c.ciclo_prom::numeric,1) AS ciclo_prom,
            ROUND((u.dias_ausente/NULLIF(c.ciclo_prom,0)*100)::numeric,1) AS pct_deuda,
            c.apariciones
        FROM ultima_vez u JOIN ciclos c ON u.animalito=c.animalito
        ORDER BY pct_deuda DESC
    """), {"hora": hora_str, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_d = max(float(r[3]) for r in rows) or 1
        for r in rows:
            d = float(r[3])
            score = min(d/max_d, 1.0)
            if d > 300: score = min(score*1.4, 1.0)
            elif d > 200: score = min(score*1.2, 1.0)
            resultado[r[0]] = {"score": round(score,4), "dias_ausente": int(r[1]),
                               "ciclo_prom": float(r[2]), "pct_deuda": d, "apariciones": int(r[4])}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 2 — FRECUENCIA RECIENTE 90d (peso 0.20)
# ══════════════════════════════════════════════════════
async def calcular_frecuencia_reciente(db, hora_str: str, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    fecha_90 = fecha_limite - timedelta(days=90)
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces FROM historico
        WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora": hora_str, "desde": fecha_90, "hasta": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        total = sum(r[1] for r in rows)
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score": r[1]/max_v, "veces": int(r[1]),
                               "pct": round(r[1]/total*100,1)}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 3 — PATRÓN DÍA SEMANA (peso 0.15)
# ══════════════════════════════════════════════════════
async def calcular_patron_dia(db, hora_str: str, dia_semana: int, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces FROM historico
        WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia AND fecha<:hoy
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora": hora_str, "dia": dia_semana, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        total = sum(r[1] for r in rows)
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score": r[1]/max_v, "veces": int(r[1]),
                               "pct": round(r[1]/total*100,1)}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 4 — ANTI-RACHA (peso 0.20)
# ══════════════════════════════════════════════════════
async def calcular_anti_racha(db, hora_str: str, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, :hoy - MAX(fecha) AS dias FROM historico
        WHERE hora=:hora AND fecha<:hoy GROUP BY animalito
    """), {"hora": hora_str, "hoy": fecha_limite})
    resultado = {}
    for r in res.fetchall():
        dias = int(r[1])
        score = 0.05 if dias<=3 else 0.25 if dias<=7 else 0.55 if dias<=14 else 0.80 if dias<=30 else 1.0
        resultado[r[0]] = {"score": score, "dias_desde_ultima": dias}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 5 — SECUENCIA MARKOV (peso 0.10)
# ══════════════════════════════════════════════════════
async def calcular_secuencia(db, fecha_limite: date = None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    res_u = await db.execute(text(
        "SELECT animalito FROM historico WHERE fecha<:hoy ORDER BY fecha DESC, hora DESC LIMIT 1"
    ), {"hoy": fecha_limite})
    ultimo = res_u.scalar()
    if not ultimo:
        return {}
    res = await db.execute(text("""
        WITH seq AS (
            SELECT animalito, LEAD(animalito) OVER (ORDER BY fecha, hora) AS siguiente
            FROM historico WHERE fecha<:hoy
        )
        SELECT siguiente, COUNT(*) AS veces FROM seq
        WHERE animalito=:ultimo AND siguiente IS NOT NULL
        GROUP BY siguiente ORDER BY veces DESC LIMIT 15
    """), {"ultimo": ultimo, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score": r[1]/max_v, "veces": int(r[1])}
    return resultado


# ══════════════════════════════════════════════════════
# ÍNDICE DE CONFIANZA
# ══════════════════════════════════════════════════════
def calcular_indice_confianza(scores: dict) -> tuple:
    if not scores: return 0, "🔴 SIN DATOS"
    valores = sorted(scores.values(), reverse=True)
    if len(valores) < 3: return 20, "🔴 DATOS INSUFICIENTES"
    top1, top2 = valores[0], valores[1]
    promedio = sum(valores)/len(valores)
    separacion = top1-top2
    dominio = top1/promedio if promedio>0 else 1
    confianza = min(100, int((separacion*50)+(min(dominio-1,1)*35)+(min(top1,1)*15)))
    if confianza>=65: return confianza, "🟢 ALTA CONFIANZA — OPERAR"
    elif confianza>=40: return confianza, "🟡 MEDIA CONFIANZA — OPERAR CON CAUTELA"
    else: return confianza, "🔴 BAJA CONFIANZA — NO OPERAR"


# ══════════════════════════════════════════════════════
# COMBINAR SEÑALES — función reutilizable
# ══════════════════════════════════════════════════════
def combinar_señales(deuda, reciente, patron, anti, secuencia):
    PESOS = {"deuda":0.35,"reciente":0.20,"patron":0.15,"anti":0.20,"secuencia":0.10}
    todos = set(list(deuda)+list(reciente)+list(patron)+list(anti)+list(secuencia))
    scores = {}
    for animal in todos:
        scores[animal] = (
            deuda.get(animal,{}).get("score",0)*PESOS["deuda"] +
            reciente.get(animal,{}).get("score",0)*PESOS["reciente"] +
            patron.get(animal,{}).get("score",0)*PESOS["patron"] +
            anti.get(animal,{}).get("score",0.5)*PESOS["anti"] +
            secuencia.get(animal,{}).get("score",0)*PESOS["secuencia"]
        )
    return scores


# ══════════════════════════════════════════════════════
# PREDICCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════
async def generar_prediccion(db) -> dict:
    try:
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_str = ahora.strftime("%I:00 %p").upper()
        dia_semana = ahora.weekday()
        hoy = ahora.date()

        deuda    = await calcular_deuda(db, hora_str)
        reciente = await calcular_frecuencia_reciente(db, hora_str)
        patron   = await calcular_patron_dia(db, hora_str, dia_semana)
        anti     = await calcular_anti_racha(db, hora_str)
        secuencia= await calcular_secuencia(db)

        scores = combinar_señales(deuda, reciente, patron, anti, secuencia)
        confianza_idx, señal_texto = calcular_indice_confianza(scores)
        ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total_scores = sum(scores.values()) or 1

        top3 = []
        for animal, score in ranking[:3]:
            nombre = re.sub(r'[^a-z]','', animal.lower())
            num = NUMERO_POR_ANIMAL.get(nombre,"--")
            pct = round(score/total_scores*100,1)
            info_d = deuda.get(animal,{})
            top3.append({
                "numero": num, "animal": nombre.upper(),
                "imagen": f"{nombre}.png", "porcentaje": f"{pct}%",
                "score_raw": round(score,4),
                "dias_ausente": info_d.get("dias_ausente",0),
                "pct_deuda": info_d.get("pct_deuda",0),
            })

        res_u = await db.execute(text(
            "SELECT animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 1"))
        ultimo = res_u.scalar()

        if top3:
            try:
                await db.execute(text("""
                    INSERT INTO auditoria_ia (fecha,hora,animal_predicho,confianza_pct,resultado_real)
                    VALUES (:f,:h,:a,:c,'PENDIENTE')
                    ON CONFLICT (fecha,hora) DO UPDATE SET
                        animal_predicho=EXCLUDED.animal_predicho,
                        confianza_pct=EXCLUDED.confianza_pct
                """), {"f":hoy,"h":hora_str,"a":top3[0]["animal"].lower(),"c":float(confianza_idx)})
                await db.commit()
            except Exception:
                await db.rollback()

        return {
            "top3": top3, "hora": hora_str,
            "ultimo_resultado": ultimo or "N/A",
            "confianza_idx": confianza_idx, "señal_texto": señal_texto,
            "analisis": f"Motor V6 | {hora_str} | Confianza: {confianza_idx}/100 | {señal_texto}"
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"top3":[],"analisis":f"Error V6: {e}","confianza_idx":0,"señal_texto":"ERROR"}


"""
FUNCIÓN llenar_auditoria_retroactiva MEJORADA
Procesa en lotes de 7 días para evitar timeout de Render.
Permite cubrir desde 2018 hasta hoy en múltiples llamadas.

USO:
  /retroactivo?desde=2025-01-01&hasta=2025-06-30
  /retroactivo?desde=2024-01-01&hasta=2024-12-31
  /retroactivo?desde=2023-01-01&hasta=2023-12-31
  etc.

El parámetro max_por_lote controla cuántos sorteos por llamada (default 84 = 7 días × 12 horas).
"""

# ══════════════════════════════════════════════════════
# REEMPLAZA la función llenar_auditoria_retroactiva
# en app/services/motor_v5.py
# ══════════════════════════════════════════════════════

async def llenar_auditoria_retroactiva(db, fecha_desde=None, fecha_hasta=None, dias: int = 30) -> dict:
    """
    Genera predicciones retroactivas para un rango de fechas.
    Sin data leakage: cada predicción usa solo datos anteriores a esa fecha.
    
    Parámetros:
    - fecha_desde: date — inicio del rango (default: hoy - dias)
    - fecha_hasta: date — fin del rango (default: ayer)
    - dias: int — alternativa si no se especifican fechas
    """
    from datetime import date, timedelta

    try:
        hoy = date.today()
        if fecha_desde is None:
            fecha_desde = hoy - timedelta(days=dias)
        if fecha_hasta is None:
            fecha_hasta = hoy - timedelta(days=1)

        # Validar rango
        total_dias = (fecha_hasta - fecha_desde).days
        if total_dias > 365:
            return {"status": "error", "message": "Rango máximo 1 año por llamada"}

        from sqlalchemy import text
        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int AS dia_semana
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta
            ORDER BY fecha ASC, hora ASC
        """), {"desde": fecha_desde, "hasta": fecha_hasta})
        sorteos = res.fetchall()

        if not sorteos:
            return {"status": "ok", "procesados": 0, 
                    "message": f"Sin sorteos entre {fecha_desde} y {fecha_hasta}"}

        insertados = 0
        omitidos = 0  # Ya existían en auditoría
        aciertos = 0
        errores = 0

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s = sorteo
            dia_s = int(dia_s)

            try:
                # Verificar si ya existe en auditoría para no duplicar
                res_existe = await db.execute(text("""
                    SELECT 1 FROM auditoria_ia 
                    WHERE fecha=:f AND hora=:h AND acierto IS NOT NULL
                    LIMIT 1
                """), {"f": fecha_s, "h": hora_s})
                if res_existe.fetchone():
                    omitidos += 1
                    continue

                # Calcular señales SIN ver el futuro (fecha_limite = fecha del sorteo)
                d = await calcular_deuda(db, hora_s, fecha_s)
                r = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                p = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                a = await calcular_anti_racha(db, hora_s, fecha_s)
                s = await calcular_secuencia(db, fecha_s)

                scores = combinar_señales(d, r, p, a, s)
                if not scores:
                    errores += 1
                    continue

                confianza_idx, _ = calcular_indice_confianza(scores)
                predicho = max(scores, key=scores.get)
                acerto = predicho.lower() == real.lower()

                await db.execute(text("""
                    INSERT INTO auditoria_ia
                        (fecha, hora, animal_predicho, confianza_pct, resultado_real, acierto)
                    VALUES (:f, :h, :a, :c, :r, :ac)
                    ON CONFLICT (fecha, hora) DO UPDATE SET
                        animal_predicho = EXCLUDED.animal_predicho,
                        confianza_pct = EXCLUDED.confianza_pct,
                        resultado_real = EXCLUDED.resultado_real,
                        acierto = EXCLUDED.acierto
                """), {
                    "f": fecha_s, "h": hora_s,
                    "a": predicho.lower(),
                    "c": float(confianza_idx),
                    "r": real.lower(),
                    "ac": acerto
                })
                insertados += 1
                if acerto:
                    aciertos += 1

            except Exception as e:
                errores += 1
                continue

        await db.commit()

        efectividad = round(aciertos / insertados * 100, 1) if insertados > 0 else 0
        
        return {
            "status": "success",
            "fecha_desde": str(fecha_desde),
            "fecha_hasta": str(fecha_hasta),
            "total_sorteos_en_rango": len(sorteos),
            "procesados": insertados,
            "omitidos_ya_existian": omitidos,
            "aciertos": aciertos,
            "errores": errores,
            "efectividad": efectividad,
            "message": f"✅ {insertados} predicciones. Efectividad: {efectividad}% ({aciertos}/{insertados}). Omitidos: {omitidos}"
        }

    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}

# ══════════════════════════════════════════════════════
# BACKTEST — versión con límite de sorteos para evitar timeout
# Render tiene timeout de 30s. Máx ~100 sorteos seguros.
# ══════════════════════════════════════════════════════
async def backtest(db, fecha_desde: date, fecha_hasta: date, max_sorteos: int = 100) -> dict:
    try:
        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int AS dia_semana
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta
            ORDER BY fecha DESC, hora DESC
            LIMIT :lim
        """), {"desde": fecha_desde, "hasta": fecha_hasta, "lim": max_sorteos})
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
            sc = combinar_señales(d, r, p, a, s)

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
                "fecha": str(fecha_s), "hora": hora_s,
                "predicho": predicho, "real": real,
                "acierto": acerto, "confianza": confianza_idx,
                "alta_confianza": alta_conf,
                "pct_deuda": round(d.get(predicho,{}).get("pct_deuda",0),1)
            })

        ef_global = round(aciertos/total*100,1) if total>0 else 0
        ef_alta = round(alta_conf_aciertos/alta_conf_total*100,1) if alta_conf_total>0 else 0

        return {
            "fecha_desde": str(fecha_desde),
            "fecha_hasta": str(fecha_hasta),
            "total_sorteos": total,
            "aciertos_global": aciertos,
            "efectividad_global": ef_global,
            "alta_confianza_total": alta_conf_total,
            "alta_confianza_aciertos": alta_conf_aciertos,
            "efectividad_alta_confianza": ef_alta,
            "nota": f"Muestra de {total} sorteos (máx {max_sorteos} para evitar timeout)",
            "mensaje": f"Alta confianza: {ef_alta}% ({alta_conf_aciertos}/{alta_conf_total}) | Global: {ef_global}% ({aciertos}/{total})",
            "detalle": detalle
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"error": str(e)}


# ══════════════════════════════════════════════════════
# ENTRENAR
# ══════════════════════════════════════════════════════
async def entrenar_modelo(db) -> dict:
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
                        WHEN hora LIKE '%PM' THEN CAST(SPLIT_PART(hora,':',1) AS INT)+12
                        ELSE CAST(SPLIT_PART(hora,':',1) AS INT)
                    END AS hora_int,
                    animalito, COUNT(*) AS total_hist
                FROM historico GROUP BY 1,2
            ),
            reciente AS (
                SELECT
                    CASE
                        WHEN hora LIKE '12:%AM' THEN 0
                        WHEN hora LIKE '12:%PM' THEN 12
                        WHEN hora LIKE '%PM' THEN CAST(SPLIT_PART(hora,':',1) AS INT)+12
                        ELSE CAST(SPLIT_PART(hora,':',1) AS INT)
                    END AS hora_int,
                    animalito, COUNT(*) AS total_rec
                FROM historico
                WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY 1,2
            ),
            totales AS (
                SELECT hora_int, SUM(total_hist) AS gran_total FROM base GROUP BY hora_int
            )
            SELECT b.hora_int, b.animalito, b.total_hist,
                ROUND((b.total_hist::FLOAT/NULLIF(t.gran_total,0)*100)::numeric,2),
                CASE WHEN COALESCE(r.total_rec,0)>=2 THEN 'CALIENTE' ELSE 'FRIO' END,
                NOW()
            FROM base b
            JOIN totales t ON b.hora_int=t.hora_int
            LEFT JOIN reciente r ON b.hora_int=r.hora_int AND b.animalito=r.animalito
            WHERE b.hora_int BETWEEN 7 AND 19
        """))

        await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto=(LOWER(TRIM(a.animal_predicho))=LOWER(TRIM(h.animalito))),
                resultado_real=h.animalito
            FROM historico h
            WHERE a.fecha=h.fecha AND a.hora=h.hora
              AND (a.acierto IS NULL OR a.resultado_real='PENDIENTE')
        """))

        await db.execute(text("""
            UPDATE metricas SET
                total=(SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL),
                aciertos=(SELECT COUNT(*) FROM auditoria_ia WHERE acierto=TRUE),
                errores=(SELECT COUNT(*) FROM auditoria_ia WHERE acierto=FALSE),
                precision=(
                    SELECT CASE WHEN COUNT(*)=0 THEN 0
                        ELSE ROUND((COUNT(CASE WHEN acierto=TRUE THEN 1 END)::FLOAT/COUNT(*)*100)::numeric,1)
                    END FROM auditoria_ia WHERE acierto IS NOT NULL
                ),
                fecha=NOW()
            WHERE id=1
        """))

        res_hist = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_hist = res_hist.scalar() or 0
        res_cal = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL"))
        calibradas = res_cal.scalar() or 0
        res_ac = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto=TRUE"))
        ac = res_ac.scalar() or 0
        efectividad = round(ac/calibradas*100,1) if calibradas>0 else 0
        await db.commit()

        return {
            "status":"success",
            "message": f"✅ Motor V6 entrenado. {total_hist:,} registros. Efectividad: {efectividad}% ({ac}/{calibradas}).",
            "registros_analizados": total_hist,
            "efectividad": efectividad, "calibradas": calibradas, "aciertos": ac
        }
    except Exception as e:
        await db.rollback()
        return {"status":"error","message":str(e)}


# ══════════════════════════════════════════════════════
# CALIBRAR
# ══════════════════════════════════════════════════════
async def calibrar_predicciones(db) -> dict:
    try:
        result = await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto=(LOWER(TRIM(a.animal_predicho))=LOWER(TRIM(h.animalito))),
                resultado_real=h.animalito
            FROM historico h
            WHERE a.fecha=h.fecha AND a.hora=h.hora
              AND (a.acierto IS NULL OR a.resultado_real='PENDIENTE')
        """))
        calibradas = result.rowcount
        await db.commit()
        return {"calibradas": calibradas}
    except Exception as e:
        await db.rollback()
        return {"calibradas":0,"error":str(e)}


# ══════════════════════════════════════════════════════
# BITÁCORA
# ══════════════════════════════════════════════════════
async def obtener_bitacora(db) -> list:
    try:
        res = await db.execute(text("""
            SELECT a.hora, a.animal_predicho,
                COALESCE(a.resultado_real,'PENDIENTE') AS resultado_real,
                a.acierto, a.confianza_pct
            FROM auditoria_ia a
            WHERE a.fecha=CURRENT_DATE ORDER BY a.hora DESC LIMIT 13
        """))
        bitacora = []
        for r in res.fetchall():
            pred = re.sub(r'[^a-z]','',(r[1] or '').lower())
            real = re.sub(r'[^a-z]','',(r[2] or '').lower())
            bitacora.append({
                "hora": r[0],
                "animal_predicho": pred.upper() if pred else "PENDIENTE",
                "resultado_real": real.upper() if real and real!='pendiente' else "PENDIENTE",
                "acierto": r[3],
                "img_predicho": f"{pred}.png" if pred else "pendiente.png",
                "img_real": f"{real}.png" if real and real!='pendiente' else "pendiente.png",
                "confianza": int(round(float(r[4] or 0)))
            })
        return bitacora
    except Exception:
        return []


# ══════════════════════════════════════════════════════
# ESTADÍSTICAS
# ══════════════════════════════════════════════════════
async def obtener_estadisticas(db) -> dict:
    try:
        res_ef = await db.execute(text("""
            SELECT COUNT(*) AS total,
                COUNT(CASE WHEN acierto=TRUE THEN 1 END) AS aciertos,
                ROUND(
                    COUNT(CASE WHEN acierto=TRUE THEN 1 END)::NUMERIC/
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)*100
                ,1) AS precision
            FROM auditoria_ia
        """))
        ef = res_ef.fetchone()
        res_hoy = await db.execute(text("""
            SELECT COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END)
            FROM auditoria_ia WHERE fecha=CURRENT_DATE
        """))
        hoy = res_hoy.fetchone()
        res_total = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_hist = res_total.scalar() or 0
        res_top = await db.execute(text("""
            SELECT animalito, COUNT(*) AS veces FROM historico
            WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY animalito ORDER BY veces DESC LIMIT 5
        """))
        top_animales = [{"animal":r[0],"veces":r[1]} for r in res_top.fetchall()]
        return {
            "efectividad_global": float(ef[2] or 0),
            "total_auditado": int(ef[0] or 0),
            "aciertos_total": int(ef[1] or 0),
            "aciertos_hoy": int(hoy[0] or 0),
            "sorteos_hoy": int(hoy[1] or 0),
            "top_animales": top_animales,
            "total_historico": total_hist
        }
    except Exception:
        return {"efectividad_global":0,"aciertos_hoy":0,"sorteos_hoy":0,"total_historico":0,"top_animales":[]}
