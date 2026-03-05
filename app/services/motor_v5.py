"""
MOTOR V8 — LOTTOAI PRO — APRENDIZAJE AUTOMÁTICO
================================================
Sistema de aprendizaje por refuerzo que:
1. Simula haber operado desde 2018 hasta hoy
2. En cada sorteo, ve el error y ajusta pesos automáticamente
3. Los pesos evolucionan solos según qué señal correlaciona con aciertos
4. Guarda pesos óptimos en BD tabla: motor_pesos
5. Notificaciones push antes del próximo sorteo

NUEVO: tabla motor_pesos en BD para persistir aprendizaje
CREATE TABLE IF NOT EXISTS motor_pesos (
    id SERIAL PRIMARY KEY,
    fecha TIMESTAMP DEFAULT NOW(),
    peso_reciente FLOAT DEFAULT 0.30,
    peso_deuda FLOAT DEFAULT 0.25,
    peso_anti FLOAT DEFAULT 0.25,
    peso_patron FLOAT DEFAULT 0.10,
    peso_secuencia FLOAT DEFAULT 0.10,
    efectividad FLOAT DEFAULT 0.0,
    total_evaluados INT DEFAULT 0,
    aciertos INT DEFAULT 0,
    generacion INT DEFAULT 1
);
INSERT INTO motor_pesos (peso_reciente,peso_deuda,peso_anti,peso_patron,peso_secuencia)
VALUES (0.30,0.25,0.25,0.10,0.10) ON CONFLICT DO NOTHING;
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
import pytz, re, random

MAPA_ANIMALES = {
    "0":"delfin","00":"ballena","1":"carnero","2":"toro","3":"ciempies",
    "4":"alacran","5":"leon","6":"rana","7":"perico","8":"raton","9":"aguila",
    "10":"tigre","11":"gato","12":"caballo","13":"mono","14":"paloma",
    "15":"zorro","16":"oso","17":"pavo","18":"burro","19":"chivo","20":"cochino",
    "21":"gallo","22":"camello","23":"cebra","24":"iguana","25":"gallina",
    "26":"vaca","27":"perro","28":"zamuro","29":"elefante","30":"caiman",
    "31":"lapa","32":"ardilla","33":"pescado","34":"venado","35":"jirafa",
    "36":"culebra"
}
NUMERO_POR_ANIMAL = {v: k for k, v in MAPA_ANIMALES.items()}

HORAS_PREMIUM = {
    "11:00 AM": 1.15, "12:00 PM": 1.10, "06:00 PM": 1.08,
    "03:00 PM": 1.05, "02:00 PM": 1.05,
}
HORAS_PENALIZAR = {
    "04:00 PM": 0.92, "05:00 PM": 0.92, "07:00 PM": 0.92,
}
ANIMALES_HORA_ESPECIALES = {
    "08:00 AM": {"jirafa": 1.20, "elefante": 1.15},
    "06:00 PM": {"jirafa": 1.10},
    "07:00 PM": {"cebra": 1.10},
    "01:00 PM": {"lapa": 1.08},
}


# ══════════════════════════════════════════════════════
# PESOS DINÁMICOS — se leen de la BD
# ══════════════════════════════════════════════════════
async def obtener_pesos_actuales(db) -> dict:
    """Lee los pesos más recientes de la BD (resultado del aprendizaje)"""
    try:
        res = await db.execute(text("""
            SELECT peso_reciente, peso_deuda, peso_anti, peso_patron, peso_secuencia
            FROM motor_pesos ORDER BY id DESC LIMIT 1
        """))
        row = res.fetchone()
        if row:
            return {
                "reciente":  float(row[0]),
                "deuda":     float(row[1]),
                "anti":      float(row[2]),
                "patron":    float(row[3]),
                "secuencia": float(row[4]),
            }
    except Exception:
        pass
    # Defaults V7 si no hay tabla
    return {"reciente":0.30,"deuda":0.25,"anti":0.25,"patron":0.10,"secuencia":0.10}


async def guardar_pesos(db, pesos: dict, efectividad: float, total: int, aciertos_n: int, generacion: int):
    """Persiste los nuevos pesos aprendidos"""
    try:
        await db.execute(text("""
            INSERT INTO motor_pesos 
                (peso_reciente,peso_deuda,peso_anti,peso_patron,peso_secuencia,
                 efectividad,total_evaluados,aciertos,generacion)
            VALUES (:r,:d,:a,:p,:s,:ef,:tot,:ac,:gen)
        """), {
            "r": pesos["reciente"], "d": pesos["deuda"], "a": pesos["anti"],
            "p": pesos["patron"], "s": pesos["secuencia"],
            "ef": efectividad, "tot": total, "ac": aciertos_n, "gen": generacion
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        print(f"Error guardando pesos: {e}")


# ══════════════════════════════════════════════════════
# SEÑALES (mismas que V7 pero usando pesos dinámicos)
# ══════════════════════════════════════════════════════
async def calcular_deuda(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa
            FROM historico WHERE hora=:hora AND fecha<:hoy
        ),
        gaps AS (SELECT animalito,(fecha-fa) AS gap FROM apariciones WHERE fa IS NOT NULL),
        ciclos AS (
            SELECT animalito, AVG(gap) AS ciclo FROM gaps
            GROUP BY animalito HAVING COUNT(*)>=3
        ),
        ultima AS (
            SELECT animalito, :hoy-MAX(fecha) AS dias
            FROM historico WHERE hora=:hora AND fecha<:hoy GROUP BY animalito
        )
        SELECT u.animalito, u.dias,
            ROUND(c.ciclo::numeric,1),
            ROUND((u.dias/NULLIF(c.ciclo,0)*100)::numeric,1)
        FROM ultima u JOIN ciclos c ON u.animalito=c.animalito ORDER BY 4 DESC
    """), {"hora": hora_str, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_d = max(float(r[3]) for r in rows) or 1
        for r in rows:
            d = float(r[3])
            score = min(d/max_d, 1.0)
            if d > 400: score = min(score*1.5, 1.0)
            elif d > 250: score = min(score*1.25, 1.0)
            resultado[r[0]] = {"score":round(score,4),"dias_ausente":int(r[1]),
                               "ciclo_prom":float(r[2]),"pct_deuda":d}
    return resultado


async def calcular_frecuencia_reciente(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    fecha_60 = fecha_limite - timedelta(days=60)
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces FROM historico
        WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora":hora_str,"desde":fecha_60,"hasta":fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        total = sum(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score":r[1]/max_v,"veces":int(r[1]),"pct":round(r[1]/total*100,1)}
    return resultado


async def calcular_patron_dia(db, hora_str, dia_semana, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces FROM historico
        WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia AND fecha<:hoy
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora":hora_str,"dia":dia_semana,"hoy":fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score":r[1]/max_v,"veces":int(r[1])}
    return resultado


async def calcular_anti_racha(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, :hoy-MAX(fecha) AS dias FROM historico
        WHERE hora=:hora AND fecha<:hoy GROUP BY animalito
    """), {"hora":hora_str,"hoy":fecha_limite})
    resultado = {}
    for r in res.fetchall():
        dias = int(r[1])
        if dias<=2: score=0.02
        elif dias<=5: score=0.15
        elif dias<=10: score=0.40
        elif dias<=20: score=0.65
        elif dias<=35: score=0.85
        else: score=1.00
        resultado[r[0]] = {"score":score,"dias_desde_ultima":dias}
    return resultado


async def calcular_secuencia(db, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res_u = await db.execute(text(
        "SELECT animalito FROM historico WHERE fecha<:hoy ORDER BY fecha DESC,hora DESC LIMIT 1"
    ), {"hoy":fecha_limite})
    ultimo = res_u.scalar()
    if not ultimo: return {}
    res = await db.execute(text("""
        WITH seq AS (
            SELECT animalito, LEAD(animalito) OVER (ORDER BY fecha,hora) AS siguiente
            FROM historico WHERE fecha<:hoy
        )
        SELECT siguiente, COUNT(*) AS veces FROM seq
        WHERE animalito=:ultimo AND siguiente IS NOT NULL
        GROUP BY siguiente ORDER BY veces DESC LIMIT 15
    """), {"ultimo":ultimo,"hoy":fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score":r[1]/max_v,"veces":int(r[1])}
    return resultado


def combinar_señales(deuda, reciente, patron, anti, secuencia, hora_str, pesos):
    todos = set(list(deuda)+list(reciente)+list(patron)+list(anti)+list(secuencia))
    hora_mult = HORAS_PREMIUM.get(hora_str, HORAS_PENALIZAR.get(hora_str, 1.0))
    especiales = ANIMALES_HORA_ESPECIALES.get(hora_str, {})
    scores = {}
    for animal in todos:
        base = (
            deuda.get(animal,{}).get("score",0)    * pesos["deuda"] +
            reciente.get(animal,{}).get("score",0) * pesos["reciente"] +
            patron.get(animal,{}).get("score",0)   * pesos["patron"] +
            anti.get(animal,{}).get("score",0.5)   * pesos["anti"] +
            secuencia.get(animal,{}).get("score",0)* pesos["secuencia"]
        )
        animal_mult = especiales.get(animal.lower(), 1.0)
        scores[animal] = base * hora_mult * animal_mult
    return scores


def calcular_indice_confianza(scores):
    if not scores: return 0, "🔴 SIN DATOS"
    valores = sorted(scores.values(), reverse=True)
    if len(valores)<3: return 20, "🔴 DATOS INSUFICIENTES"
    top1, top2 = valores[0], valores[1]
    promedio = sum(valores)/len(valores)
    separacion = top1-top2
    dominio = top1/promedio if promedio>0 else 1
    confianza = min(100, int((separacion*50)+(min(dominio-1,1)*35)+(min(top1,1)*15)))
    if confianza>=65: return confianza, "🟢 ALTA CONFIANZA — OPERAR"
    elif confianza>=40: return confianza, "🟡 MEDIA CONFIANZA — OPERAR CON CAUTELA"
    else: return confianza, "🔴 BAJA CONFIANZA — NO OPERAR"


# ══════════════════════════════════════════════════════
# APRENDIZAJE POR REFUERZO — NÚCLEO DEL MOTOR V8
# Simula desde fecha_inicio hasta hoy, ajusta pesos
# en cada "generación" de 30 días
# ══════════════════════════════════════════════════════
async def aprender_desde_historico(db, fecha_inicio=None, dias_por_generacion=30) -> dict:
    """
    Algoritmo de aprendizaje:
    1. Parte de los pesos actuales
    2. Evalúa sobre ventana de 30 días
    3. Calcula correlación de cada señal con aciertos
    4. Ajusta pesos proporcionalmente
    5. Guarda nuevos pesos si mejoran efectividad
    6. Repite hasta hoy
    """
    try:
        hoy = date.today()
        if fecha_inicio is None:
            fecha_inicio = hoy - timedelta(days=365)  # Por defecto: último año

        # Obtener generación actual
        res_gen = await db.execute(text(
            "SELECT COALESCE(MAX(generacion),0) FROM motor_pesos"
        ))
        generacion_actual = (res_gen.scalar() or 0) + 1

        # Pesos de partida (los mejores encontrados hasta ahora)
        pesos = await obtener_pesos_actuales(db)

        mejor_efectividad = 0.0
        mejores_pesos = pesos.copy()
        total_global = 0
        aciertos_global = 0
        generaciones_completadas = 0
        log = []

        # Procesar por ventanas de 30 días
        fecha_ventana = fecha_inicio
        while fecha_ventana < hoy - timedelta(days=7):
            fecha_fin_ventana = min(fecha_ventana + timedelta(days=dias_por_generacion), hoy - timedelta(days=1))

            # Obtener sorteos de esta ventana
            res = await db.execute(text("""
                SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
                FROM historico
                WHERE fecha BETWEEN :desde AND :hasta
                ORDER BY fecha ASC, hora ASC
                LIMIT 500
            """), {"desde": fecha_ventana, "hasta": fecha_fin_ventana})
            sorteos = res.fetchall()

            if not sorteos:
                fecha_ventana += timedelta(days=dias_por_generacion)
                continue

            # Evaluar señales individualmente para medir correlación
            aciertos_por_señal = {"reciente":0,"deuda":0,"anti":0,"patron":0,"secuencia":0}
            total_ventana = 0
            aciertos_ventana = 0

            for sorteo in sorteos[:50]:  # Muestra de 50 para no hacer timeout
                fecha_s, hora_s, real, dia_s = sorteo
                dia_s = int(dia_s)
                try:
                    d = await calcular_deuda(db, hora_s, fecha_s)
                    r = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                    p = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                    a = await calcular_anti_racha(db, hora_s, fecha_s)
                    s = await calcular_secuencia(db, fecha_s)

                    # ¿Qué señal hubiera acertado sola?
                    if d and max(d, key=lambda x: d[x]["score"]).lower() == real.lower():
                        aciertos_por_señal["deuda"] += 1
                    if r and max(r, key=lambda x: r[x]["score"]).lower() == real.lower():
                        aciertos_por_señal["reciente"] += 1
                    if a and max(a, key=lambda x: a[x]["score"]).lower() == real.lower():
                        aciertos_por_señal["anti"] += 1
                    if p and max(p, key=lambda x: p[x]["score"]).lower() == real.lower():
                        aciertos_por_señal["patron"] += 1
                    if s and max(s, key=lambda x: s[x]["score"]).lower() == real.lower():
                        aciertos_por_señal["secuencia"] += 1

                    # Evaluar con pesos actuales
                    sc = combinar_señales(d, r, p, a, s, hora_s, pesos)
                    if sc:
                        predicho = max(sc, key=sc.get)
                        if predicho.lower() == real.lower():
                            aciertos_ventana += 1
                    total_ventana += 1

                except Exception:
                    continue

            if total_ventana == 0:
                fecha_ventana += timedelta(days=dias_por_generacion)
                continue

            ef_ventana = aciertos_ventana / total_ventana

            # AJUSTE DE PESOS basado en correlación de señales
            total_señal = sum(aciertos_por_señal.values()) or 1
            nuevos_pesos = {}
            for señal, aciertos_s in aciertos_por_señal.items():
                # Peso proporcional a efectividad de la señal + suavizado
                peso_señal = (aciertos_s / total_señal)
                peso_suavizado = 0.7 * pesos[señal] + 0.3 * peso_señal
                nuevos_pesos[señal] = peso_suavizado

            # Normalizar a suma = 1.0
            total_nuevo = sum(nuevos_pesos.values())
            nuevos_pesos = {k: round(v/total_nuevo, 4) for k, v in nuevos_pesos.items()}

            # Solo actualizar si mejora o es primera generación
            if ef_ventana >= mejor_efectividad or generaciones_completadas == 0:
                if ef_ventana > mejor_efectividad:
                    mejor_efectividad = ef_ventana
                    mejores_pesos = nuevos_pesos.copy()
                pesos = nuevos_pesos  # Avanzar con nuevos pesos

            total_global += total_ventana
            aciertos_global += aciertos_ventana
            generaciones_completadas += 1

            log.append({
                "ventana": f"{fecha_ventana} → {fecha_fin_ventana}",
                "sorteos": total_ventana,
                "efectividad": round(ef_ventana*100, 1),
                "mejor_señal": max(aciertos_por_señal, key=aciertos_por_señal.get),
                "pesos_nuevos": nuevos_pesos
            })

            fecha_ventana += timedelta(days=dias_por_generacion)

        # Guardar los mejores pesos encontrados
        ef_global = round(aciertos_global/total_global*100, 1) if total_global > 0 else 0
        await guardar_pesos(db, mejores_pesos, ef_global, total_global, aciertos_global, generacion_actual)

        return {
            "status": "success",
            "generacion": generacion_actual,
            "fecha_inicio": str(fecha_inicio),
            "fecha_fin": str(hoy),
            "generaciones_completadas": generaciones_completadas,
            "total_sorteos_evaluados": total_global,
            "aciertos_total": aciertos_global,
            "efectividad_global": ef_global,
            "mejores_pesos": mejores_pesos,
            "message": f"✅ Generación {generacion_actual} completada. Efectividad: {ef_global}% | Pesos: {mejores_pesos}",
            "log_ventanas": log[-5:]  # Últimas 5 ventanas para no saturar
        }

    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# PREDICCIÓN — usa pesos aprendidos de BD
# ══════════════════════════════════════════════════════
async def generar_prediccion(db) -> dict:
    try:
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_str = ahora.strftime("%I:00 %p").upper()
        dia_semana = ahora.weekday()
        hoy = ahora.date()

        pesos = await obtener_pesos_actuales(db)

        deuda    = await calcular_deuda(db, hora_str)
        reciente = await calcular_frecuencia_reciente(db, hora_str)
        patron   = await calcular_patron_dia(db, hora_str, dia_semana)
        anti     = await calcular_anti_racha(db, hora_str)
        secuencia= await calcular_secuencia(db)

        scores = combinar_señales(deuda, reciente, patron, anti, secuencia, hora_str, pesos)
        confianza_idx, señal_texto = calcular_indice_confianza(scores)
        ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total_scores = sum(scores.values()) or 1

        top3 = []
        for animal, score in ranking[:3]:
            nombre = re.sub(r'[^a-z]','', animal.lower())
            num = NUMERO_POR_ANIMAL.get(nombre,"--")
            pct = round(score/total_scores*100, 1)
            info_d = deuda.get(animal, {})
            top3.append({
                "numero": num, "animal": nombre.upper(),
                "imagen": f"{nombre}.png", "porcentaje": f"{pct}%",
                "score_raw": round(score, 4),
                "dias_ausente": info_d.get("dias_ausente", 0),
                "pct_deuda": info_d.get("pct_deuda", 0),
            })

        res_u = await db.execute(text(
            "SELECT animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 1"))
        ultimo = res_u.scalar()

        es_hora_premium = hora_str in HORAS_PREMIUM
        if es_hora_premium:
            señal_texto = "⭐ " + señal_texto

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

        # Próximo sorteo (para notificación)
        horas_sorteo = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
                        "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM","06:00 PM","07:00 PM"]
        idx_actual = horas_sorteo.index(hora_str) if hora_str in horas_sorteo else -1
        proxima_hora = horas_sorteo[idx_actual+1] if idx_actual < len(horas_sorteo)-1 else None

        return {
            "top3": top3, "hora": hora_str,
            "ultimo_resultado": ultimo or "N/A",
            "confianza_idx": confianza_idx,
            "señal_texto": señal_texto,
            "hora_premium": es_hora_premium,
            "proxima_hora": proxima_hora,
            "pesos_actuales": pesos,
            "analisis": f"Motor V8 | {hora_str} | Confianza: {confianza_idx}/100 | {señal_texto}"
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"top3":[],"analisis":f"Error V8: {e}","confianza_idx":0,"señal_texto":"ERROR"}


# ══════════════════════════════════════════════════════
# ENTRENAR — calibra + actualiza métricas
# ══════════════════════════════════════════════════════
async def entrenar_modelo(db) -> dict:
    try:
        # Calibrar predicciones pendientes contra resultados reales
        await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto=(LOWER(TRIM(a.animal_predicho))=LOWER(TRIM(h.animalito))),
                resultado_real=h.animalito
            FROM historico h WHERE a.fecha=h.fecha AND a.hora=h.hora
              AND (a.acierto IS NULL OR a.resultado_real='PENDIENTE')
        """))

        # Actualizar tabla probabilidades_hora
        await db.execute(text("DELETE FROM probabilidades_hora"))
        await db.execute(text("""
            INSERT INTO probabilidades_hora
                (hora,animalito,frecuencia,probabilidad,tendencia,ultima_actualizacion)
            WITH base AS (
                SELECT CASE
                    WHEN hora LIKE '12:%AM' THEN 0 WHEN hora LIKE '12:%PM' THEN 12
                    WHEN hora LIKE '%PM' THEN CAST(SPLIT_PART(hora,':',1) AS INT)+12
                    ELSE CAST(SPLIT_PART(hora,':',1) AS INT) END AS hora_int,
                    animalito, COUNT(*) AS total_hist FROM historico GROUP BY 1,2
            ),
            reciente AS (
                SELECT CASE
                    WHEN hora LIKE '12:%AM' THEN 0 WHEN hora LIKE '12:%PM' THEN 12
                    WHEN hora LIKE '%PM' THEN CAST(SPLIT_PART(hora,':',1) AS INT)+12
                    ELSE CAST(SPLIT_PART(hora,':',1) AS INT) END AS hora_int,
                    animalito, COUNT(*) AS total_rec FROM historico
                WHERE fecha>=CURRENT_DATE-INTERVAL '60 days' GROUP BY 1,2
            ),
            totales AS (SELECT hora_int, SUM(total_hist) AS gran_total FROM base GROUP BY hora_int)
            SELECT b.hora_int, b.animalito, b.total_hist,
                ROUND((b.total_hist::FLOAT/NULLIF(t.gran_total,0)*100)::numeric,2),
                CASE WHEN COALESCE(r.total_rec,0)>=2 THEN 'CALIENTE' ELSE 'FRIO' END, NOW()
            FROM base b JOIN totales t ON b.hora_int=t.hora_int
            LEFT JOIN reciente r ON b.hora_int=r.hora_int AND b.animalito=r.animalito
            WHERE b.hora_int BETWEEN 7 AND 19
        """))

        res_hist = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_hist = res_hist.scalar() or 0
        res_cal = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL"))
        cal = res_cal.scalar() or 0
        res_ac = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto=TRUE"))
        ac = res_ac.scalar() or 0
        ef = round(ac/cal*100,1) if cal>0 else 0
        await db.commit()

        return {
            "status": "success",
            "message": f"✅ Motor V8 calibrado. {total_hist:,} registros. Efectividad: {ef}% ({ac}/{cal}).",
            "registros_analizados": total_hist,
            "efectividad": ef, "calibradas": cal, "aciertos": ac
        }
    except Exception as e:
        await db.rollback()
        return {"status":"error","message":str(e)}


async def calibrar_predicciones(db) -> dict:
    try:
        result = await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto=(LOWER(TRIM(a.animal_predicho))=LOWER(TRIM(h.animalito))),
                resultado_real=h.animalito
            FROM historico h WHERE a.fecha=h.fecha AND a.hora=h.hora
              AND (a.acierto IS NULL OR a.resultado_real='PENDIENTE')
        """))
        cal = result.rowcount
        await db.commit()
        return {"calibradas": cal}
    except Exception as e:
        await db.rollback()
        return {"calibradas":0,"error":str(e)}


async def llenar_auditoria_retroactiva(db, fecha_desde=None, fecha_hasta=None, dias=30) -> dict:
    try:
        hoy = date.today()
        if fecha_desde is None: fecha_desde = hoy - timedelta(days=dias)
        if fecha_hasta is None: fecha_hasta = hoy - timedelta(days=1)
        if (fecha_hasta-fecha_desde).days > 366:
            return {"status":"error","message":"Rango máximo 1 año"}

        pesos = await obtener_pesos_actuales(db)

        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
            FROM historico WHERE fecha BETWEEN :desde AND :hasta ORDER BY fecha ASC, hora ASC
        """), {"desde":fecha_desde,"hasta":fecha_hasta})
        sorteos = res.fetchall()
        if not sorteos:
            return {"status":"ok","procesados":0,"message":f"Sin sorteos entre {fecha_desde} y {fecha_hasta}"}

        insertados=0; omitidos=0; aciertos=0

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s = sorteo
            dia_s = int(dia_s)
            try:
                res_e = await db.execute(text(
                    "SELECT 1 FROM auditoria_ia WHERE fecha=:f AND hora=:h AND acierto IS NOT NULL LIMIT 1"
                ), {"f":fecha_s,"h":hora_s})
                if res_e.fetchone():
                    omitidos+=1; continue

                d = await calcular_deuda(db,hora_s,fecha_s)
                r = await calcular_frecuencia_reciente(db,hora_s,fecha_s)
                p = await calcular_patron_dia(db,hora_s,dia_s,fecha_s)
                a = await calcular_anti_racha(db,hora_s,fecha_s)
                s = await calcular_secuencia(db,fecha_s)
                sc = combinar_señales(d,r,p,a,s,hora_s,pesos)
                if not sc: continue

                confianza_idx,_ = calcular_indice_confianza(sc)
                predicho = max(sc,key=sc.get)
                acerto = predicho.lower()==real.lower()

                await db.execute(text("""
                    INSERT INTO auditoria_ia (fecha,hora,animal_predicho,confianza_pct,resultado_real,acierto)
                    VALUES (:f,:h,:a,:c,:r,:ac)
                    ON CONFLICT (fecha,hora) DO UPDATE SET
                        animal_predicho=EXCLUDED.animal_predicho,
                        confianza_pct=EXCLUDED.confianza_pct,
                        resultado_real=EXCLUDED.resultado_real,
                        acierto=EXCLUDED.acierto
                """), {"f":fecha_s,"h":hora_s,"a":predicho.lower(),"c":float(confianza_idx),"r":real.lower(),"ac":acerto})
                insertados+=1
                if acerto: aciertos+=1
            except Exception:
                continue

        await db.commit()
        ef = round(aciertos/insertados*100,1) if insertados>0 else 0
        return {
            "status":"success","procesados":insertados,"omitidos_ya_existian":omitidos,
            "aciertos":aciertos,"efectividad":ef,
            "message":f"✅ {insertados} predicciones V8. Efectividad: {ef}% ({aciertos}/{insertados})"
        }
    except Exception as e:
        await db.rollback()
        return {"status":"error","message":str(e)}


async def backtest(db, fecha_desde, fecha_hasta, max_sorteos=100) -> dict:
    try:
        pesos = await obtener_pesos_actuales(db)
        res = await db.execute(text("""
            SELECT fecha,hora,animalito,EXTRACT(DOW FROM fecha)::int
            FROM historico WHERE fecha BETWEEN :desde AND :hasta
            ORDER BY fecha DESC,hora DESC LIMIT :lim
        """), {"desde":fecha_desde,"hasta":fecha_hasta,"lim":max_sorteos})
        sorteos = res.fetchall()
        if not sorteos: return {"error":"Sin datos en ese rango"}

        aciertos=0; total=0; alta_conf_total=0; alta_conf_aciertos=0; detalle=[]

        for sorteo in sorteos:
            fecha_s,hora_s,real,dia_s = sorteo
            dia_s = int(dia_s)
            d = await calcular_deuda(db,hora_s,fecha_s)
            r = await calcular_frecuencia_reciente(db,hora_s,fecha_s)
            p = await calcular_patron_dia(db,hora_s,dia_s,fecha_s)
            a = await calcular_anti_racha(db,hora_s,fecha_s)
            s = await calcular_secuencia(db,fecha_s)
            sc = combinar_señales(d,r,p,a,s,hora_s,pesos)
            if not sc: continue

            confianza_idx,_ = calcular_indice_confianza(sc)
            predicho = max(sc,key=sc.get)
            acerto = predicho.lower()==real.lower()
            alta_conf = confianza_idx>=60
            total+=1
            if acerto: aciertos+=1
            if alta_conf:
                alta_conf_total+=1
                if acerto: alta_conf_aciertos+=1
            detalle.append({
                "fecha":str(fecha_s),"hora":hora_s,"predicho":predicho,"real":real,
                "acierto":acerto,"confianza":confianza_idx,"alta_confianza":alta_conf,
                "pct_deuda":round(d.get(predicho,{}).get("pct_deuda",0),1)
            })

        ef_g = round(aciertos/total*100,1) if total>0 else 0
        ef_a = round(alta_conf_aciertos/alta_conf_total*100,1) if alta_conf_total>0 else 0
        return {
            "fecha_desde":str(fecha_desde),"fecha_hasta":str(fecha_hasta),
            "total_sorteos":total,"aciertos_global":aciertos,"efectividad_global":ef_g,
            "alta_confianza_total":alta_conf_total,"alta_confianza_aciertos":alta_conf_aciertos,
            "efectividad_alta_confianza":ef_a,
            "pesos_usados":pesos,
            "mensaje":f"V8: {ef_g}% | Alta conf: {ef_a}% ({alta_conf_aciertos}/{alta_conf_total})",
            "detalle":detalle
        }
    except Exception as e:
        return {"error":str(e)}


async def obtener_bitacora(db) -> list:
    try:
        res = await db.execute(text("""
            SELECT a.hora,a.animal_predicho,
                COALESCE(a.resultado_real,'PENDIENTE'),a.acierto,a.confianza_pct
            FROM auditoria_ia a WHERE a.fecha=CURRENT_DATE ORDER BY a.hora DESC LIMIT 13
        """))
        bitacora = []
        for r in res.fetchall():
            pred = re.sub(r'[^a-z]','',(r[1] or '').lower())
            real = re.sub(r'[^a-z]','',(r[2] or '').lower())
            bitacora.append({
                "hora":r[0],
                "animal_predicho":pred.upper() if pred else "PENDIENTE",
                "resultado_real":real.upper() if real and real!='pendiente' else "PENDIENTE",
                "acierto":r[3],
                "img_predicho":f"{pred}.png" if pred else "pendiente.png",
                "img_real":f"{real}.png" if real and real!='pendiente' else "pendiente.png",
                "confianza":int(round(float(r[4] or 0)))
            })
        return bitacora
    except Exception:
        return []


async def obtener_estadisticas(db) -> dict:
    try:
        res_ef = await db.execute(text("""
            SELECT COUNT(*),
                COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                ROUND(COUNT(CASE WHEN acierto=TRUE THEN 1 END)::NUMERIC/
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)*100,1)
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
            SELECT animalito, COUNT(*) FROM historico
            WHERE fecha>=CURRENT_DATE-INTERVAL '30 days'
            GROUP BY animalito ORDER BY 2 DESC LIMIT 5
        """))
        top_animales = [{"animal":r[0],"veces":r[1]} for r in res_top.fetchall()]

        # Pesos actuales
        pesos = await obtener_pesos_actuales(db)
        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),1) FROM motor_pesos"))
        generacion = res_gen.scalar() or 1

        return {
            "efectividad_global": float(ef[2] or 0),
            "total_auditado": int(ef[0] or 0),
            "aciertos_total": int(ef[1] or 0),
            "aciertos_hoy": int(hoy[0] or 0),
            "sorteos_hoy": int(hoy[1] or 0),
            "top_animales": top_animales,
            "total_historico": total_hist,
            "pesos_actuales": pesos,
            "generacion": generacion
        }
    except Exception:
        return {"efectividad_global":0,"aciertos_hoy":0,"sorteos_hoy":0,
                "total_historico":0,"top_animales":[],"generacion":1}
