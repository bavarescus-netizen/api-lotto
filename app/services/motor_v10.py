"""
MOTOR V10 — LOTTOAI PRO (VERSIÓN CORREGIDA)
===========================================
- Fix: TypeError: unhashable type: 'dict' (Blindaje en combinación de señales)
- Fix: Import path para Render (Estructura de raíz)
- Estabilidad: Filtro isinstance en el loop principal
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
import pytz, re, math

# ══════════════════════════════════════════════════════
# CATÁLOGO COMPLETO — 38 animales Lotto Activo Venezuela
# ══════════════════════════════════════════════════════
MAPA_ANIMALES = {
    "1":"carnero",  "2":"toro",     "3":"ciempies", "4":"alacran",
    "5":"leon",     "6":"rana",     "7":"perico",   "8":"raton",
    "9":"aguila",   "10":"tigre",   "11":"gato",    "12":"caballo",
    "13":"mono",    "14":"paloma",  "15":"zorro",   "16":"oso",
    "17":"pavo",    "18":"burro",   "19":"chivo",   "20":"cochino",
    "21":"gallo",   "22":"camello", "23":"cebra",   "24":"iguana",
    "25":"gallina", "26":"vaca",    "27":"perro",   "28":"zamuro",
    "29":"elefante","30":"caiman",  "31":"lapa",    "32":"ardilla",
    "33":"pescado", "34":"venado",  "35":"jirafa",  "36":"culebra",
    "0":"delfin",   "00":"ballena",
}
NUMERO_POR_ANIMAL = {v: k for k, v in MAPA_ANIMALES.items()}
TODOS_LOS_ANIMALES = sorted(set(MAPA_ANIMALES.values()))

_ALIAS = {
    "alacrán":"alacran",  "caimán":"caiman",   "ciempiés":"ciempies",
    "delfín":"delfin",    "león":"leon",        "pavo real":"pavo",
    "águila":"aguila",    "culebra":"culebra",  "serpiente":"culebra",
    "vibora":"culebra",   "cochino":"cochino",  "cerdo":"cochino",
    "chancho":"cochino",
}

def _normalizar(nombre: str) -> str:
    if not nombre:
        return ""
    n = nombre.lower().strip()
    n = re.sub(r'[^a-záéíóúñ\s]', '', n).strip()
    if n in _ALIAS:
        return _ALIAS[n]
    n = (n.replace('á','a').replace('é','e').replace('í','i')
           .replace('ó','o').replace('ú','u').replace('ñ','n'))
    return n

HORAS_SORTEO_STR = [
    "08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
    "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
    "06:00 PM","07:00 PM",
]

AZAR_ESPERADO = 1.0 / 38
_UMBRAL_RENTABILIDAD_DEFAULT = 10.0
_UMBRAL_CONFIANZA_DEFAULT    = 25

# ══════════════════════════════════════════════════════
# MIGRACIÓN AUTOMÁTICA
# ══════════════════════════════════════════════════════
async def migrar_schema(db):
    sqls = [
        "ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS prediccion_1 VARCHAR(50)",
        "ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS prediccion_2 VARCHAR(50)",
        "ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS prediccion_3 VARCHAR(50)",
        "ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS confianza_hora FLOAT DEFAULT 0",
        "ALTER TABLE auditoria_ia ADD COLUMN IF NOT EXISTS es_hora_rentable BOOLEAN DEFAULT FALSE",
        """CREATE TABLE IF NOT EXISTS rentabilidad_hora (
            hora VARCHAR(20) PRIMARY KEY,
            total_sorteos INT DEFAULT 0,
            aciertos_top1 INT DEFAULT 0,
            aciertos_top3 INT DEFAULT 0,
            efectividad_top1 FLOAT DEFAULT 0,
            efectividad_top3 FLOAT DEFAULT 0,
            es_rentable BOOLEAN DEFAULT FALSE,
            ultima_actualizacion TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS markov_intraday (
            id SERIAL PRIMARY KEY,
            hora_origen VARCHAR(20) NOT NULL,
            hora_destino VARCHAR(20) NOT NULL,
            animal_origen VARCHAR(50) NOT NULL,
            animal_destino VARCHAR(50) NOT NULL,
            frecuencia INTEGER DEFAULT 0,
            probabilidad DOUBLE PRECISION DEFAULT 0,
            ventaja_vs_azar DOUBLE PRECISION DEFAULT 0,
            ultima_actualizacion TIMESTAMP DEFAULT NOW(),
            UNIQUE(hora_origen, hora_destino, animal_origen)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_markov_intraday_lookup ON markov_intraday(hora_origen, hora_destino, animal_origen)",
    ]
    for sql in sqls:
        try:
            await db.execute(text(sql))
        except Exception: pass
    try: await db.commit()
    except Exception: await db.rollback()

# ══════════════════════════════════════════════════════
# CONFIG DINÁMICA
# ══════════════════════════════════════════════════════
async def cargar_config_dinamica(db) -> dict:
    config = {
        "multiplicador_hora":    {},
        "es_rentable_hora":      {},
        "umbral_rentabilidad":   _UMBRAL_RENTABILIDAD_DEFAULT,
        "umbral_confianza":      _UMBRAL_CONFIANZA_DEFAULT,
        "peso_anti_racha_hora":  {},
        "ef_top3_por_hora":      {},
    }
    try:
        res = await db.execute(text("SELECT hora, efectividad_top3, es_rentable, total_sorteos FROM rentabilidad_hora ORDER BY hora"))
        rows = res.fetchall()
        ef_values = []
        for r in rows:
            hora, ef3, rentable, total = r[0], float(r[1] or 0), bool(r[2]), int(r[3] or 0)
            config["ef_top3_por_hora"][hora] = ef3
            config["es_rentable_hora"][hora] = rentable
            if total < 10: mult = 0.90
            elif ef3 >= 15.0: mult = 1.40
            elif ef3 >= 12.0: mult = 1.30
            elif ef3 >= 10.0: mult = 1.15
            elif ef3 >= 8.5:  mult = 1.00
            elif ef3 >= 7.0:  mult = 0.90
            elif ef3 >= 5.0:  mult = 0.75
            else:             mult = 0.60
            config["multiplicador_hora"][hora] = mult
            if ef3 > 0: ef_values.append(ef3)
        if len(ef_values) >= 4:
            ef_sorted = sorted(ef_values)
            config["umbral_rentabilidad"] = round(ef_sorted[int(len(ef_sorted) * 0.75)], 1)
    except Exception: pass

    try:
        res = await db.execute(text("SELECT COUNT(*), SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END) FROM auditoria_señales WHERE fecha >= CURRENT_DATE - INTERVAL '90 days' AND acierto_top3 IS NOT NULL"))
        r = res.fetchone()
        if r and int(r[0] or 0) >= 50:
            config["umbral_confianza"] = max(int((float(r[1])/float(r[0])*100) * 0.85), 20)
    except Exception: pass

    try:
        res = await db.execute(text("""
            WITH pares AS (
                SELECT h1.hora, COUNT(*) AS total, SUM(CASE WHEN LOWER(TRIM(h1.animalito)) = LOWER(TRIM(h2.animalito)) THEN 1 ELSE 0 END) AS repeticiones
                FROM historico h1 JOIN historico h2 ON h1.hora = h2.hora AND h2.fecha = h1.fecha + INTERVAL '1 day'
                WHERE h1.fecha >= CURRENT_DATE - INTERVAL '365 days' AND h1.loteria = 'Lotto Activo' GROUP BY h1.hora
            ) SELECT hora, ROUND((repeticiones::float / NULLIF(total,0) * 100)::numeric, 2) FROM pares WHERE total >= 20
        """))
        for r in res.fetchall():
            ratio = float(r[1] or 2.63) / 2.63
            if ratio <= 0.30: peso = 0.42
            elif ratio <= 0.50: peso = 0.36
            elif ratio <= 0.70: peso = 0.30
            elif ratio <= 1.10: peso = 0.18
            else: peso = 0.12
            config["peso_anti_racha_hora"][r[0]] = peso
    except Exception: pass
    return config

# ══════════════════════════════════════════════════════
# PESOS
# ══════════════════════════════════════════════════════
async def obtener_pesos_para_hora(db, hora_str: str) -> dict:
    try:
        res = await db.execute(text("SELECT peso_decay, peso_markov, peso_gap, peso_reciente FROM motor_pesos_hora WHERE hora = :hora ORDER BY generacion DESC LIMIT 1"), {"hora": hora_str})
        row = res.fetchone()
        if row and any(v is not None for v in row):
            return {"reciente": float(row[3] or 0.25), "deuda": float(row[2] or 0.25), "anti": float(row[0] or 0.25), "patron": float(row[1] or 0.15), "secuencia": 0.10}
    except Exception: pass
    return await _obtener_pesos_globales(db)

async def _obtener_pesos_globales(db) -> dict:
    try:
        res = await db.execute(text("SELECT peso_reciente, peso_deuda, peso_anti, peso_patron, peso_secuencia FROM motor_pesos ORDER BY id DESC LIMIT 1"))
        row = res.fetchone()
        if row: return {"reciente": float(row[0]), "deuda": float(row[1]), "anti": float(row[2]), "patron": float(row[3]), "secuencia": float(row[4])}
    except Exception: pass
    return {"reciente": 0.25, "deuda": 0.28, "anti": 0.22, "patron": 0.15, "secuencia": 0.10}

# ══════════════════════════════════════════════════════
# SEÑALES (DEUDA, FRECUENCIA, PATRON, ANTI, MARKOV, CICLO, FECHA)
# ══════════════════════════════════════════════════════
async def calcular_deuda(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (SELECT animalito, fecha, LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'),
        gaps AS (SELECT animalito, (fecha-fa) AS gap FROM apariciones WHERE fa IS NOT NULL),
        ciclos AS (SELECT animalito, AVG(gap) AS ciclo, STDDEV(gap) AS varianza FROM gaps GROUP BY animalito HAVING COUNT(*)>=3),
        ultima AS (SELECT animalito, :hoy-MAX(fecha) AS dias FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito)
        SELECT u.animalito, u.dias, ROUND(c.ciclo::numeric,1), ROUND((u.dias/NULLIF(c.ciclo,0)*100)::numeric,1), ROUND(COALESCE(c.varianza,0)::numeric,1)
        FROM ultima u JOIN ciclos c ON u.animalito=c.animalito ORDER BY 4 DESC
    """), {"hora": hora_str, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_d = max(float(r[3]) for r in rows) or 1
        for r in rows:
            d, var = float(r[3]), float(r[4] or 5.0)
            score = min(d / max_d, 1.0)
            if d > 400: score = min(score * 1.5, 1.0)
            if var > 15: score *= 0.85
            resultado[_normalizar(r[0])] = {"score": round(score, 4), "dias_ausente": int(r[1]), "ciclo_prom": float(r[2]), "pct_deuda": d}
    return resultado

async def calcular_frecuencia_reciente(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    f7, f30, f90 = fecha_limite - timedelta(days=7), fecha_limite - timedelta(days=30), fecha_limite - timedelta(days=90)
    res = await db.execute(text("""
        WITH v7 AS (SELECT animalito, COUNT(*) AS c FROM historico WHERE hora=:hora AND fecha>=:f7 AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito),
        v30 AS (SELECT animalito, COUNT(*) AS c FROM historico WHERE hora=:hora AND fecha>=:f30 AND fecha<:f7 AND loteria='Lotto Activo' GROUP BY animalito),
        v90 AS (SELECT animalito, COUNT(*) AS c FROM historico WHERE hora=:hora AND fecha>=:f90 AND fecha<:f30 AND loteria='Lotto Activo' GROUP BY animalito),
        todos AS (SELECT animalito FROM historico WHERE hora=:hora AND fecha>=:f90 AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito)
        SELECT t.animalito, COALESCE(v7.c,0)*0.5 + COALESCE(v30.c,0)*0.3 + COALESCE(v90.c,0)*0.2, COALESCE(v7.c,0), COALESCE(v30.c,0), COALESCE(v90.c,0)
        FROM todos t LEFT JOIN v7 ON t.animalito=v7.animalito LEFT JOIN v30 ON t.animalito=v30.animalito LEFT JOIN v90 ON t.animalito=v90.animalito ORDER BY 2 DESC
    """), {"hora": hora_str, "f7": f7, "f30": f30, "f90": f90, "hoy": fecha_limite})
    rows, resultado = res.fetchall(), {}
    if rows:
        max_sc = max(float(r[1]) for r in rows) or 1.0
        for r in rows:
            resultado[_normalizar(r[0])] = {"score": float(r[1])/max_sc, "veces_7d": int(r[2]), "veces_30d": int(r[3]), "veces_90d": int(r[4])}
    return resultado

async def calcular_patron_dia(db, hora_str, dia_semana, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        WITH h AS (SELECT animalito, COUNT(*) AS total FROM historico WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito),
        r AS (SELECT animalito, COUNT(*) AS rec FROM historico WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia AND fecha>=:hoy-INTERVAL'730 days' AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito)
        SELECT h.animalito, h.total*0.6 + COALESCE(r.rec,0)*0.4 FROM h LEFT JOIN r ON h.animalito=r.animalito ORDER BY 2 DESC
    """), {"hora": hora_str, "dia": dia_semana, "hoy": fecha_limite})
    rows, resultado = res.fetchall(), {}
    if rows:
        max_v = max(float(r[1]) for r in rows) or 1.0
        for r in rows: resultado[_normalizar(r[0])] = {"score": float(r[1])/max_v}
    return resultado

async def calcular_anti_racha(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("SELECT animalito, :hoy-MAX(fecha) FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito"), {"hora": hora_str, "hoy": fecha_limite})
    resultado = {}
    for r in res.fetchall():
        dias = int(r[1])
        if dias <= 1: score = 0.01
        elif dias <= 7: score = 0.35
        else: score = 1.0
        resultado[_normalizar(r[0])] = {"score": score, "dias_desde_ultima": dias, "bloquear": dias <= 1}
    return resultado

async def calcular_markov_intraday(db, hora_str, fecha_limite=None) -> dict:
    if fecha_limite is None: fecha_limite = date.today()
    orden = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM","01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM","06:00 PM","07:00 PM"]
    try: idx = orden.index(hora_str)
    except: return {}
    horas_origen = [orden[i] for i in range(max(0, idx-3), idx)]
    mejor_par, mejor_v = None, 0.0
    for h_orig in horas_origen:
        try:
            res_a = await db.execute(text("SELECT LOWER(TRIM(animalito)) FROM historico WHERE hora=:h AND fecha=:hoy AND loteria='Lotto Activo' LIMIT 1"), {"h": h_orig, "hoy": fecha_limite})
            row_a = res_a.fetchone()
            if not row_a: continue
            animal_ant = _normalizar(row_a[0])
            res_p = await db.execute(text("SELECT animal_destino, ventaja_vs_azar, probabilidad FROM markov_intraday WHERE hora_origen=:ho AND hora_destino=:hd AND animal_origen=:a AND frecuencia>=3 AND ventaja_vs_azar>5.0 ORDER BY ventaja_vs_azar DESC LIMIT 1"), {"ho": h_orig, "hd": hora_str, "a": animal_ant})
            row_p = res_p.fetchone()
            if row_p and float(row_p[1]) > mejor_v:
                mejor_v = float(row_p[1])
                mejor_par = {"animal": _normalizar(row_p[0]), "ventaja": mejor_v, "prob": float(row_p[2])}
        except: continue
    if not mejor_par: return {}
    return {mejor_par["animal"]: {"score": round(min(mejor_par["ventaja"]/10.7, 1.0), 4), "ventaja_pct": mejor_par["ventaja"]}}

async def calcular_pares_correlacionados(db, hora_str, fecha_limite=None) -> dict:
    if fecha_limite is None: fecha_limite = date.today()
    try:
        res = await db.execute(text("SELECT animalito FROM historico WHERE hora=:h AND fecha=:a AND loteria='Lotto Activo' LIMIT 1"), {"h": hora_str, "a": fecha_limite-timedelta(days=1)})
        row = res.fetchone()
        if not row: return {}
        animal_ayer = _normalizar(row[0])
        res_p = await db.execute(text("SELECT animal_sig, probabilidad FROM markov_transiciones WHERE hora=:h AND animal_previo=:a AND frecuencia>=5 AND probabilidad>4.0 ORDER BY probabilidad DESC LIMIT 5"), {"h": hora_str, "a": animal_ayer})
        rows = res_p.fetchall()
        resultado = {}
        if rows:
            m_p = max(float(r[1]) for r in rows)
            for r in rows: resultado[_normalizar(r[0])] = {"score": round(float(r[1])/m_p, 4), "prob_real": float(r[1])}
        return resultado
    except: return {}

async def calcular_markov_hora(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    try:
        res_u = await db.execute(text("SELECT animalito FROM historico WHERE hora=:h AND fecha<:hoy AND loteria='Lotto Activo' ORDER BY fecha DESC LIMIT 1"), {"h": hora_str, "hoy": fecha_limite})
        ultimo = res_u.scalar()
        if not ultimo: return {}
        res = await db.execute(text("SELECT animal_sig, probabilidad FROM markov_transiciones WHERE hora=:h AND animal_previo=:p AND frecuencia>=3 ORDER BY probabilidad DESC LIMIT 10"), {"h": hora_str, "p": ultimo})
        rows = res.fetchall()
        if rows:
            m_p = max(float(r[1]) for r in rows)
            return {_normalizar(r[0]): {"score": min(1.0, float(r[1])/m_p), "prob": float(r[1])} for r in rows if float(r[1]) > 0}
    except: pass
    return {}

async def calcular_ciclo_exacto(db, hora_str, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        WITH gaps AS (SELECT animalito, (fecha - LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha)) AS gap FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'),
        stats AS (SELECT animalito, AVG(gap) AS cp, COUNT(*) AS n FROM gaps GROUP BY animalito HAVING COUNT(*)>=5),
        last AS (SELECT animalito, :hoy-MAX(fecha) AS aus FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito)
        SELECT s.animalito, s.cp, l.aus FROM stats s JOIN last l ON s.animalito=l.animalito
    """), {"hora": hora_str, "hoy": fecha_limite})
    resultado = {}
    for r in res.fetchall():
        cp, aus = float(r[1]), int(r[2])
        pct = aus/cp if cp > 0 else 0
        if pct < 0.8: s = 0.3
        elif pct < 1.2: s = 0.9
        else: s = 0.7
        resultado[_normalizar(r[0])] = {"score": round(s, 4), "ciclo_prom_dias": round(cp, 1), "pct_ciclo": round(pct*100, 1)}
    return resultado

async def calcular_patron_fecha_exacta(db, hora_str, dia_semana, mes, fecha_limite=None):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) FROM historico WHERE hora=:h AND EXTRACT(DOW FROM fecha)=:d AND EXTRACT(MONTH FROM fecha)=:m AND fecha<:hoy AND loteria='Lotto Activo' GROUP BY animalito ORDER BY 2 DESC
    """), {"h": hora_str, "d": dia_semana, "m": mes, "hoy": fecha_limite})
    rows, resultado = res.fetchall(), {}
    if rows:
        m_v = max(int(r[1]) for r in rows)
        for r in rows: resultado[_normalizar(r[0])] = {"score": int(r[1])/m_v}
    return resultado

# ══════════════════════════════════════════════════════
# PENALIZACIONES
# ══════════════════════════════════════════════════════
async def calcular_penalizacion_sobreprediccion(db, hora_str, fecha_limite=None, ventana_dias=30):
    if fecha_limite is None: fecha_limite = date.today()
    try:
        res = await db.execute(text("SELECT animal_predicho, COUNT(*), COUNT(CASE WHEN acierto=TRUE THEN 1 END) FROM auditoria_ia WHERE hora=:h AND fecha>=:hoy-INTERVAL'30 days' AND fecha<:hoy GROUP BY animal_predicho"), {"h": hora_str, "hoy": fecha_limite})
        pen = {}
        for r in res.fetchall():
            animal, n_p, n_a = _normalizar(r[0] or ""), int(r[1]), int(r[2])
            if n_p >= 5 and (n_a/n_p) < AZAR_ESPERADO: pen[animal] = round(0.4 + (n_a/n_p)/AZAR_ESPERADO*0.3, 3)
        return pen
    except: return {}

async def calcular_penalizacion_reciente(db, hora_str, fecha_limite=None, ventana=5):
    if fecha_limite is None: fecha_limite = date.today()
    res = await db.execute(text("SELECT animalito FROM historico WHERE hora=:h AND fecha<:hoy AND loteria='Lotto Activo' ORDER BY fecha DESC LIMIT :v"), {"h": hora_str, "hoy": fecha_limite, "v": ventana})
    pen = {}
    for i, r in enumerate(res.fetchall()): pen[_normalizar(r[0])] = round(max(1.0 - (0.15*(ventana-i)/ventana), 0.3), 3)
    return pen

# ══════════════════════════════════════════════════════
# 🛡️ COMBINAR SEÑALES V10 — CON BLINDAJE DE TIPOS
# ══════════════════════════════════════════════════════
def combinar_señales_v10(deuda, reciente, patron, anti, markov, ciclo_exacto, pen_reciente, pen_sobreprediccion, hora_str, pesos, config, patron_fecha=None, pares=None, intraday=None):
    patron_fecha, pares, intraday = patron_fecha or {}, pares or {}, intraday or {}
    
    # 🧠 BLINDAJE: Re-construcción segura de 'todos' (solo strings)
    fuentes = [deuda, reciente, patron, anti, markov, ciclo_exacto, patron_fecha, pares, intraday]
    todos_limpios = set()
    for f in fuentes:
        if isinstance(f, dict):
            for k in f.keys():
                if isinstance(k, str): todos_limpios.add(k)
    todos = todos_limpios

    mult_hora = config.get("multiplicador_hora", {}).get(hora_str, 0.90)
    peso_anti_hora = config.get("peso_anti_racha_hora", {}).get(hora_str, pesos.get("anti", 0.22))
    
    p_ciclo, p_fecha, p_pares, p_intra = 0.15, 0.12, 0.08, 0.14
    suma_pesos = pesos["deuda"] + pesos["reciente"] + pesos["patron"] + peso_anti_hora + pesos["secuencia"] + p_ciclo + p_fecha + p_pares + p_intra
    
    scores = {}
    for animal in todos:
        # 🛡️ Segunda validación dentro del loop
        if not isinstance(animal, str): continue 

        anti_info = anti.get(animal, {})
        bloquear = anti_info.get("bloquear", False)
        
        s_reciente = 0.0 if bloquear else reciente.get(animal, {}).get("score", 0)
        s_par = pares.get(animal, {}).get("score", 0)
        
        base = (
            deuda.get(animal, {}).get("score", 0) * pesos["deuda"] +
            s_reciente * pesos["reciente"] +
            patron.get(animal, {}).get("score", 0) * pesos["patron"] +
            anti_info.get("score", 0.5) * peso_anti_hora +
            markov.get(animal, {}).get("score", 0) * pesos["secuencia"] +
            ciclo_exacto.get(animal,{}).get("score", 0) * p_ciclo +
            patron_fecha.get(animal,{}).get("score", 0) * p_fecha +
            max(s_par, 0) * p_pares +
            intraday.get(animal, {}).get("score", 0) * p_intra
        )
        base /= suma_pesos
        if s_par < 0: base *= 0.70
        base *= pen_reciente.get(animal, 1.0)
        base *= pen_sobreprediccion.get(animal, 1.0)
        
        # 🎯 Aquí animal es string garantizado
        scores[animal] = round(base * mult_hora, 6)
    return scores

# ══════════════════════════════════════════════════════
# ÍNDICE CONFIANZA
# ══════════════════════════════════════════════════════
def wilson_lower(aciertos: int, total: int, z: float = 1.645) -> float:
    if total == 0: return 0.0
    p = aciertos / total
    denom = 1 + z**2 / total
    centro, marg = p + z**2 / (2 * total), z * math.sqrt(p*(1-p)/total + z**2/(4*total**2))
    return max((centro - marg) / denom, 0.0)

def calcular_indice_confianza_v10(scores, config, hora_str, efectividad_hora_top3=None, total_sorteos_hora=0, aciertos_top3_hora=0, racha_fallos=0, ef_top3_reciente=None):
    umbral_operar = config.get("umbral_confianza", _UMBRAL_CONFIANZA_DEFAULT)
    if not scores: return 0, "🔴 SIN DATOS", False
    valores = sorted(scores.values(), reverse=True)
    if len(valores) < 3: return 10, "🔴 DATOS INSUFICIENTES", False
    
    top1, top2, top3 = valores[0], valores[1], valores[2]
    brecha = (top1 - top2) / max(top1, 0.01)
    estabilidad = 1.0 - (top1 - top3)
    
    base = 40
    if ef_top3_reciente: base += (ef_top3_reciente * 1.5)
    base += (brecha * 30) + (estabilidad * 10)
    
    mult_h = config.get("multiplicador_hora", {}).get(hora_str, 1.0)
    confianza = min(int(base * mult_h), 100)
    
    if racha_fallos >= 4: confianza *= 0.8
    if total_sorteos_hora > 20:
        p_inf = wilson_lower(aciertos_top3_hora, total_sorteos_hora)
        if p_inf < 0.05: confianza *= 0.85

    operar = confianza >= umbral_operar and config.get("es_rentable_hora", {}).get(hora_str, False)
    if confianza >= 85: status = "🔥 MUY ALTA"
    elif confianza >= 70: status = "🟢 ALTA"
    elif confianza >= 55: status = "🟡 MEDIA"
    else: status = "🔴 BAJA"
    return int(confianza), status, operar

# ══════════════════════════════════════════════════════
# PREDICCIÓN FINAL
# ══════════════════════════════════════════════════════
async def predecir_v10(db: AsyncSession, hora_str: str, fecha: date = None):
    if fecha is None: fecha = date.today()
    await migrar_schema(db)
    config = await cargar_config_dinamica(db)
    pesos = await obtener_pesos_para_hora(db, hora_str)
    
    # Ejecución en paralelo de señales
    import asyncio
    d, r, p, a, m, c, pf, pc, mi, pr, ps = await asyncio.gather(
        calcular_deuda(db, hora_str, fecha),
        calcular_frecuencia_reciente(db, hora_str, fecha),
        calcular_patron_dia(db, hora_str, fecha.weekday(), fecha),
        calcular_anti_racha(db, hora_str, fecha),
        calcular_markov_hora(db, hora_str, fecha),
        calcular_ciclo_exacto(db, hora_str, fecha),
        calcular_patron_fecha_exacta(db, hora_str, fecha.weekday(), fecha.month, fecha),
        calcular_pares_correlacionados(db, hora_str, fecha),
        calcular_markov_intraday(db, hora_str, fecha),
        calcular_penalizacion_reciente(db, hora_str, fecha),
        calcular_penalizacion_sobreprediccion(db, hora_str, fecha)
    )
    
    scores = combinar_señales_v10(d, r, p, a, m, c, pr, ps, hora_str, pesos, config, pf, pc, mi)
    ef_h3 = config["ef_top3_por_hora"].get(hora_str, 0)
    conf, status, op = calcular_indice_confianza_v10(scores, config, hora_str, ef_h3)
    
    top3 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:3]
    return {
        "hora": hora_str, "fecha": str(fecha), "top3": [{"animal": a, "score": s, "numero": NUMERO_POR_ANIMAL.get(a, "??")} for a, s in top3],
        "confianza": conf, "status": status, "operar": op, "metadatos": {"pesos": pesos, "umbral": config["umbral_confianza"]}
    }
