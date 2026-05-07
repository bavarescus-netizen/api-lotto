"""
MOTOR V101 — LOTTOAI PRO 
========================
VERSIÓN DINÁMICA COMPLETA
Todo se lee desde la BD — sin diccionarios hardcodeados.
Los patrones se actualizan solos con cada ENTRENAR COMPLETO.

Cambios vs versión anterior:
  - _MULTIPLICADOR_HORA        → lee rentabilidad_hora.efectividad_top3
  - _HORAS_PRIORITARIAS        → lee rentabilidad_hora (mismo dato, una sola fuente)
  - _PARES_CORRELACIONADOS     → lee markov_transiciones dinámicamente
  - _PARES_INVERSOS            → lee markov_transiciones (prob < 1.5%)
  - _MARKOV_INTRADAY           → lee markov_intraday (tabla dedicada)
  - _PESO_ANTI_RACHA_HORA      → calcula desde historico real
  - _HORAS_ORIGEN_PARA         → calcula desde markov_intraday
  - UMBRAL_RENTABILIDAD_TOP3   → percentil 75 de rentabilidad_hora
  - UMBRAL_CONFIANZA_OPERAR    → basado en ef promedio histórico
  - pesos ciclo/fecha/pares    → lee motor_pesos_hora
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import re, math

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

# Azar esperado para 38 animales
AZAR_ESPERADO = 1.0 / 38  # 2.63%

# Umbrales base — se recalculan dinámicamente en runtime
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
        except Exception:
            pass
    try:
        await db.commit()
    except Exception:
        await db.rollback()


# ══════════════════════════════════════════════════════
# CONFIG DINÁMICA — carga todo desde BD en una pasada
# Se llama una vez por predicción para no hacer N queries
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
        res = await db.execute(text("""
            SELECT hora, efectividad_top3, es_rentable, total_sorteos
            FROM rentabilidad_hora
            ORDER BY hora
        """))
        rows = res.fetchall()
        ef_values = []
        for r in rows:
            hora    = r[0]
            ef3     = float(r[1] or 0)
            rentable = bool(r[2])
            total   = int(r[3] or 0)
            config["ef_top3_por_hora"][hora]   = ef3
            config["es_rentable_hora"][hora]   = rentable

            if total < 10:
                mult = 0.90
            elif ef3 >= 15.0: mult = 1.40
            elif ef3 >= 12.0: mult = 1.30
            elif ef3 >= 10.0: mult = 1.15
            elif ef3 >= 8.5:  mult = 1.00
            elif ef3 >= 7.0:  mult = 0.90
            elif ef3 >= 5.0:  mult = 0.75
            else:             mult = 0.60

            config["multiplicador_hora"][hora] = mult
            if ef3 > 0:
                ef_values.append(ef3)

        if len(ef_values) >= 4:
            ef_sorted = sorted(ef_values)
            p75_idx   = int(len(ef_sorted) * 0.75)
            config["umbral_rentabilidad"] = round(ef_sorted[p75_idx], 1)

    except Exception:
        pass

    try:
        res = await db.execute(text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END) AS ac3
            FROM auditoria_señales
            WHERE fecha >= CURRENT_DATE - INTERVAL '90 days'
              AND acierto_top3 IS NOT NULL
        """))
        r = res.fetchone()
        if r and int(r[0] or 0) >= 50:
            ef_global = float(r[1] or 0) / float(r[0]) * 100
            config["umbral_confianza"] = max(int(ef_global * 0.85), 20)
    except Exception:
        pass

    try:
        res = await db.execute(text("""
            WITH pares AS (
                SELECT h1.hora,
                    COUNT(*) AS total,
                    SUM(CASE WHEN LOWER(TRIM(h1.animalito)) = LOWER(TRIM(h2.animalito))
                        THEN 1 ELSE 0 END) AS repeticiones
                FROM historico h1
                JOIN historico h2
                    ON h1.hora = h2.hora
                    AND h2.fecha = h1.fecha + INTERVAL '1 day'
                    AND h1.loteria = 'Lotto Activo'
                    AND h2.loteria = 'Lotto Activo'
                WHERE h1.fecha >= CURRENT_DATE - INTERVAL '365 days'
                GROUP BY h1.hora
            )
            SELECT hora,
                   total,
                   repeticiones,
                   ROUND((repeticiones::float / NULLIF(total,0) * 100)::numeric, 2) AS pct_rep
            FROM pares
            WHERE total >= 20
        """))
        rows = res.fetchall()
        azar_rep = 2.63
        for r in rows:
            hora    = r[0]
            pct_rep = float(r[3] or azar_rep)
            ratio   = pct_rep / azar_rep
            if ratio <= 0.30:   peso = 0.42
            elif ratio <= 0.50: peso = 0.36
            elif ratio <= 0.70: peso = 0.30
            elif ratio <= 0.90: peso = 0.22
            elif ratio <= 1.10: peso = 0.18
            elif ratio <= 1.30: peso = 0.15
            else:               peso = 0.12
            config["peso_anti_racha_hora"][hora] = peso
    except Exception:
        pass

    return config


# ══════════════════════════════════════════════════════
# PESOS POR HORA — lee motor_pesos_hora
# ══════════════════════════════════════════════════════
async def obtener_pesos_para_hora(db, hora_str: str) -> dict:
    try:
        res = await db.execute(text("""
            SELECT peso_decay, peso_markov, peso_gap, peso_reciente
            FROM motor_pesos_hora
            WHERE hora = :hora
            ORDER BY generacion DESC LIMIT 1
        """), {"hora": hora_str})
        row = res.fetchone()
        if row and any(v is not None for v in row):
            return {
                "reciente":  float(row[3] or 0.25),
                "deuda":     float(row[2] or 0.25),
                "anti":      float(row[0] or 0.25),
                "patron":    float(row[1] or 0.15),
                "secuencia": 0.10,
            }
    except Exception:
        pass
    return await _obtener_pesos_globales(db)


async def _obtener_pesos_globales(db) -> dict:
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
    return {"reciente": 0.25, "deuda": 0.28, "anti": 0.22, "patron": 0.15, "secuencia": 0.10}


async def guardar_pesos(db, pesos, efectividad, total, aciertos, generacion):
    try:
        await db.execute(text("""
            INSERT INTO motor_pesos
                (peso_reciente,peso_deuda,peso_anti,peso_patron,peso_secuencia,
                 efectividad,total_evaluados,aciertos,generacion)
            VALUES (:r,:d,:a,:p,:s,:ef,:tot,:ac,:gen)
        """), {
            "r": pesos["reciente"], "d": pesos["deuda"], "a": pesos["anti"],
            "p": pesos["patron"],   "s": pesos["secuencia"],
            "ef": efectividad, "tot": total, "ac": aciertos, "gen": generacion
        })
        await db.commit()
    except Exception as e:
        await db.rollback()


# ══════════════════════════════════════════════════════
# SEÑAL 1: DEUDA
# ══════════════════════════════════════════════════════
async def calcular_deuda(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa
            FROM historico
            WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        ),
        gaps AS (SELECT animalito, (fecha-fa) AS gap FROM apariciones WHERE fa IS NOT NULL),
        ciclos AS (
            SELECT animalito, AVG(gap) AS ciclo, STDDEV(gap) AS varianza
            FROM gaps GROUP BY animalito HAVING COUNT(*)>=3
        ),
        ultima AS (
            SELECT animalito, :hoy-MAX(fecha) AS dias
            FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT u.animalito, u.dias,
            ROUND(c.ciclo::numeric,1),
            ROUND((u.dias/NULLIF(c.ciclo,0)*100)::numeric,1),
            ROUND(COALESCE(c.varianza,0)::numeric,1)
        FROM ultima u JOIN ciclos c ON u.animalito=c.animalito
        ORDER BY 4 DESC
    """), {"hora": hora_str, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_d = max(float(r[3]) for r in rows) or 1
        for r in rows:
            d = float(r[3])
            varianza = float(r[4]) if r[4] else 5.0
            score = min(d / max_d, 1.0)
            if d > 400:   score = min(score * 1.5, 1.0)
            elif d > 250: score = min(score * 1.25, 1.0)
            if varianza > 15: score *= 0.85
            resultado[_normalizar(r[0])] = {
                "score": round(score, 4),
                "dias_ausente": int(r[1]),
                "ciclo_prom": float(r[2]),
                "pct_deuda": d,
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 2: FRECUENCIA RECIENTE
# ══════════════════════════════════════════════════════
async def calcular_frecuencia_reciente(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    f7  = fecha_limite - timedelta(days=7)
    f30 = fecha_limite - timedelta(days=30)
    f90 = fecha_limite - timedelta(days=90)
    res = await db.execute(text("""
        WITH v7 AS (
            SELECT animalito, COUNT(*) AS c FROM historico
            WHERE hora=:hora AND fecha>=:f7 AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        v30 AS (
            SELECT animalito, COUNT(*) AS c FROM historico
            WHERE hora=:hora AND fecha>=:f30 AND fecha<:f7 AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        v90 AS (
            SELECT animalito, COUNT(*) AS c FROM historico
            WHERE hora=:hora AND fecha>=:f90 AND fecha<:f30 AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        todos AS (
            SELECT animalito FROM historico
            WHERE hora=:hora AND fecha>=:f90 AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT t.animalito,
               COALESCE(v7.c,0)*0.50 + COALESCE(v30.c,0)*0.30 + COALESCE(v90.c,0)*0.20 AS score_pond,
               COALESCE(v7.c,0) AS c7,
               COALESCE(v30.c,0) AS c30,
               COALESCE(v90.c,0) AS c90
        FROM todos t
        LEFT JOIN v7  ON t.animalito=v7.animalito
        LEFT JOIN v30 ON t.animalito=v30.animalito
        LEFT JOIN v90 ON t.animalito=v90.animalito
        ORDER BY score_pond DESC
    """), {"hora": hora_str, "f7": f7, "f30": f30, "f90": f90, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_sc = max(float(r[1]) for r in rows) or 1.0
        n_animales = len(rows)
        azar_local = 1.0 / max(n_animales, 1)
        total_bruto = sum(int(r[2])+int(r[3])+int(r[4]) for r in rows) or 1
        for r in rows:
            animal = _normalizar(r[0])
            score_norm = float(r[1]) / max_sc
            c7, c30, c90 = int(r[2]), int(r[3]), int(r[4])
            total_c = c7 + c30 + c90
            freq_real = total_c / total_bruto
            ratio = freq_real / azar_local if azar_local > 0 else 1.0
            promedio_esperado_7d = (total_c / 90 * 7) if total_c > 0 else 0
            tendencia = "🔥" if c7 > promedio_esperado_7d * 1.3 else ("❄" if c7 == 0 and c90 > 2 else "→")
            resultado[animal] = {
                "score": score_norm,
                "ratio_vs_azar": round(ratio, 2),
                "veces_7d": c7, "veces_30d": c30, "veces_90d": c90,
                "tendencia": tendencia,
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 3: PATRÓN DÍA SEMANA
# ══════════════════════════════════════════════════════
async def calcular_patron_dia(db, hora_str, dia_semana, fecha_limite=None):
    """
    Señal adaptativa día+hora — V11
    Combina 3 ventanas temporales con pesos dinámicos:
      - 90 días  (peso 0.60) — comportamiento reciente
      - 365 días (peso 0.30) — tendencia anual
      - histórico (peso 0.10) — base estadística
    Los pesos se ajustan automáticamente según cuántos datos hay en cada ventana.
    """
    if fecha_limite is None:
        fecha_limite = date.today()

    f90  = fecha_limite - timedelta(days=90)
    f365 = fecha_limite - timedelta(days=365)

    res = await db.execute(text("""
        WITH v90 AS (
            SELECT animalito, COUNT(*) AS c
            FROM historico
            WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia
              AND fecha>=:f90 AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        v365 AS (
            SELECT animalito, COUNT(*) AS c
            FROM historico
            WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia
              AND fecha>=:f365 AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        vtotal AS (
            SELECT animalito, COUNT(*) AS c
            FROM historico
            WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia
              AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT t.animalito,
               COALESCE(v90.c, 0)                          AS c90,
               COALESCE(v365.c, 0)                         AS c365,
               t.c                                          AS ctotal,
               -- Score ponderado: más peso a lo reciente
               COALESCE(v90.c,0)*0.60
               + COALESCE(v365.c,0)*0.30
               + t.c*0.10                                   AS score_pond
        FROM vtotal t
        LEFT JOIN v90  ON t.animalito=v90.animalito
        LEFT JOIN v365 ON t.animalito=v365.animalito
        ORDER BY score_pond DESC
    """), {
        "hora": hora_str, "dia": dia_semana,
        "hoy": fecha_limite, "f90": f90, "f365": f365
    })
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(float(r[4]) for r in rows) or 1.0
        total_90 = sum(int(r[1]) for r in rows)
        for r in rows:
            c90, c365, ctotal = int(r[1]), int(r[2]), int(r[3])
            score = float(r[4]) / max_v
            # Bonus si el animal está en tendencia creciente en ventana 90d
            if total_90 > 0:
                pct_90 = c90 / total_90
                if pct_90 > 0.15:   # domina más del 15% en 90 días → bonus
                    score = min(score * 1.20, 1.0)
                elif pct_90 == 0:   # no apareció en 90 días → penalizar
                    score *= 0.70
            resultado[_normalizar(r[0])] = {
                "score":    score,
                "veces_90": c90,
                "veces_365": c365,
                "veces_total": ctotal,
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 4: ANTI-RACHA
# ══════════════════════════════════════════════════════
async def calcular_anti_racha(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, :hoy-MAX(fecha) AS dias FROM historico
        WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        GROUP BY animalito
    """), {"hora": hora_str, "hoy": fecha_limite})
    resultado = {}
    for r in res.fetchall():
        dias = int(r[1])
        if dias <= 1:    score = 0.01
        elif dias <= 3:  score = 0.08
        elif dias <= 7:  score = 0.35
        elif dias <= 14: score = 0.60
        elif dias <= 30: score = 0.80
        else:            score = 1.00
        resultado[_normalizar(r[0])] = {
            "score": score,
            "dias_desde_ultima": dias,
            "bloquear": dias <= 1,
        }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL NUEVA: DECAY POR RACHA DE FALLOS — V11
# Si un animal lleva N fallos consecutivos en esta hora → penalizar
# Si lleva N aciertos → bonus
# ══════════════════════════════════════════════════════
async def calcular_decay_racha(db, hora_str, fecha_limite=None) -> dict:
    """
    FIX v11.1 — Usa LEFT JOIN en lugar de INNER JOIN.

    ANTES: JOIN auditoria_ia → solo animales con predicción registrada aparecen.
           Animales sin predicción previa desaparecen del análisis.
    AHORA:
      - LEFT JOIN → todos los animales del historico participan.
      - Si NO hay registro en auditoria_ia → acierto = None → score neutro (0.55).
      - Evalúa si el animal fue predicho (acierto) o simplemente salió (aparición real).
      - Para animales predichos: rastrea racha de fallos/aciertos de la predicción.
      - Para animales sin predicción: usa frecuencia de aparición real como proxy.
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    try:
        # LEFT JOIN: incluye TODOS los animales que salieron, tengan o no predicción
        res = await db.execute(text("""
            WITH ultimos AS (
                SELECT
                    h.animalito,
                    a.acierto,
                    ROW_NUMBER() OVER (PARTITION BY h.animalito ORDER BY h.fecha DESC) AS rn
                FROM historico h
                LEFT JOIN auditoria_ia a
                    ON a.fecha = h.fecha AND a.hora = h.hora
                WHERE h.hora    = :hora
                  AND h.fecha   < :hoy
                  AND h.loteria = 'Lotto Activo'
            )
            SELECT animalito, acierto
            FROM ultimos
            WHERE rn <= 10
            ORDER BY animalito, rn
        """), {"hora": hora_str, "hoy": fecha_limite})
        rows = res.fetchall()

        por_animal = {}
        for r in rows:
            animal  = _normalizar(r[0])
            acierto = r[1]   # puede ser True, False, o None (sin predicción)
            if animal not in por_animal:
                por_animal[animal] = []
            por_animal[animal].append(acierto)

        resultado = {}
        for animal, resultados in por_animal.items():
            fallos_consec   = 0
            aciertos_consec = 0
            sin_pred_count  = sum(1 for ac in resultados if ac is None)

            for ac in resultados:
                if ac is False:
                    fallos_consec   += 1
                    aciertos_consec  = 0
                elif ac is True:
                    aciertos_consec += 1
                    break
                else:
                    # None = no había predicción → no rompe racha pero tampoco cuenta
                    break

            # Score base: 0.55 neutro
            if fallos_consec >= 7:     score = 0.10
            elif fallos_consec >= 5:   score = 0.25
            elif fallos_consec >= 3:   score = 0.40
            elif aciertos_consec >= 3: score = 0.85
            elif aciertos_consec >= 2: score = 0.70
            elif sin_pred_count >= 8:  score = 0.55   # casi nunca predito → neutro
            else:                      score = 0.55

            resultado[animal] = {
                "score":           score,
                "fallos_consec":   fallos_consec,
                "aciertos_consec": aciertos_consec,
                "sin_pred":        sin_pred_count,
            }
        return resultado
    except Exception:
        return {}


# ══════════════════════════════════════════════════════
# SEÑAL NUEVA: MEMORIA CORTA vs LARGA — V11
# 70% últimos 90 días + 30% histórico completo
# Detecta cambios de patrón recientes
# ══════════════════════════════════════════════════════
async def calcular_memoria_adaptativa(db, hora_str, fecha_limite=None) -> dict:
    """
    Combina memoria corta (90 días) y larga (histórico) con peso dinámico.
    Si hay divergencia entre ambas → el sistema prioriza lo reciente.
    Esto permite detectar cambios de patrón automáticamente.
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    f90 = fecha_limite - timedelta(days=90)
    try:
        res = await db.execute(text("""
            WITH corto AS (
                SELECT animalito, COUNT(*) as c
                FROM historico
                WHERE hora=:hora AND fecha>=:f90 AND fecha<:hoy
                  AND loteria='Lotto Activo'
                GROUP BY animalito
            ),
            largo AS (
                SELECT animalito, COUNT(*) as c
                FROM historico
                WHERE hora=:hora AND fecha<:hoy
                  AND loteria='Lotto Activo'
                GROUP BY animalito
            )
            SELECT l.animalito,
                   COALESCE(c.c, 0) as c_corto,
                   l.c              as c_largo
            FROM largo l
            LEFT JOIN corto c ON l.animalito=c.animalito
        """), {"hora": hora_str, "hoy": fecha_limite, "f90": f90})
        rows = res.fetchall()
        if not rows:
            return {}

        total_corto = sum(int(r[1]) for r in rows) or 1
        total_largo = sum(int(r[2]) for r in rows) or 1

        resultado = {}
        max_score = 0.0
        scores_raw = {}
        for r in rows:
            animal = _normalizar(r[0])
            pct_corto = int(r[1]) / total_corto
            pct_largo = int(r[2]) / total_largo
            # 70% memoria corta + 30% memoria larga
            score = pct_corto * 0.70 + pct_largo * 0.30
            # Detectar divergencia: si en corto sube vs largo → bonus tendencia
            divergencia = pct_corto - pct_largo
            if divergencia > 0.05:    score *= 1.15  # tendencia creciente
            elif divergencia < -0.05: score *= 0.85  # tendencia decreciente
            scores_raw[animal] = {
                "score_raw": score,
                "pct_90d": round(pct_corto * 100, 2),
                "pct_hist": round(pct_largo * 100, 2),
                "tendencia": "↑" if divergencia > 0.03 else ("↓" if divergencia < -0.03 else "→"),
            }
            max_score = max(max_score, score)

        max_score = max_score or 1.0
        for animal, data in scores_raw.items():
            resultado[animal] = {
                **data,
                "score": data["score_raw"] / max_score,
            }
        return resultado
    except Exception:
        return {}


# ══════════════════════════════════════════════════════
# SEÑAL 5: MARKOV INTRA-DÍA — DINÁMICO desde markov_intraday
# ══════════════════════════════════════════════════════
async def calcular_markov_intraday(db, hora_str, fecha_limite=None,
                                    resultados_hoy: dict = None) -> dict:
    """
    FIX v11.1 — Intraday con doble fuente de contexto.

    ANTES: buscaba en historico WHERE fecha=HOY → vacío hasta el mediodía.
    AHORA:
      1. Usa resultados_hoy (dict {hora: animal}) si se pasa desde generar_prediccion.
         Esto garantiza que siempre hay contexto real de las horas anteriores del día.
      2. Fallback: si no hay resultado de hoy en hora_origen, usa el último histórico
         de esa hora (día anterior o días pasados) — NUNCA devuelve {} por falta de datos.
      3. Pondera el resultado: si el animal viene de hoy → ventaja × 1.3 (boost).
         Si viene del historial → ventaja × 0.8 (señal más débil pero presente).
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    if resultados_hoy is None:
        resultados_hoy = {}

    orden = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
             "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
             "06:00 PM","07:00 PM"]
    try:
        idx_destino = orden.index(hora_str)
    except ValueError:
        return {}

    horas_origen = [orden[i] for i in range(max(0, idx_destino-3), idx_destino)]
    if not horas_origen:
        return {}

    mejor_par    = None
    mejor_score  = 0.0

    for hora_origen in horas_origen:
        try:
            # ── Fuente 1: resultado real de HOY (pasado desde generar_prediccion) ──
            animal_hoy = resultados_hoy.get(hora_origen)
            boost      = 1.30  # animal confirmado de hoy → señal fuerte

            # ── Fuente 2 (fallback): último histórico de esa hora ──
            if not animal_hoy:
                res_hist = await db.execute(text("""
                    SELECT LOWER(TRIM(animalito))
                    FROM historico
                    WHERE hora = :hora AND fecha < :hoy AND loteria = 'Lotto Activo'
                    ORDER BY fecha DESC LIMIT 1
                """), {"hora": hora_origen, "hoy": fecha_limite})
                row_hist = res_hist.fetchone()
                if not row_hist:
                    continue
                animal_hoy = _normalizar(row_hist[0])
                boost      = 0.80  # señal histórica → penalizar ligeramente

            animal_anterior = _normalizar(animal_hoy)

            # ── Buscar par en markov_intraday (umbral más bajo: 3.0 en lugar de 5.0) ──
            res_par = await db.execute(text("""
                SELECT animal_destino, probabilidad, ventaja_vs_azar, frecuencia
                FROM markov_intraday
                WHERE hora_origen    = :h_orig
                  AND hora_destino   = :h_dest
                  AND animal_origen  = :animal
                  AND frecuencia    >= 3
                  AND ventaja_vs_azar > 3.0
                ORDER BY ventaja_vs_azar DESC
                LIMIT 3
            """), {
                "h_orig": hora_origen,
                "h_dest": hora_str,
                "animal": animal_anterior,
            })
            pares = res_par.fetchall()
            if not pares:
                continue

            for row_par in pares:
                animal_pred = _normalizar(row_par[0])
                ventaja     = float(row_par[2]) * boost
                score       = min(ventaja / 10.70, 1.0)

                if score > mejor_score:
                    mejor_score = score
                    mejor_par = {
                        "animal":      animal_pred,
                        "score":       round(score, 4),
                        "ventaja_pct": round(float(row_par[2]), 2),
                        "ventaja_adj": round(ventaja, 2),
                        "prob_real":   float(row_par[1]),
                        "frecuencia":  int(row_par[3]),
                        "origen":      animal_anterior,
                        "hora_origen": hora_origen,
                        "fuente":      "hoy" if boost > 1.0 else "historico",
                        "tipo":        "intraday",
                    }
        except Exception:
            continue

    if not mejor_par:
        return {}

    return {
        mejor_par["animal"]: {k: v for k, v in mejor_par.items() if k != "animal"}
    }


# ══════════════════════════════════════════════════════
# SEÑAL 6: PARES CORRELACIONADOS — DINÁMICO desde markov_transiciones
# ══════════════════════════════════════════════════════
async def calcular_pares_correlacionados(db, hora_str, fecha_limite=None) -> dict:
    if fecha_limite is None:
        fecha_limite = date.today()
    try:
        res = await db.execute(text("""
            SELECT animalito FROM historico
            WHERE hora    = :hora
              AND fecha   = :ayer
              AND loteria = 'Lotto Activo'
            LIMIT 1
        """), {"hora": hora_str, "ayer": fecha_limite - timedelta(days=1)})
        row = res.fetchone()
        if not row:
            return {}

        animal_ayer = _normalizar(row[0])

        res_pares = await db.execute(text("""
            SELECT animal_sig, probabilidad, frecuencia
            FROM markov_transiciones
            WHERE hora         = :hora
              AND animal_previo = :animal
              AND frecuencia   >= 5
              AND probabilidad  > 4.0
            ORDER BY probabilidad DESC
            LIMIT 5
        """), {"hora": hora_str, "animal": animal_ayer})
        pares = res_pares.fetchall()

        resultado = {}
        if pares:
            max_prob = max(float(r[1]) for r in pares)
            for r in pares:
                animal_dest = _normalizar(r[0])
                prob        = float(r[1])
                ventaja     = prob - 2.63
                score       = min(prob / max_prob, 1.0)
                resultado[animal_dest] = {
                    "score":       round(score, 4),
                    "ventaja_pct": round(ventaja, 2),
                    "prob_real":   round(prob, 2),
                    "origen":      animal_ayer,
                    "tipo":        "positivo",
                    "frecuencia":  int(r[2]),
                }

        res_inv = await db.execute(text("""
            SELECT animal_sig, probabilidad
            FROM markov_transiciones
            WHERE hora         = :hora
              AND animal_previo = :animal
              AND frecuencia   >= 5
              AND probabilidad  < 1.5
            ORDER BY probabilidad ASC
            LIMIT 3
        """), {"hora": hora_str, "animal": animal_ayer})
        for r in res_inv.fetchall():
            animal_pen = _normalizar(r[0])
            resultado[animal_pen] = {
                "score":       -0.5,
                "ventaja_pct": round(float(r[1]) - 2.63, 2),
                "prob_real":   round(float(r[1]), 2),
                "origen":      animal_ayer,
                "tipo":        "negativo",
            }

        return resultado
    except Exception:
        return {}


# ══════════════════════════════════════════════════════
# SEÑAL 7: MARKOV DÍA→DÍA (misma hora)
# ══════════════════════════════════════════════════════
async def calcular_markov_hora(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    try:
        res_u = await db.execute(text("""
            SELECT animalito FROM historico
            WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
            ORDER BY fecha DESC LIMIT 1
        """), {"hora": hora_str, "hoy": fecha_limite})
        ultimo_mk = res_u.scalar()

        if ultimo_mk:
            res = await db.execute(text("""
                SELECT animal_sig, probabilidad, frecuencia
                FROM markov_transiciones
                WHERE hora=:hora AND animal_previo=:prev AND frecuencia >= 3
                ORDER BY probabilidad DESC LIMIT 10
            """), {"hora": hora_str, "prev": ultimo_mk})
        else:
            res = await db.execute(text("""
                SELECT animal_sig, AVG(probabilidad) AS prob, SUM(frecuencia) AS frec
                FROM markov_transiciones
                WHERE hora=:hora AND frecuencia >= 3
                GROUP BY animal_sig ORDER BY prob DESC LIMIT 10
            """), {"hora": hora_str})

        rows = res.fetchall()
        if rows:
            max_p = max(float(r[1]) for r in rows)
            if max_p > 0:
                return {
                    _normalizar(r[0]): {
                        "score": min(1.0, float(r[1]) / max_p),
                        "prob":  round(float(r[1]), 2),
                    }
                    for r in rows if float(r[1]) > 0
                }
    except Exception:
        pass

    try:
        res_u = await db.execute(text("""
            SELECT animalito FROM historico
            WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
            ORDER BY fecha DESC LIMIT 1
        """), {"hora": hora_str, "hoy": fecha_limite})
        ultimo = res_u.scalar()
        if not ultimo:
            return {}
        res = await db.execute(text("""
            WITH seq AS (
                SELECT animalito,
                    LEAD(animalito) OVER (PARTITION BY hora ORDER BY fecha) AS siguiente
                FROM historico
                WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
            )
            SELECT siguiente, COUNT(*) AS veces FROM seq
            WHERE animalito=:ultimo AND siguiente IS NOT NULL
            GROUP BY siguiente ORDER BY veces DESC LIMIT 10
        """), {"hora": hora_str, "ultimo": ultimo, "hoy": fecha_limite})
        rows = res.fetchall()
        resultado = {}
        if rows:
            max_v = max(r[1] for r in rows)
            for r in rows:
                resultado[_normalizar(r[0])] = {
                    "score": r[1] / max_v,
                    "veces": int(r[1]),
                }
        return resultado
    except Exception:
        return {}


# ══════════════════════════════════════════════════════
# SEÑAL 8: CICLO EXACTO
# ══════════════════════════════════════════════════════
async def calcular_ciclo_exacto(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa
            FROM historico
            WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        ),
        gaps AS (SELECT animalito, (fecha-fa) AS gap FROM apariciones WHERE fa IS NOT NULL),
        estadisticas AS (
            SELECT animalito, AVG(gap) AS ciclo_prom, MIN(gap) AS ciclo_min,
                MAX(gap) AS ciclo_max, COUNT(*) AS n_ap
            FROM gaps GROUP BY animalito HAVING COUNT(*)>=5
        ),
        ultima_vez AS (
            SELECT animalito, MAX(fecha) AS ultima_fecha
            FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT e.animalito, e.ciclo_prom, e.ciclo_min, e.ciclo_max,
            e.n_ap, (:hoy - u.ultima_fecha) AS dias_aus
        FROM estadisticas e JOIN ultima_vez u ON e.animalito=u.animalito
    """), {"hora": hora_str, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    for r in rows:
        animal    = _normalizar(r[0])
        ciclo_prom = float(r[1])
        n_ap      = int(r[4])
        dias_aus  = int(r[5])
        pct_ciclo = dias_aus / ciclo_prom if ciclo_prom > 0 else 0

        if pct_ciclo < 0.5:      score = 0.05
        elif pct_ciclo < 0.8:    score = 0.3 + (pct_ciclo - 0.5) * 1.5
        elif pct_ciclo < 1.2:    score = min(0.85 + (pct_ciclo - 0.8) * 0.5, 1.0)
        elif pct_ciclo < 2.0:    score = 1.0
        else:                    score = 0.70

        confiabilidad = min(n_ap / 50.0, 1.0)
        resultado[animal] = {
            "score": round(score * (0.7 + 0.3 * confiabilidad), 4),
            "ciclo_prom_dias": round(ciclo_prom, 1),
            "dias_ausente": dias_aus,
            "pct_ciclo": round(pct_ciclo * 100, 1),
            "n_apariciones": n_ap,
            "ventana": f"{round(float(r[2]),0)}-{round(float(r[3]),0)} días",
        }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 9: PATRÓN FECHA EXACTA
# ══════════════════════════════════════════════════════
async def calcular_patron_fecha_exacta(db, hora_str, dia_semana, mes, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH contexto AS (
            SELECT animalito, COUNT(*) AS total, MAX(fecha) AS ultima
            FROM historico
            WHERE hora=:hora
              AND EXTRACT(DOW FROM fecha)=:dia
              AND EXTRACT(MONTH FROM fecha)=:mes
              AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        reciente_2y AS (
            SELECT animalito, COUNT(*) AS rec
            FROM historico
            WHERE hora=:hora
              AND EXTRACT(DOW FROM fecha)=:dia
              AND EXTRACT(MONTH FROM fecha)=:mes
              AND fecha>=:hoy - INTERVAL '2 years'
              AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT c.animalito,
               c.total * 0.55 + COALESCE(r.rec,0) * 0.45 AS score_pond,
               c.total, COALESCE(r.rec,0) AS rec_2y, c.ultima
        FROM contexto c
        LEFT JOIN reciente_2y r ON c.animalito=r.animalito
        ORDER BY score_pond DESC
    """), {"hora": hora_str, "dia": dia_semana, "mes": mes, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(float(r[1]) for r in rows) or 1.0
        total_muestras = sum(int(r[2]) for r in rows)
        for r in rows:
            animal = _normalizar(r[0])
            resultado[animal] = {
                "score":       float(r[1]) / max_v,
                "veces_total": int(r[2]),
                "veces_2y":    int(r[3]),
                "ultima":      str(r[4]),
                "pct_slot":    round(int(r[2]) / max(total_muestras, 1) * 100, 1),
                "muestras":    total_muestras,
            }
    return resultado


# ══════════════════════════════════════════════════════
# PENALIZACIONES
# ══════════════════════════════════════════════════════
async def calcular_penalizacion_sobreprediccion(db, hora_str, fecha_limite=None, ventana_dias=30):
    """
    FIX v11.1 — Usa prediccion_1/2/3 en lugar de animal_predicho (que es nullable).
    Analiza las 3 posiciones del TOP3 para detectar animales que se repiten
    sin acertar. También revisa acierto_top3 (no solo acierto TOP1).
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    fecha_ini = fecha_limite - timedelta(days=ventana_dias)
    try:
        # Conteo unificado: cada aparición en TOP3 cuenta como predicción
        res = await db.execute(text("""
            WITH predicciones AS (
                SELECT prediccion_1 AS animal, fecha, hora FROM auditoria_ia
                WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta
                  AND prediccion_1 IS NOT NULL
                UNION ALL
                SELECT prediccion_2, fecha, hora FROM auditoria_ia
                WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta
                  AND prediccion_2 IS NOT NULL
                UNION ALL
                SELECT prediccion_3, fecha, hora FROM auditoria_ia
                WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta
                  AND prediccion_3 IS NOT NULL
            ),
            con_resultado AS (
                SELECT p.animal,
                       COUNT(*) AS n_pred,
                       COUNT(CASE WHEN LOWER(TRIM(h.animalito))=LOWER(TRIM(p.animal))
                                  THEN 1 END) AS n_ac
                FROM predicciones p
                LEFT JOIN historico h
                    ON h.fecha=p.fecha AND h.hora=p.hora
                    AND h.loteria='Lotto Activo'
                GROUP BY p.animal
            )
            SELECT animal, n_pred, n_ac FROM con_resultado
        """), {"hora": hora_str, "desde": fecha_ini, "hasta": fecha_limite})

        penalizacion = {}
        for r in res.fetchall():
            animal = _normalizar(r[0] or "")
            if not animal:
                continue
            n_pred = int(r[1])
            n_ac   = int(r[2])
            tasa   = n_ac / n_pred if n_pred > 0 else 0
            # Penalizar si: apareció ≥5 veces y tasa < azar esperado (2.63%)
            if n_pred >= 5 and tasa < AZAR_ESPERADO:
                # Factor: entre 0.35 (pésimo) y 0.70 (justo bajo el azar)
                factor = round(0.35 + (tasa / AZAR_ESPERADO) * 0.35, 3)
                penalizacion[animal] = factor
            elif n_pred >= 10 and tasa == 0:
                penalizacion[animal] = 0.25   # 10+ predicciones sin un solo acierto → penalización dura
            else:
                penalizacion[animal] = 1.0
        return penalizacion
    except Exception:
        return {}


async def calcular_penalizacion_reciente(db, hora_str, fecha_limite=None, ventana=5):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito FROM historico
        WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        ORDER BY fecha DESC LIMIT :ventana
    """), {"hora": hora_str, "hoy": fecha_limite, "ventana": ventana})
    rows = res.fetchall()
    penalizacion = {}
    for i, r in enumerate(rows):
        factor = 1.0 - (0.15 * (ventana - i) / ventana)
        penalizacion[_normalizar(r[0])] = round(max(factor, 0.3), 3)
    return penalizacion


# ══════════════════════════════════════════════════════
# SEÑAL NUEVA: PATRONES INTRADAY CONFIRMADOS — V11.2
# Consulta patrones aprendidos automáticamente.
# La tabla crece sola con descubrir_patrones_nuevos().
# ══════════════════════════════════════════════════════
async def calcular_patrones_confirmados(db, hora_str, fecha_hoy=None,
                                         resultados_hoy: dict = None,
                                         resultado_ayer: dict = None) -> dict:
    """
    Consulta patrones_intraday_confirmados y devuelve boost para
    animales que tienen señal activa basada en lo que ya salió hoy
    o ayer (para patrones tipo 'dia_siguiente').

    Score = ventaja_vs_azar normalizada → 0.0–1.0
    Se suma con peso_patrones_conf = 0.08 en combinar_señales.
    """
    if fecha_hoy is None:
        fecha_hoy = date.today()
    if resultados_hoy is None:
        resultados_hoy = {}
    if resultado_ayer is None:
        resultado_ayer = {}

    try:
        res = await db.execute(text("""
            SELECT trigger_hora, trigger_animal,
                   resultado_hora, resultado_animal,
                   tipo, pct_confirmado, ventaja_vs_azar
            FROM patrones_intraday_confirmados
            WHERE activo = true
              AND resultado_hora = :hora
            ORDER BY ventaja_vs_azar DESC
        """), {"hora": hora_str})
        patrones = res.fetchall()
        if not patrones:
            return {}

        boost = {}
        for p in patrones:
            t_hora   = p[0]
            t_animal = _normalizar(p[1])
            r_animal = _normalizar(p[3])
            tipo     = p[4]
            pct      = float(p[5])
            ventaja  = float(p[6])

            triggered = False
            if tipo == 'mismo_dia':
                # El trigger debe haber salido HOY en una hora anterior
                animal_en_hora = _normalizar(resultados_hoy.get(t_hora, ""))
                if animal_en_hora == t_animal:
                    triggered = True
            elif tipo == 'dia_siguiente':
                # El trigger debe haber salido AYER en esa hora
                animal_ayer = _normalizar(resultado_ayer.get(t_hora, ""))
                if animal_ayer == t_animal:
                    triggered = True

            if triggered:
                score = min(ventaja / 15.0, 1.0)   # normalizar: 15x = score 1.0
                if r_animal not in boost or score > boost[r_animal]["score"]:
                    boost[r_animal] = {
                        "score":        round(score, 4),
                        "pct":          pct,
                        "ventaja":      ventaja,
                        "trigger":      t_animal,
                        "trigger_hora": t_hora,
                        "tipo":         tipo,
                    }
        return boost
    except Exception:
        return {}


async def descubrir_patrones_nuevos(db, min_casos: int = 4,
                                     min_pct: float = 18.0,
                                     ventana_dias: int = 365) -> int:
    """
    Job semanal que analiza el historico buscando pares nuevos
    con pct_real >= min_pct y los inserta en patrones_intraday_confirmados
    si no existen ya.

    Retorna el número de patrones nuevos insertados.
    Llamar desde scheduler cada lunes a las 06:00 AM.
    """
    fecha_desde = date.today() - timedelta(days=ventana_dias)
    try:
        # Contar total de apariciones por (hora, animal) como denominador
        res_total = await db.execute(text("""
            SELECT hora, LOWER(TRIM(animalito)) as animal, COUNT(*) as total
            FROM historico
            WHERE loteria = 'Lotto Activo'
              AND fecha >= :desde
            GROUP BY hora, animal
        """), {"desde": fecha_desde})
        totales = {(r[0], r[1]): int(r[2]) for r in res_total.fetchall()}

        # Contar pares (trigger_hora, trigger_animal) → (resultado_hora, resultado_animal)
        res_pares = await db.execute(text("""
            SELECT
                h1.hora        AS t_hora,
                LOWER(TRIM(h1.animalito)) AS t_animal,
                h2.hora        AS r_hora,
                LOWER(TRIM(h2.animalito)) AS r_animal,
                COUNT(*)       AS coincidencias
            FROM historico h1
            JOIN historico h2
                ON h1.fecha    = h2.fecha
                AND h1.loteria = h2.loteria
                AND h1.hora   != h2.hora
            WHERE h1.loteria = 'Lotto Activo'
              AND h1.fecha   >= :desde
            GROUP BY t_hora, t_animal, r_hora, r_animal
            HAVING COUNT(*) >= :min_casos
        """), {"desde": fecha_desde, "min_casos": min_casos})

        # Patrones ya existentes en la tabla
        res_exist = await db.execute(text("""
            SELECT trigger_hora, trigger_animal, resultado_hora, resultado_animal
            FROM patrones_intraday_confirmados
        """))
        existentes = {
            (r[0], _normalizar(r[1]), r[2], _normalizar(r[3]))
            for r in res_exist.fetchall()
        }

        HORAS_ORDEN = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
                       "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
                       "06:00 PM","07:00 PM"]

        nuevos = 0
        for row in res_pares.fetchall():
            t_hora   = row[0]
            t_animal = _normalizar(row[1])
            r_hora   = row[2]
            r_animal = _normalizar(row[3])
            n_casos  = int(row[4])

            total = totales.get((t_hora, t_animal), 0)
            if total == 0:
                continue
            pct = round(n_casos * 100.0 / total, 2)
            if pct < min_pct:
                continue

            # Ignorar pares ya existentes
            key = (t_hora, t_animal, r_hora, r_animal)
            if key in existentes:
                continue

            # Determinar tipo: mismo_dia si r_hora > t_hora, sino dia_siguiente
            try:
                idx_t = HORAS_ORDEN.index(t_hora)
                idx_r = HORAS_ORDEN.index(r_hora)
                tipo  = 'mismo_dia' if idx_r > idx_t else 'dia_siguiente'
            except ValueError:
                tipo = 'mismo_dia'

            ventaja = round(pct / 2.63, 2)   # vs azar esperado 2.63%

            await db.execute(text("""
                INSERT INTO patrones_intraday_confirmados
                (trigger_hora, trigger_animal, resultado_hora, resultado_animal,
                 tipo, n_casos, total_trigger, pct_confirmado, ventaja_vs_azar,
                 activo, fecha_actualizacion)
                VALUES
                (:t_hora, :t_animal, :r_hora, :r_animal,
                 :tipo, :n_casos, :total, :pct, :ventaja,
                 true, CURRENT_DATE)
            """), {
                "t_hora":  t_hora,  "t_animal": t_animal,
                "r_hora":  r_hora,  "r_animal": r_animal,
                "tipo":    tipo,    "n_casos":  n_casos,
                "total":   total,   "pct":      pct,
                "ventaja": ventaja,
            })
            existentes.add(key)
            nuevos += 1

        await db.commit()
        return nuevos
    except Exception as e:
        return 0


# ══════════════════════════════════════════════════════
# SEÑAL NUEVA: ESTACIONALIDAD MENSUAL — V11.2
# Detecta qué animales dominan en el mes actual
# comparando su frecuencia vs el promedio histórico
# ══════════════════════════════════════════════════════
async def calcular_estacionalidad_mensual(db, hora_str, mes: int,
                                           fecha_limite=None) -> dict:
    """
    Señal de estacionalidad mensual — V11.2

    Lógica:
      1. Cuenta apariciones de cada animal en el mes actual (todos los años, esa hora).
      2. Cuenta apariciones promedio en los demás meses.
      3. Calcula ratio = freq_mes / freq_promedio → si > 1.2 el animal 'domina' ese mes.
      4. Score normalizado 0-1 basado en el ratio, con boost si el animal
         también dominó el mismo mes el año anterior (patrón recurrente).

    Pesos para combinar_señales: peso_estacional = 0.10
    No reemplaza ninguna señal existente — se suma como señal adicional.
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    try:
        res = await db.execute(text("""
            WITH por_mes AS (
                SELECT
                    LOWER(TRIM(animalito)) AS animal,
                    EXTRACT(MONTH FROM fecha)::int AS mes,
                    COUNT(*) AS apariciones
                FROM historico
                WHERE hora    = :hora
                  AND fecha   < :hoy
                  AND loteria = 'Lotto Activo'
                  AND fecha  >= :desde
                GROUP BY animal, mes
            ),
            total_meses AS (
                SELECT animal,
                    SUM(apariciones) AS total,
                    COUNT(DISTINCT mes) AS n_meses
                FROM por_mes GROUP BY animal
            ),
            mes_actual AS (
                SELECT animal, apariciones
                FROM por_mes WHERE mes = :mes
            )
            SELECT
                tm.animal,
                COALESCE(ma.apariciones, 0)            AS freq_mes,
                tm.total                               AS freq_total,
                tm.n_meses                             AS meses_con_datos,
                ROUND((tm.total::float / NULLIF(tm.n_meses,0))::numeric, 2) AS prom_otros
            FROM total_meses tm
            LEFT JOIN mes_actual ma ON tm.animal = ma.animal
            WHERE tm.total >= 5
            ORDER BY freq_mes DESC
        """), {
            "hora":  hora_str,
            "hoy":   fecha_limite,
            "mes":   mes,
            "desde": fecha_limite - timedelta(days=730),  # 2 años de historia
        })
        rows = res.fetchall()
        if not rows:
            return {}

        resultado = {}
        ratios = []
        raw = {}

        for r in rows:
            animal      = _normalizar(r[0])
            freq_mes    = int(r[1])
            prom_otros  = float(r[4]) if r[4] else 1.0
            n_meses     = int(r[3])

            if prom_otros < 0.5:
                prom_otros = 0.5   # evitar divisiones por casi-cero

            ratio = freq_mes / prom_otros
            raw[animal] = {"freq_mes": freq_mes, "prom": prom_otros,
                           "ratio": ratio, "n_meses": n_meses}
            ratios.append(ratio)

        if not ratios:
            return {}

        max_ratio = max(ratios) or 1.0
        min_ratio = min(ratios)

        for animal, d in raw.items():
            ratio = d["ratio"]
            # Normalizar ratio a score 0-1
            score = (ratio - min_ratio) / max(max_ratio - min_ratio, 0.01)
            # Bonus si ratio > 1.3 (animal claramente por encima de su promedio)
            if ratio >= 1.5:   score = min(score * 1.20, 1.0)
            elif ratio >= 1.3: score = min(score * 1.10, 1.0)
            # Penalizar si el animal casi no sale este mes
            elif ratio < 0.6:  score *= 0.60
            elif ratio < 0.8:  score *= 0.80

            resultado[animal] = {
                "score":        round(score, 4),
                "freq_mes":     d["freq_mes"],
                "prom_mensual": round(d["prom"], 2),
                "ratio":        round(ratio, 3),
                "tendencia":    "+" if ratio >= 1.2 else ("-" if ratio < 0.8 else "="),
                "tipo":         "estacional",
            }
        return resultado
    except Exception:
        return {}


# ══════════════════════════════════════════════════════
# COMBINAR SEÑALES V10 — FIX: blindaje de tipos
# ══════════════════════════════════════════════════════
def combinar_señales_v10(deuda, reciente, patron, anti, markov,
                          ciclo_exacto, pen_reciente, pen_sobreprediccion,
                          hora_str, pesos, config,
                          patron_fecha=None, pares=None, intraday=None,
                          decay_racha=None, memoria_adap=None,
                          estacional=None, pat_conf=None):
    """
    Combina señales usando config dinámica desde BD — V11.2
    Nueva señal: estacional + patrones intraday confirmados (auto-aprendizaje)
    """
    # ── Blindaje de tipos ──
    patron_fecha = patron_fecha if isinstance(patron_fecha, dict) else {}
    pares        = pares        if isinstance(pares,        dict) else {}
    intraday     = intraday     if isinstance(intraday,     dict) else {}
    decay_racha  = decay_racha  if isinstance(decay_racha,  dict) else {}
    memoria_adap = memoria_adap if isinstance(memoria_adap, dict) else {}
    estacional   = estacional   if isinstance(estacional,   dict) else {}
    pat_conf     = pat_conf     if isinstance(pat_conf,     dict) else {}

    todos = set(
        list(deuda) + list(reciente) + list(patron) +
        list(anti) + list(markov) + list(ciclo_exacto) +
        list(patron_fecha) + list(pares) + list(intraday) +
        list(decay_racha) + list(memoria_adap) + list(estacional) +
        list(pat_conf)
    )

    mult_hora = config.get("multiplicador_hora", {}).get(hora_str, 0.90)

    peso_anti_hora = config.get("peso_anti_racha_hora", {}).get(
        hora_str, pesos.get("anti", 0.22)
    )

    peso_ciclo      = 0.12
    peso_fecha      = 0.10
    peso_pares      = 0.06
    peso_intra      = 0.12
    peso_decay      = 0.08  # V11: decay por racha
    peso_memoria    = 0.08  # V11: memoria adaptativa
    peso_estacional = 0.08  # V11.2: estacionalidad mensual
    peso_pat_conf   = 0.10  # V11.2: patrones confirmados auto-aprendizaje

    suma_pesos = (
        pesos["deuda"] + pesos["reciente"] + pesos["patron"] +
        peso_anti_hora + pesos["secuencia"] +
        peso_ciclo + peso_fecha + peso_pares + peso_intra +
        peso_decay + peso_memoria + peso_estacional + peso_pat_conf
    )

    scores = {}
    for animal in todos:
        anti_info = anti.get(animal, {})
        bloquear  = anti_info.get("bloquear", False)
        score_reciente = 0.0 if bloquear else reciente.get(animal, {}).get("score", 0)

        par_info  = pares.get(animal, {})
        par_score = par_info.get("score", 0)
        par_contribucion = max(par_score, 0) * peso_pares

        intra_info  = intraday.get(animal, {})
        intra_score = intra_info.get("score", 0)
        intra_contribucion = intra_score * peso_intra

        # V11: decay por racha de fallos/aciertos
        decay_info  = decay_racha.get(animal, {})
        decay_score = decay_info.get("score", 0.55)
        decay_contribucion = decay_score * peso_decay

        # V11: memoria adaptativa 70/30
        mem_info  = memoria_adap.get(animal, {})
        mem_score = mem_info.get("score", 0)
        mem_contribucion = mem_score * peso_memoria

        # V11.2: estacionalidad mensual
        est_info  = estacional.get(animal, {})
        est_score = est_info.get("score", 0.5)   # neutro si no hay dato
        est_contribucion = est_score * peso_estacional

        # V11.2: patrones confirmados auto-aprendizaje
        pc_info  = pat_conf.get(animal, {})
        pc_score = pc_info.get("score", 0.0)     # 0 si no hay patrón activo
        pc_contribucion = pc_score * peso_pat_conf

        base = (
            deuda.get(animal,       {}).get("score", 0) * pesos["deuda"]     +
            score_reciente                               * pesos["reciente"]  +
            patron.get(animal,      {}).get("score", 0) * pesos["patron"]    +
            anti_info.get("score", 0.5)                 * peso_anti_hora     +
            markov.get(animal,      {}).get("score", 0) * pesos["secuencia"] +
            ciclo_exacto.get(animal,{}).get("score", 0) * peso_ciclo         +
            patron_fecha.get(animal,{}).get("score", 0) * peso_fecha         +
            par_contribucion                                                   +
            intra_contribucion                                                 +
            decay_contribucion                                                 +
            mem_contribucion                                                   +
            est_contribucion                                                   +
            pc_contribucion
        )
        base /= suma_pesos

        if par_score < 0:
            base *= 0.70

        base *= pen_reciente.get(animal, 1.0)
        base *= pen_sobreprediccion.get(animal, 1.0)
        scores[animal] = round(base * mult_hora, 6)

    return scores


# ══════════════════════════════════════════════════════
# ÍNDICE DE CONFIANZA V10 — dinámico
# ══════════════════════════════════════════════════════
def wilson_lower(aciertos: int, total: int, z: float = 1.645) -> float:
    if total == 0:
        return 0.0
    p = aciertos / total
    denom  = 1 + z**2 / total
    centro = p + z**2 / (2 * total)
    margen = z * math.sqrt(p*(1-p)/total + z**2/(4*total**2))
    return max((centro - margen) / denom, 0.0)


def calcular_indice_confianza_v10(scores, config, hora_str,
                                   efectividad_hora_top3=None,
                                   total_sorteos_hora=0,
                                   aciertos_top3_hora=0,
                                   racha_fallos=0,
                                   ef_top3_reciente=None):
    umbral_operar = config.get("umbral_confianza", _UMBRAL_CONFIANZA_DEFAULT)

    if not scores:
        return 0, "🔴 SIN DATOS", False

    valores = sorted(scores.values(), reverse=True)
    if len(valores) < 3:
        return 10, "🔴 DATOS INSUFICIENTES", False

    # ── FIX V11.2: escala de confianza recalibrada ──
    # La fórmula anterior dividía por 20 → multiplicaba x5 la efectividad real
    # Con ef_top3 = 8% → base = 40 (muy alto para solo 8% de aciertos)
    # AHORA: escala lineal honesta. 8% efectividad = ~11 de base (cerca del azar)
    # Techo en 50 solo si efectividad > 25% (excelente)
    if ef_top3_reciente is not None and ef_top3_reciente > 0:
        # Azar = 7.5% TOP3. Solo dar crédito por encima del azar.
        sobre_azar = max(ef_top3_reciente - 7.5, 0)
        base_reciente = min(int(15 + sobre_azar * 2.5), 55)
    elif total_sorteos_hora >= 20 and aciertos_top3_hora > 0:
        wilson = wilson_lower(aciertos_top3_hora, total_sorteos_hora)
        base_reciente = min(int(wilson * 300), 45)
    elif efectividad_hora_top3 is not None:
        sobre_azar = max(efectividad_hora_top3 - 7.5, 0)
        base_reciente = min(int(12 + sobre_azar * 2.0), 40)
    else:
        base_reciente = 15  # neutro conservador

    mult_hora  = config.get("multiplicador_hora", {}).get(hora_str or "", 0.90)
    bonus_hora = int((mult_hora - 0.90) * 100)

    top1, top2 = valores[0], valores[1]
    sep_rel    = (top1 - top2) / top1 if top1 > 0 else 0
    bonus_sep  = min(int(sep_rel * 30), 15)
    if sep_rel < 0.05:
        bonus_sep = -10

    confianza = base_reciente + bonus_hora + bonus_sep

    if racha_fallos >= 5:   confianza = max(confianza - 20, 0)
    elif racha_fallos >= 3: confianza = max(confianza - 12, 0)
    elif racha_fallos >= 2: confianza = max(confianza - 6, 0)

    confianza = min(100, max(0, confianza))
    operar    = confianza >= umbral_operar

    if confianza >= 50:
        texto = "🟢 ALTA — OPERAR"
    elif confianza >= umbral_operar:
        texto = "🟡 MEDIA — OPERAR CON CAUTELA"
    else:
        texto = "🔴 BAJA — NO OPERAR"

    return confianza, texto, operar


# ══════════════════════════════════════════════════════
# RENTABILIDAD POR HORA
# ══════════════════════════════════════════════════════
async def calcular_rentabilidad_horas(db) -> dict:
    resultado = {}
    for hora in HORAS_SORTEO_STR:
        try:
            res = await db.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN acierto=TRUE THEN 1 END) AS ac_top1,
                    COUNT(CASE WHEN
                        LOWER(TRIM(h.animalito)) IN (
                            LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                            LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                            LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                        ) THEN 1 END) AS ac_top3
                FROM auditoria_ia a
                JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora
                    AND h.loteria='Lotto Activo'
                WHERE a.hora=:hora AND a.acierto IS NOT NULL
            """), {"hora": hora})
            r = res.fetchone()
            if r and r[0] > 0:
                total = int(r[0]); ac1 = int(r[1]); ac3 = int(r[2])
                ef1   = round(ac1/total*100, 2)
                ef3   = round(ac3/total*100, 2)
                wl    = wilson_lower(ac3, total)
                resultado[hora] = {
                    "total": total, "aciertos_top1": ac1, "aciertos_top3": ac3,
                    "efectividad_top1": ef1, "efectividad_top3": ef3,
                    "wilson_lower_top3": round(wl*100, 2),
                    "es_rentable": ef3 >= 8.5,
                }
            else:
                resultado[hora] = {
                    "total": 0, "efectividad_top1": 0.0, "efectividad_top3": 0.0,
                    "wilson_lower_top3": 0.0, "es_rentable": False,
                }
        except Exception:
            resultado[hora] = {"total":0,"efectividad_top1":0,"efectividad_top3":0,"es_rentable":False}
    return resultado


async def actualizar_tabla_rentabilidad(db, rentabilidad: dict):
    for hora, datos in rentabilidad.items():
        try:
            await db.execute(text("""
                INSERT INTO rentabilidad_hora
                    (hora,total_sorteos,aciertos_top1,aciertos_top3,
                     efectividad_top1,efectividad_top3,es_rentable,ultima_actualizacion)
                VALUES (:hora,:tot,:ac1,:ac3,:ef1,:ef3,:rent,NOW())
                ON CONFLICT (hora) DO UPDATE SET
                    total_sorteos    = EXCLUDED.total_sorteos,
                    aciertos_top1    = EXCLUDED.aciertos_top1,
                    aciertos_top3    = EXCLUDED.aciertos_top3,
                    efectividad_top1 = EXCLUDED.efectividad_top1,
                    efectividad_top3 = EXCLUDED.efectividad_top3,
                    es_rentable      = EXCLUDED.es_rentable,
                    ultima_actualizacion = NOW()
            """), {
                "hora": hora, "tot": datos.get("total",0),
                "ac1": datos.get("aciertos_top1",0), "ac3": datos.get("aciertos_top3",0),
                "ef1": datos.get("efectividad_top1",0), "ef3": datos.get("efectividad_top3",0),
                "rent": datos.get("es_rentable",False),
            })
        except Exception:
            pass
    try:
        await db.commit()
    except Exception:
        await db.rollback()


async def obtener_rentabilidad_hora(db, hora_str) -> dict:
    try:
        res = await db.execute(text("""
            SELECT efectividad_top1, efectividad_top3, es_rentable,
                   aciertos_top3, total_sorteos
            FROM rentabilidad_hora WHERE hora=:hora
        """), {"hora": hora_str})
        r = res.fetchone()
        if r:
            return {
                "efectividad_top1": float(r[0]),
                "efectividad_top3": float(r[1]),
                "es_rentable":      bool(r[2]),
                "aciertos_top3":    int(r[3] or 0),
                "total_sorteos":    int(r[4] or 0),
            }
    except Exception:
        pass
    return {"efectividad_top1":0.0,"efectividad_top3":0.0,
            "es_rentable":False,"aciertos_top3":0,"total_sorteos":0}


# ══════════════════════════════════════════════════════
# RECALCULAR markov_intraday
# ══════════════════════════════════════════════════════
async def recalcular_markov_intraday(db) -> dict:
    try:
        orden = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
                 "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
                 "06:00 PM","07:00 PM"]

        insertados = 0
        azar = 100.0 / 38

        for salto in [1, 2, 3]:
            for i in range(len(orden) - salto):
                h_orig = orden[i]
                h_dest = orden[i + salto]

                res = await db.execute(text("""
                    WITH pares AS (
                        SELECT
                            LOWER(TRIM(h1.animalito)) AS origen,
                            LOWER(TRIM(h2.animalito)) AS destino,
                            COUNT(*) AS ocurrencias
                        FROM historico h1
                        JOIN historico h2
                            ON h2.fecha   = h1.fecha
                            AND h2.hora   = :h_dest
                            AND h1.loteria = 'Lotto Activo'
                            AND h2.loteria = 'Lotto Activo'
                        WHERE h1.hora = :h_orig
                        GROUP BY origen, destino
                        HAVING COUNT(*) >= 3
                    ),
                    totales AS (
                        SELECT origen, SUM(ocurrencias) AS total_origen
                        FROM pares GROUP BY origen
                    )
                    SELECT p.origen, p.destino, p.ocurrencias,
                           ROUND((p.ocurrencias::float / t.total_origen * 100)::numeric, 2) AS prob
                    FROM pares p
                    JOIN totales t ON p.origen = t.origen
                    WHERE p.ocurrencias >= 3
                    ORDER BY prob DESC
                """), {"h_orig": h_orig, "h_dest": h_dest})

                rows = res.fetchall()
                for r in rows:
                    animal_orig = _normalizar(r[0])
                    animal_dest = _normalizar(r[1])
                    ocurrencias = int(r[2])
                    prob        = float(r[3])
                    ventaja     = round(prob - azar, 2)

                    try:
                        await db.execute(text("""
                            INSERT INTO markov_intraday
                                (hora_origen, hora_destino, animal_origen, animal_destino,
                                 frecuencia, probabilidad, ventaja_vs_azar, ultima_actualizacion)
                            VALUES (:ho, :hd, :ao, :ad, :frec, :prob, :ventaja, NOW())
                            ON CONFLICT (hora_origen, hora_destino, animal_origen) DO UPDATE SET
                                animal_destino       = EXCLUDED.animal_destino,
                                frecuencia           = EXCLUDED.frecuencia,
                                probabilidad         = EXCLUDED.probabilidad,
                                ventaja_vs_azar      = EXCLUDED.ventaja_vs_azar,
                                ultima_actualizacion = NOW()
                        """), {
                            "ho": h_orig, "hd": h_dest,
                            "ao": animal_orig, "ad": animal_dest,
                            "frec": ocurrencias, "prob": prob, "ventaja": ventaja,
                        })
                        insertados += 1
                    except Exception:
                        await db.rollback()  # ← FIX: limpiar transacción inválida antes de continuar
                        continue

        await db.commit()
        return {
            "status":     "success",
            "insertados": insertados,
            "message":    f"✅ markov_intraday recalculado — {insertados} pares actualizados",
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# CONTEXTO DIARIO
# ══════════════════════════════════════════════════════
async def obtener_contexto_diario(db, hora_actual_str, fecha=None) -> dict:
    if fecha is None:
        from datetime import date as _date
        fecha = _date.today()

    contexto = {
        "resultados_hoy":     {},
        "pares_activos":      [],
        "pares_cumplidos":    [],
        "pares_fallados":     [],
        "animales_vistos":    [],
        "animales_no_vistos": [],
        "hora_actual":        hora_actual_str,
        "total_sorteos_hoy":  0,
        "patron_dia":         [],
        "cadenas_activas":    [],
        "error":              None,
    }

    try:
        orden_horas = ["08:00 AM","09:00 AM","10:00 AM","11:00 AM",
                       "12:00 PM","01:00 PM","02:00 PM","03:00 PM",
                       "04:00 PM","05:00 PM","06:00 PM","07:00 PM"]

        res = await db.execute(text("""
            SELECT TRIM(hora) AS hora, LOWER(TRIM(animalito)) AS animal
            FROM historico
            WHERE fecha=:hoy AND loteria='Lotto Activo'
            ORDER BY hora ASC
        """), {"hoy": fecha})
        for hora, animal in res.fetchall():
            animal_norm = _normalizar(animal)
            contexto["resultados_hoy"][hora] = animal_norm
            contexto["animales_vistos"].append(animal_norm)

        contexto["total_sorteos_hoy"] = len(contexto["resultados_hoy"])
        contexto["patron_dia"] = [
            contexto["resultados_hoy"].get(h)
            for h in orden_horas if contexto["resultados_hoy"].get(h)
        ]
        contexto["animales_no_vistos"] = sorted(
            set(TODOS_LOS_ANIMALES) - set(contexto["animales_vistos"])
        )

        try:
            idx_actual    = orden_horas.index(hora_actual_str)
            horas_futuras = set(orden_horas[idx_actual:])
        except ValueError:
            horas_futuras = set(orden_horas)

        res_pares = await db.execute(text("""
            SELECT hora_origen, hora_destino, animal_origen, animal_destino, ventaja_vs_azar
            FROM markov_intraday
            WHERE ventaja_vs_azar > 5.0 AND frecuencia >= 3
            ORDER BY ventaja_vs_azar DESC
        """))
        pares_bd = res_pares.fetchall()

        for par in pares_bd:
            h_orig, h_dest, a_orig, a_dest, ventaja = par
            animal_en_origen = contexto["resultados_hoy"].get(h_orig)
            if not animal_en_origen:
                continue
            if _normalizar(animal_en_origen) != _normalizar(a_orig):
                continue

            par_info = {
                "hora_origen":    h_orig,
                "hora_destino":   h_dest,
                "animal_origen":  _normalizar(a_orig),
                "animal_destino": _normalizar(a_dest),
                "ventaja":        float(ventaja),
            }

            if h_dest in contexto["resultados_hoy"]:
                real = contexto["resultados_hoy"][h_dest]
                if real == _normalizar(a_dest):
                    par_info["resultado"] = "✅ CUMPLIDO"
                    contexto["pares_cumplidos"].append(par_info)
                else:
                    par_info["resultado"] = f"❌ salió {real}"
                    contexto["pares_fallados"].append(par_info)
            elif h_dest in horas_futuras:
                par_info["resultado"] = "⏳ pendiente"
                contexto["pares_activos"].append(par_info)

        contexto["pares_activos"].sort(key=lambda x: x["ventaja"], reverse=True)

    except Exception as e:
        contexto["error"] = str(e)

    return contexto


# ══════════════════════════════════════════════════════
# PREDICCIÓN V10 — NÚCLEO PRINCIPAL
# ══════════════════════════════════════════════════════
async def generar_prediccion(db, hora: str = None) -> dict:
    """
    Genera predicción para una hora específica.
    Si hora=None, calcula la hora actual/próxima del sorteo (comportamiento original).
    Si hora se especifica (ej: '03:00 PM'), usa esa hora directamente — 
    esto permite predicciones tentativas y por hora con contexto correcto.
    """
    try:
        tz   = ZoneInfo('America/Caracas')
        ahora = datetime.now(tz)
        _mn  = ahora.minute
        _h   = ahora.hour
        _lbls = {8:'08:00 AM',9:'09:00 AM',10:'10:00 AM',11:'11:00 AM',
                 12:'12:00 PM',13:'01:00 PM',14:'02:00 PM',15:'03:00 PM',
                 16:'04:00 PM',17:'05:00 PM',18:'06:00 PM',19:'07:00 PM'}

        if hora:
            # ✅ Hora explícita — usar la hora indicada con fecha actual
            hora_str = hora
        else:
            # Comportamiento original — calcular hora actual/próxima
            if _h < 8:      hora_str = _lbls[8]
            elif _h >= 19:  hora_str = _lbls[8]
            elif _mn > 2:   hora_str = _lbls.get(_h+1, _lbls[8])
            else:           hora_str = _lbls.get(_h, _lbls[8])

        dia_semana = ahora.weekday()
        hoy        = ahora.date()

        config    = await cargar_config_dinamica(db)
        pesos     = await obtener_pesos_para_hora(db, hora_str)
        rent_hora = await obtener_rentabilidad_hora(db, hora_str)

        # ── FIX v11.1: ctx_dia primero → resultados_hoy para intraday ──
        ctx_dia        = await obtener_contexto_diario(db, hora_str, hoy)
        resultados_hoy = ctx_dia.get("resultados_hoy", {})

        # ── V11.2: resultado_ayer para patrones dia_siguiente ──
        ayer = hoy - timedelta(days=1)
        res_ayer = await db.execute(text("""
            SELECT hora, LOWER(TRIM(animalito))
            FROM historico
            WHERE fecha = :ayer AND loteria = 'Lotto Activo'
        """), {"ayer": ayer})
        resultado_ayer = {r[0]: r[1] for r in res_ayer.fetchall()}

        deuda        = await calcular_deuda(db, hora_str)
        reciente     = await calcular_frecuencia_reciente(db, hora_str)
        patron       = await calcular_patron_dia(db, hora_str, dia_semana)
        anti         = await calcular_anti_racha(db, hora_str)
        markov       = await calcular_markov_hora(db, hora_str)
        ciclo_exacto = await calcular_ciclo_exacto(db, hora_str)
        pen_rec      = await calcular_penalizacion_reciente(db, hora_str)
        pen_sobrep   = await calcular_penalizacion_sobreprediccion(db, hora_str)
        patron_fecha = await calcular_patron_fecha_exacta(db, hora_str, dia_semana, ahora.month)
        pares_corr   = await calcular_pares_correlacionados(db, hora_str)
        # ── FIX v11.1: intraday recibe resultados reales del día ──
        intraday     = await calcular_markov_intraday(db, hora_str,
                                                       resultados_hoy=resultados_hoy)
        # ── V11: Nuevas señales adaptativas ──
        decay_racha  = await calcular_decay_racha(db, hora_str)
        memoria_adap = await calcular_memoria_adaptativa(db, hora_str)
        # ── V11.2: Estacionalidad mensual ──
        estacional   = await calcular_estacionalidad_mensual(db, hora_str, ahora.month)
        # ── V11.2: Patrones intraday confirmados (auto-aprendizaje) ──
        pat_conf     = await calcular_patrones_confirmados(
                           db, hora_str,
                           fecha_hoy=hoy,
                           resultados_hoy=resultados_hoy,
                           resultado_ayer=resultado_ayer)

        # ── FIX v11.1: penalizar animales ya vistos hoy (repetición intradiaria) ──
        animales_vistos_hoy = set(resultados_hoy.values())
        _pen_dia_actual = {a: 0.30 for a in animales_vistos_hoy}

        scores_raw = combinar_señales_v10(
            deuda, reciente, patron, anti, markov,
            ciclo_exacto, pen_rec, pen_sobrep, hora_str, pesos, config,
            patron_fecha=patron_fecha, pares=pares_corr, intraday=intraday,
            decay_racha=decay_racha, memoria_adap=memoria_adap,
            estacional=estacional, pat_conf=pat_conf
        )
        # Aplicar penalización de repetición del día
        scores = {
            animal: score * _pen_dia_actual.get(animal, 1.0)
            for animal, score in scores_raw.items()
        }

        racha_fallos = 0
        ef_top3_reciente = None
        try:
            res_racha = await db.execute(text("""
                SELECT acierto FROM auditoria_ia
                WHERE hora=:hora AND acierto IS NOT NULL
                ORDER BY fecha DESC LIMIT 5
            """), {"hora": hora_str})
            for ac in [r[0] for r in res_racha.fetchall()]:
                if ac is False: racha_fallos += 1
                else: break
        except Exception:
            pass

        try:
            res_ef = await db.execute(text("""
                SELECT COUNT(*),
                    SUM(CASE WHEN LOWER(TRIM(h.animalito)) IN (
                        LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                    ) THEN 1 ELSE 0 END)
                FROM auditoria_ia a
                JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora
                    AND h.loteria='Lotto Activo'
                WHERE a.hora=:hora
                  AND a.fecha >= CURRENT_DATE - INTERVAL '28 days'
                  AND a.acierto IS NOT NULL
            """), {"hora": hora_str})
            row_ef = res_ef.fetchone()
            if row_ef and int(row_ef[0] or 0) >= 10:
                ef_top3_reciente = round(int(row_ef[1] or 0) / int(row_ef[0]) * 100, 1)
        except Exception:
            pass

        confianza_idx, señal_texto, operar = calcular_indice_confianza_v10(
            scores, config, hora_str,
            efectividad_hora_top3 = rent_hora.get("efectividad_top3"),
            total_sorteos_hora    = rent_hora.get("total_sorteos", 0),
            aciertos_top3_hora    = rent_hora.get("aciertos_top3", 0),
            racha_fallos          = racha_fallos,
            ef_top3_reciente      = ef_top3_reciente,
        )

        ranking  = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total_sc = sum(scores.values()) or 1

        top3 = []
        for animal, score in ranking[:3]:
            nombre = _normalizar(animal)
            num    = NUMERO_POR_ANIMAL.get(nombre, "--")
            pct    = round(score / total_sc * 100, 1)
            info_d = deuda.get(nombre, {})
            info_c = ciclo_exacto.get(nombre, {})
            top3.append({
                "numero":        num,
                "animal":        nombre.upper(),
                "imagen":        f"{nombre}.png",
                "porcentaje":    f"{pct}%",
                "score_raw":     round(score, 4),
                "dias_ausente":  info_d.get("dias_ausente", 0),
                "pct_deuda":     info_d.get("pct_deuda", 0),
                "pct_ciclo":     info_c.get("pct_ciclo", 0),
                "ciclo_ventana": info_c.get("ventana", ""),
                "ratio_vs_azar": reciente.get(nombre, {}).get("ratio_vs_azar", 0),
            })

        res_u  = await db.execute(text(
            "SELECT animalito FROM historico WHERE loteria='Lotto Activo' "
            "ORDER BY fecha DESC, hora DESC LIMIT 1"
        ))
        ultimo = res_u.scalar()

        es_hora_rentable = config.get("es_rentable_hora", {}).get(hora_str, False)

        if top3:
            try:
                pred1 = top3[0]["animal"].lower() if len(top3) > 0 else None
                pred2 = top3[1]["animal"].lower() if len(top3) > 1 else None
                pred3 = top3[2]["animal"].lower() if len(top3) > 2 else None
                await db.execute(text("""
                    INSERT INTO auditoria_ia
                        (fecha,hora,animal_predicho,prediccion_1,prediccion_2,prediccion_3,
                         confianza_pct,confianza_hora,es_hora_rentable,resultado_real)
                    VALUES (:f,:h,:a,:p1,:p2,:p3,:c,:ch,:rent,'PENDIENTE')
                    ON CONFLICT (fecha,hora) DO UPDATE SET
                        animal_predicho  = EXCLUDED.animal_predicho,
                        prediccion_1     = EXCLUDED.prediccion_1,
                        prediccion_2     = EXCLUDED.prediccion_2,
                        prediccion_3     = EXCLUDED.prediccion_3,
                        confianza_pct    = EXCLUDED.confianza_pct,
                        confianza_hora   = EXCLUDED.confianza_hora,
                        es_hora_rentable = EXCLUDED.es_hora_rentable
                """), {
                    "f": hoy, "h": hora_str, "a": pred1,
                    "p1": pred1, "p2": pred2, "p3": pred3,
                    "c": float(confianza_idx),
                    "ch": float(rent_hora.get("efectividad_top3", 0)),
                    "rent": es_hora_rentable,
                })
                await db.commit()
            except Exception:
                await db.rollback()

            try:
                animal_top1 = _normalizar(top3[0]["animal"]) if top3 else None
                if animal_top1:
                    sc_t = scores.get(animal_top1, 0) or 0
                    await db.execute(text("""
                        INSERT INTO auditoria_señales (
                            fecha,hora,animal_predicho,resultado_real,
                            score_deuda,score_reciente,score_patron_dia,
                            score_anti_racha,score_markov,score_ciclo_exacto,
                            score_patron_fecha,score_intraday,score_pares,
                            score_final,
                            peso_deuda,peso_reciente,peso_patron,
                            peso_anti,peso_markov,confianza
                        ) VALUES (
                            :f,:h,:animal,'PENDIENTE',
                            :s_deuda,:s_rec,:s_patron,
                            :s_anti,:s_markov,:s_ciclo,
                            :s_fecha,:s_intra,:s_pares,
                            :s_final,
                            :p_deuda,:p_rec,:p_patron,
                            :p_anti,:p_markov,:conf
                        )
                        ON CONFLICT (fecha,hora) DO UPDATE SET
                            animal_predicho    = EXCLUDED.animal_predicho,
                            score_deuda        = EXCLUDED.score_deuda,
                            score_reciente     = EXCLUDED.score_reciente,
                            score_patron_dia   = EXCLUDED.score_patron_dia,
                            score_anti_racha   = EXCLUDED.score_anti_racha,
                            score_markov       = EXCLUDED.score_markov,
                            score_ciclo_exacto = EXCLUDED.score_ciclo_exacto,
                            score_patron_fecha = EXCLUDED.score_patron_fecha,
                            score_intraday     = EXCLUDED.score_intraday,
                            score_pares        = EXCLUDED.score_pares,
                            score_final        = EXCLUDED.score_final,
                            confianza          = EXCLUDED.confianza
                    """), {
                        "f": hoy, "h": hora_str, "animal": animal_top1,
                        "s_deuda":  round(deuda.get(animal_top1,{}).get("score",0)*pesos["deuda"],4),
                        "s_rec":    round(reciente.get(animal_top1,{}).get("score",0)*pesos["reciente"],4),
                        "s_patron": round(patron.get(animal_top1,{}).get("score",0)*pesos["patron"],4),
                        "s_anti":   round(anti.get(animal_top1,{}).get("score",0)*pesos["anti"],4),
                        "s_markov": round(markov.get(animal_top1,{}).get("score",0)*pesos["secuencia"],4),
                        "s_ciclo":  round(ciclo_exacto.get(animal_top1,{}).get("score",0)*0.15,4),
                        "s_fecha":  round(patron_fecha.get(animal_top1,{}).get("score",0)*0.12,4),
                        "s_intra":  round(intraday.get(animal_top1,{}).get("score",0)*0.14,4),
                        "s_pares":  round(pares_corr.get(animal_top1,{}).get("score",0)*0.08,4),
                        "s_final":  round(sc_t,6),
                        "p_deuda":  pesos["deuda"], "p_rec": pesos["reciente"],
                        "p_patron": pesos["patron"], "p_anti": pesos["anti"],
                        "p_markov": pesos["secuencia"],
                        "conf": int(confianza_idx),
                    })
                    await db.commit()
            except Exception:
                await db.rollback()

        idx_actual   = HORAS_SORTEO_STR.index(hora_str) if hora_str in HORAS_SORTEO_STR else -1
        proxima_hora = (HORAS_SORTEO_STR[idx_actual+1]
                        if 0 <= idx_actual < len(HORAS_SORTEO_STR)-1 else None)

        return {
            "top3":                  top3,
            "hora":                  hora_str,
            "ultimo_resultado":      ultimo or "N/A",
            "confianza_idx":         confianza_idx,
            "señal_texto":           señal_texto,
            "operar":                operar,
            "hora_premium":          es_hora_rentable,
            "efectividad_hora_top3": rent_hora.get("efectividad_top3", 0),
            "wilson_lower":          rent_hora.get("wilson_lower_top3", 0),
            "proxima_hora":          proxima_hora,
            "pesos_actuales":        pesos,
            "config_dinamica": {
                "mult_hora":           config.get("multiplicador_hora",{}).get(hora_str, 0.90),
                "peso_anti":           config.get("peso_anti_racha_hora",{}).get(hora_str, 0.22),
                "umbral_rentabilidad": config.get("umbral_rentabilidad", 10.0),
                "umbral_confianza":    config.get("umbral_confianza", 25),
            },
            "contexto_dia": {
                "resultados_hoy":     ctx_dia.get("resultados_hoy", {}),
                "patron_dia":         ctx_dia.get("patron_dia", []),
                "total_sorteos":      ctx_dia.get("total_sorteos_hoy", 0),
                "pares_activos":      ctx_dia.get("pares_activos", []),
                "pares_cumplidos":    ctx_dia.get("pares_cumplidos", []),
                "pares_fallados":     ctx_dia.get("pares_fallados", []),
                "cadenas_activas":    ctx_dia.get("cadenas_activas", []),
                "animales_no_vistos": ctx_dia.get("animales_no_vistos", []),
                "intraday_activo":    intraday,
            },
            "analisis": (
                f"Motor V10 Dinámico | {hora_str} | Conf: {confianza_idx}/100 | "
                f"Ef.Hora(top3): {rent_hora.get('efectividad_top3',0)}% | "
                f"{'✅ OPERAR' if operar else '🚫 NO OPERAR'} | "
                f"Sorteos hoy: {ctx_dia.get('total_sorteos_hoy',0)} | "
                f"Pares activos: {len(ctx_dia.get('pares_activos',[]))}"
            )
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"top3":[],"analisis":f"Error V10: {e}",
                "confianza_idx":0,"operar":False,"señal_texto":"ERROR"}


# ══════════════════════════════════════════════════════
# ENTRENAR
# ══════════════════════════════════════════════════════
async def entrenar_modelo(db) -> dict:
    try:
        await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto        = (LOWER(TRIM(a.animal_predicho))=LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha=h.fecha AND a.hora=h.hora AND h.loteria='Lotto Activo'
              AND (a.acierto IS NULL OR a.resultado_real='PENDIENTE' OR a.resultado_real IS NULL)
        """))

        try:
            await db.execute(text("DELETE FROM probabilidades_hora"))
            await db.execute(text("""
                INSERT INTO probabilidades_hora
                    (hora,animalito,frecuencia,probabilidad,tendencia,ultima_actualizacion)
                WITH base AS (
                    SELECT hora, animalito, COUNT(*) AS total_hist
                    FROM historico WHERE loteria='Lotto Activo' GROUP BY hora, animalito
                ),
                reciente_60 AS (
                    SELECT hora, animalito, COUNT(*) AS total_rec
                    FROM historico
                    WHERE fecha>=CURRENT_DATE-INTERVAL '60 days' AND loteria='Lotto Activo'
                    GROUP BY hora, animalito
                ),
                reciente_7 AS (
                    SELECT hora, animalito, COUNT(*) AS total_7
                    FROM historico
                    WHERE fecha>=CURRENT_DATE-INTERVAL '7 days' AND loteria='Lotto Activo'
                    GROUP BY hora, animalito
                ),
                score_pond AS (
                    SELECT b.hora, b.animalito,
                           b.total_hist*0.50+COALESCE(r60.total_rec,0)*2.0+COALESCE(r7.total_7,0)*5.0 AS score_w
                    FROM base b
                    LEFT JOIN reciente_60 r60 ON b.hora=r60.hora AND b.animalito=r60.animalito
                    LEFT JOIN reciente_7  r7  ON b.hora=r7.hora  AND b.animalito=r7.animalito
                ),
                totales AS (SELECT hora, SUM(score_w) AS gran_total FROM score_pond GROUP BY hora)
                SELECT sp.hora::VARCHAR, sp.animalito, b.total_hist,
                    ROUND((sp.score_w/NULLIF(t.gran_total,0)*100)::numeric,2),
                    CASE WHEN COALESCE(r7.total_7,0)>=2 THEN 'CALIENTE'
                         WHEN COALESCE(r60.total_rec,0)>=3 THEN 'TIBIO'
                         ELSE 'FRIO' END,
                    NOW()
                FROM score_pond sp
                JOIN base b ON sp.hora=b.hora AND sp.animalito=b.animalito
                JOIN totales t ON sp.hora=t.hora
                LEFT JOIN reciente_60 r60 ON sp.hora=r60.hora AND sp.animalito=r60.animalito
                LEFT JOIN reciente_7  r7  ON sp.hora=r7.hora  AND sp.animalito=r7.animalito
            """))
            await db.commit()
        except Exception:
            await db.rollback()

        rentabilidad = await calcular_rentabilidad_horas(db)
        await actualizar_tabla_rentabilidad(db, rentabilidad)

        resultado_intraday = await recalcular_markov_intraday(db)

        res1 = await db.execute(text("SELECT COUNT(*) FROM historico WHERE loteria='Lotto Activo'"))
        total_hist = res1.scalar() or 0
        res2 = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL"))
        cal  = res2.scalar() or 0
        res3 = await db.execute(text("SELECT COUNT(*) FROM auditoria_ia WHERE acierto=TRUE"))
        ac   = res3.scalar() or 0
        res4 = await db.execute(text("""
            SELECT COUNT(*) FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora AND h.loteria='Lotto Activo'
            WHERE LOWER(TRIM(h.animalito)) IN (
                LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
            ) AND a.acierto IS NOT NULL
        """))
        ac3  = res4.scalar() or 0

        ef1  = round(ac/cal*100,1)  if cal > 0 else 0
        ef3  = round(ac3/cal*100,1) if cal > 0 else 0
        horas_rentables = [h for h,d in rentabilidad.items() if d.get("es_rentable")]

        await db.commit()
        return {
            "status":                "success",
            "registros_analizados":  total_hist,
            "efectividad_top1":      ef1,
            "efectividad_top3":      ef3,
            "calibradas":            cal,
            "aciertos_top1":         ac,
            "aciertos_top3":         ac3,
            "horas_rentables":       horas_rentables,
            "rentabilidad_detalle":  rentabilidad,
            "markov_intraday":       resultado_intraday,
            "message": (
                f"✅ V10 Dinámico entrenado. {total_hist:,} registros. "
                f"Top1: {ef1}% | Top3: {ef3}% | "
                f"Horas rentables: {len(horas_rentables)} | "
                f"Pares intraday: {resultado_intraday.get('insertados',0)}"
            ),
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# APRENDIZAJE POR REFUERZO
# ══════════════════════════════════════════════════════
async def aprender_desde_historico(db, fecha_inicio=None, dias_por_generacion=30) -> dict:
    try:
        hoy = date.today()
        if fecha_inicio is None:
            fecha_inicio = date(2018, 1, 1)

        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),0) FROM motor_pesos"))
        generacion_actual = (res_gen.scalar() or 0) + 1

        pesos = await _obtener_pesos_globales(db)
        total_global = aciertos_global = aciertos_top3_global = 0
        generaciones = 0
        log = []
        mejor_ef = 0.0
        mejores_pesos = pesos.copy()
        config = await cargar_config_dinamica(db)

        fecha_ventana = fecha_inicio
        while fecha_ventana < hoy - timedelta(days=7):
            fecha_fin_v = min(fecha_ventana + timedelta(days=dias_por_generacion),
                              hoy - timedelta(days=1))
            res = await db.execute(text("""
                SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
                FROM historico
                WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
                ORDER BY fecha ASC, hora ASC LIMIT 500
            """), {"desde": fecha_ventana, "hasta": fecha_fin_v})
            sorteos = res.fetchall()
            if not sorteos:
                fecha_ventana += timedelta(days=dias_por_generacion)
                continue

            ac_señal = {k: 0 for k in ("reciente","deuda","anti","patron","secuencia")}
            total_v = ac_v = ac3_v = 0

            for s in sorteos[:60]:
                fecha_s, hora_s, real, dia_s = s
                dia_s  = int(dia_s)
                real_n = _normalizar(real)
                try:
                    d   = await calcular_deuda(db, hora_s, fecha_s)
                    r   = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                    p   = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                    a   = await calcular_anti_racha(db, hora_s, fecha_s)
                    m   = await calcular_markov_hora(db, hora_s, fecha_s)
                    ce  = await calcular_ciclo_exacto(db, hora_s, fecha_s)
                    pr  = await calcular_penalizacion_reciente(db, hora_s, fecha_s)
                    ps  = await calcular_penalizacion_sobreprediccion(db, hora_s, fecha_s)
                    pfe = await calcular_patron_fecha_exacta(db, hora_s, dia_s,
                              fecha_s.month, fecha_s)

                    for señal, datos in [("deuda",d),("reciente",r),("anti",a),
                                          ("patron",p),("secuencia",m)]:
                        if datos:
                            mejor = max(datos, key=lambda x: datos[x]["score"])
                            if _normalizar(mejor) == real_n:
                                ac_señal[señal] += 1

                    sc = combinar_señales_v10(d,r,p,a,m,ce,pr,ps,hora_s,pesos,config,
                             patron_fecha=pfe)
                    if sc:
                        rank = sorted(sc.items(), key=lambda x:x[1], reverse=True)
                        top3_pred = [_normalizar(x[0]) for x in rank[:3]]
                        if _normalizar(rank[0][0]) == real_n: ac_v  += 1
                        if real_n in top3_pred:               ac3_v += 1
                    total_v += 1
                except Exception:
                    continue

            if not total_v:
                fecha_ventana += timedelta(days=dias_por_generacion)
                continue

            ef_v  = ac_v  / total_v
            ef3_v = ac3_v / total_v

            total_señal = sum(ac_señal.values()) or 1
            nuevos_p = {}
            for s, ac_s in ac_señal.items():
                nuevos_p[s] = 0.65 * pesos[s] + 0.35 * (ac_s / total_señal)
            tp = sum(nuevos_p.values())
            nuevos_p = {k: round(v/tp, 4) for k,v in nuevos_p.items()}

            if ef_v >= mejor_ef or generaciones == 0:
                if ef_v > mejor_ef:
                    mejor_ef = ef_v
                    mejores_pesos = nuevos_p.copy()
                pesos = nuevos_p

            total_global         += total_v
            aciertos_global      += ac_v
            aciertos_top3_global += ac3_v
            generaciones         += 1
            log.append({
                "ventana":     f"{fecha_ventana}→{fecha_fin_v}",
                "sorteos":     total_v,
                "ef_top1":     round(ef_v*100,1),
                "ef_top3":     round(ef3_v*100,1),
                "mejor_señal": max(ac_señal, key=ac_señal.get),
            })
            fecha_ventana += timedelta(days=dias_por_generacion)

        ef_g  = round(aciertos_global/total_global*100,1)  if total_global > 0 else 0
        ef3_g = round(aciertos_top3_global/total_global*100,1) if total_global > 0 else 0
        await guardar_pesos(db, mejores_pesos, ef_g, total_global, aciertos_global, generacion_actual)

        for hora in HORAS_SORTEO_STR:
            try:
                await db.execute(text("""
                    INSERT INTO motor_pesos_hora
                        (hora,generacion,peso_decay,peso_markov,peso_gap,peso_reciente,
                         efectividad,total_evaluados,aciertos_top3)
                    VALUES (:hora,:gen,:anti,:markov,:deuda,:rec,:ef,:tot,:ac3)
                    ON CONFLICT (hora,generacion) DO UPDATE SET
                        peso_decay=EXCLUDED.peso_decay, peso_markov=EXCLUDED.peso_markov,
                        peso_gap=EXCLUDED.peso_gap, peso_reciente=EXCLUDED.peso_reciente,
                        efectividad=EXCLUDED.efectividad
                """), {
                    "hora": hora, "gen": generacion_actual,
                    "anti":   mejores_pesos["anti"],
                    "markov": mejores_pesos["secuencia"],
                    "deuda":  mejores_pesos["deuda"],
                    "rec":    mejores_pesos["reciente"],
                    "ef": ef3_g, "tot": total_global, "ac3": aciertos_top3_global,
                })
            except Exception:
                pass
        await db.commit()

        return {
            "status":                    "success",
            "generacion":                generacion_actual,
            "total_sorteos_evaluados":   total_global,
            "efectividad_top1":          ef_g,
            "efectividad_top3":          ef3_g,
            "mejores_pesos":             mejores_pesos,
            "message": (
                f"✅ V10 Gen {generacion_actual} | "
                f"Top1: {ef_g}% | Top3: {ef3_g}% | "
                f"Pesos: {mejores_pesos}"
            ),
            "log_ventanas": log[-5:],
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# SCORE POR SEÑAL
# ══════════════════════════════════════════════════════
async def obtener_score_señales(db, dias=90) -> dict:
    try:
        fecha_ini = date.today() - timedelta(days=dias)

        for col in ['score_intraday', 'score_pares']:
            try:
                await db.execute(text(
                    f"ALTER TABLE auditoria_señales ADD COLUMN IF NOT EXISTS {col} FLOAT DEFAULT 0"
                ))
                await db.commit()
            except Exception:
                await db.rollback()

        res = await db.execute(text("""
            SELECT
                CASE
                    WHEN score_deuda >= GREATEST(score_reciente,score_patron_dia,
                         score_anti_racha,score_markov,score_ciclo_exacto,
                         score_patron_fecha,COALESCE(score_intraday,0),COALESCE(score_pares,0))
                    THEN 'deuda'
                    WHEN score_reciente >= GREATEST(score_deuda,score_patron_dia,
                         score_anti_racha,score_markov,score_ciclo_exacto,
                         score_patron_fecha,COALESCE(score_intraday,0),COALESCE(score_pares,0))
                    THEN 'reciente'
                    WHEN score_patron_dia >= GREATEST(score_deuda,score_reciente,
                         score_anti_racha,score_markov,score_ciclo_exacto,
                         score_patron_fecha,COALESCE(score_intraday,0),COALESCE(score_pares,0))
                    THEN 'patron_dia'
                    WHEN score_markov >= GREATEST(score_deuda,score_reciente,
                         score_patron_dia,score_anti_racha,score_ciclo_exacto,
                         score_patron_fecha,COALESCE(score_intraday,0),COALESCE(score_pares,0))
                    THEN 'markov'
                    WHEN COALESCE(score_intraday,0) >= GREATEST(score_deuda,score_reciente,
                         score_patron_dia,score_anti_racha,score_markov,
                         score_ciclo_exacto,score_patron_fecha,COALESCE(score_pares,0))
                    THEN 'intraday'
                    WHEN COALESCE(score_pares,0) >= GREATEST(score_deuda,score_reciente,
                         score_patron_dia,score_anti_racha,score_markov,
                         score_ciclo_exacto,score_patron_fecha)
                    THEN 'pares'
                    WHEN score_ciclo_exacto >= GREATEST(score_deuda,score_reciente,
                         score_patron_dia,score_anti_racha,score_markov,score_patron_fecha)
                    THEN 'ciclo_exacto'
                    WHEN score_patron_fecha >= GREATEST(score_deuda,score_reciente,
                         score_patron_dia,score_anti_racha,score_markov,score_ciclo_exacto)
                    THEN 'patron_fecha'
                    ELSE 'anti_racha'
                END AS señal_dominante,
                COUNT(*) AS total,
                SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END) AS aciertos
            FROM auditoria_señales
            WHERE fecha>=:desde AND acierto_top3 IS NOT NULL
            GROUP BY señal_dominante ORDER BY total DESC
        """), {"desde": fecha_ini})
        rows_dom = {r[0]: (int(r[1]), int(r[2])) for r in res.fetchall()}

        res2 = await db.execute(text("""
            SELECT COUNT(*),
                SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END),
                ROUND(AVG(score_deuda)::numeric,4),
                ROUND(AVG(score_reciente)::numeric,4),
                ROUND(AVG(score_patron_dia)::numeric,4),
                ROUND(AVG(score_anti_racha)::numeric,4),
                ROUND(AVG(score_markov)::numeric,4),
                ROUND(AVG(score_ciclo_exacto)::numeric,4),
                ROUND(AVG(score_patron_fecha)::numeric,4),
                ROUND(AVG(COALESCE(score_intraday,0))::numeric,4),
                ROUND(AVG(COALESCE(score_pares,0))::numeric,4),
                ROUND(AVG(score_final)::numeric,6),
                ROUND(AVG(confianza)::numeric,1)
            FROM auditoria_señales
            WHERE fecha>=:desde AND acierto_top3 IS NOT NULL
        """), {"desde": fecha_ini})
        g = res2.fetchone()
        total_g = int(g[0] or 0)
        ac_g    = int(g[1] or 0)
        ef_g    = round(ac_g/total_g*100,1) if total_g > 0 else 0

        def _señal_info(nombre, score_col_idx, col_label):
            dom = rows_dom.get(nombre, (0,0))
            dom_total, dom_ac = dom
            dom_ef = round(dom_ac/dom_total*100,1) if dom_total > 0 else 0
            contrib = float(g[score_col_idx] or 0)
            contrib_pct = round(contrib/float(g[11] or 1)*100,1) if g[11] else 0
            return {
                "señal":             col_label,
                "total":             dom_total,
                "aciertos":          dom_ac,
                "ef_top3":           dom_ef,
                "vs_azar":           round(dom_ef/7.89, 2),
                "contribucion_avg":  contrib,
                "contribucion_pct":  contrib_pct,
                "recomendacion": (
                    "✅ MANTENER"     if dom_ef >= 9.0 else
                    "⚠️ REVISAR"      if dom_ef >= 7.0 and dom_total >= 20 else
                    "🔴 REDUCIR PESO" if dom_total >= 10 else
                    "📊 SIN DATOS"
                ),
            }

        señales = [
            _señal_info("deuda",       2, "Deuda (días ausente)"),
            _señal_info("reciente",    3, "Frecuencia Reciente"),
            _señal_info("anti_racha",  5, "Anti-racha"),
            _señal_info("markov",      6, "Markov"),
            _señal_info("intraday",    9, "Markov Intra-día"),
            _señal_info("pares",      10, "Pares Correlacionados"),
            _señal_info("patron_dia",  4, "Patrón Día Semana"),
            _señal_info("ciclo_exacto",7, "Ciclo Exacto"),
            _señal_info("patron_fecha",8, "Patrón Fecha Exacta"),
        ]
        señales.sort(key=lambda x: x["total"], reverse=True)

        return {
            "dias_analizados":    dias,
            "total_predicciones": total_g,
            "ef_top3_global":     ef_g,
            "confianza_promedio": float(g[12] or 0),
            "señales":            señales,
        }
    except Exception as e:
        return {"error": str(e), "señales": [], "total_predicciones": 0}


# ══════════════════════════════════════════════════════
# CALIBRAR
# ══════════════════════════════════════════════════════
async def calibrar_predicciones(db) -> dict:
    try:
        r = await db.execute(text("""
            UPDATE auditoria_ia a
            SET acierto        = (LOWER(TRIM(a.animal_predicho))=LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha=h.fecha AND a.hora=h.hora AND h.loteria='Lotto Activo'
              AND (a.acierto IS NULL OR a.resultado_real='PENDIENTE' OR a.resultado_real IS NULL)
        """))
        cal = r.rowcount
        await db.commit()
        return {"calibradas": cal}
    except Exception as e:
        await db.rollback()
        return {"calibradas": 0, "error": str(e)}


async def actualizar_resultados_señales(db) -> dict:
    try:
        r = await db.execute(text("""
            UPDATE auditoria_señales s
            SET resultado_real = h.animalito,
                acierto_top1   = (LOWER(TRIM(s.animal_predicho))=LOWER(TRIM(h.animalito))),
                acierto_top3   = (
                    LOWER(TRIM(h.animalito)) IN (
                        SELECT LOWER(TRIM(a.prediccion_1)) FROM auditoria_ia a
                        WHERE a.fecha=s.fecha AND a.hora=s.hora
                        UNION
                        SELECT LOWER(TRIM(a.prediccion_2)) FROM auditoria_ia a
                        WHERE a.fecha=s.fecha AND a.hora=s.hora
                        UNION
                        SELECT LOWER(TRIM(a.prediccion_3)) FROM auditoria_ia a
                        WHERE a.fecha=s.fecha AND a.hora=s.hora
                    )
                )
            FROM historico h
            WHERE s.fecha=h.fecha AND s.hora=h.hora AND h.loteria='Lotto Activo'
              AND (s.resultado_real='PENDIENTE' OR s.resultado_real IS NULL)
        """))
        actualizados = r.rowcount
        await db.commit()
        return {"actualizados": actualizados}
    except Exception as e:
        await db.rollback()
        return {"actualizados": 0, "error": str(e)}


# ══════════════════════════════════════════════════════
# BITÁCORA Y ESTADÍSTICAS
# ══════════════════════════════════════════════════════
async def obtener_bitacora(db) -> list:
    try:
        res = await db.execute(text("""
            SELECT a.hora, a.animal_predicho, a.prediccion_1, a.prediccion_2, a.prediccion_3,
                COALESCE(a.resultado_real,'PENDIENTE'), a.acierto, a.confianza_pct,
                a.es_hora_rentable
            FROM auditoria_ia a
            WHERE a.fecha=CURRENT_DATE
            ORDER BY a.hora DESC LIMIT 13
        """))
        bitacora = []
        for r in res.fetchall():
            pred  = _normalizar(r[1] or "")
            pred2 = _normalizar(r[2] or "")
            pred3 = _normalizar(r[3] or "")
            pred4 = _normalizar(r[4] or "")
            real  = _normalizar(r[5] or "")
            bitacora.append({
                "hora":             r[0],
                "animal_predicho":  pred.upper() if pred else "PENDIENTE",
                "prediccion_2":     pred2.upper() if pred2 else "",
                "prediccion_3":     pred3.upper() if pred3 else "",
                "prediccion_4":     pred4.upper() if pred4 else "",
                "resultado_real":   real.upper() if real and real != "pendiente" else "PENDIENTE",
                "acierto":          r[6],
                "img_predicho":     f"{pred}.png" if pred else "pendiente.png",
                "img_real":         f"{real}.png" if real and real != "pendiente" else "pendiente.png",
                "confianza":        int(round(float(r[7] or 0))),
                "es_hora_rentable": bool(r[8]) if r[8] is not None else False,
            })
        return bitacora
    except Exception:
        return []


async def obtener_estadisticas(db) -> dict:
    try:
        res_ef = await db.execute(text("""
            SELECT COUNT(*),
                COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                ROUND(COUNT(CASE WHEN acierto=TRUE THEN 1 END)::NUMERIC /
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)*100,1)
            FROM auditoria_ia
        """))
        ef = res_ef.fetchone()

        res_ef3 = await db.execute(text("""
            SELECT COUNT(*) FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora AND h.loteria='Lotto Activo'
            WHERE LOWER(TRIM(h.animalito)) IN (
                LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
            ) AND a.acierto IS NOT NULL
        """))
        ac3 = res_ef3.scalar() or 0
        total_cal = int(ef[0] or 0)
        ef3 = round(ac3/total_cal*100,1) if total_cal > 0 else 0

        res_hoy = await db.execute(text("""
            SELECT COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END)
            FROM auditoria_ia WHERE fecha=CURRENT_DATE
        """))
        hoy = res_hoy.fetchone()

        res_total = await db.execute(text(
            "SELECT COUNT(*) FROM historico WHERE loteria='Lotto Activo'"))
        total_hist = res_total.scalar() or 0

        res_top = await db.execute(text("""
            SELECT animalito, COUNT(*) FROM historico
            WHERE fecha>=CURRENT_DATE-INTERVAL '30 days' AND loteria='Lotto Activo'
            GROUP BY animalito ORDER BY 2 DESC LIMIT 5
        """))
        top_animales = [{"animal": r[0], "veces": r[1]} for r in res_top.fetchall()]

        res_rent = await db.execute(text("""
            SELECT hora, efectividad_top3, es_rentable
            FROM rentabilidad_hora WHERE es_rentable=TRUE
            ORDER BY efectividad_top3 DESC
        """))
        horas_rentables = [{"hora": r[0], "ef_top3": float(r[1])} for r in res_rent.fetchall()]

        pesos = await _obtener_pesos_globales(db)
        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),1) FROM motor_pesos"))
        generacion = res_gen.scalar() or 1
        wilson_top3 = round(wilson_lower(ac3, total_cal)*100, 2) if total_cal > 0 else 0

        config = await cargar_config_dinamica(db)

        return {
            "efectividad_global":    float(ef[2] or 0),
            "efectividad_top3":      ef3,
            "wilson_lower_top3":     wilson_top3,
            "total_auditado":        total_cal,
            "aciertos_total":        int(ef[1] or 0),
            "aciertos_top3":         ac3,
            "aciertos_hoy":          int(hoy[0] or 0),
            "sorteos_hoy":           int(hoy[1] or 0),
            "top_animales":          top_animales,
            "total_historico":       total_hist,
            "horas_rentables":       horas_rentables,
            "pesos_actuales":        pesos,
            "generacion":            generacion,
            "umbral_rentabilidad":   config.get("umbral_rentabilidad", 10.0),
            "umbral_confianza":      config.get("umbral_confianza", 25),
        }
    except Exception:
        return {
            "efectividad_global":0,"efectividad_top3":0,"wilson_lower_top3":0,
            "aciertos_hoy":0,"sorteos_hoy":0,"total_historico":0,
            "top_animales":[],"horas_rentables":[],"generacion":1,
        }


# ══════════════════════════════════════════════════════
# BACKTEST V10
# ══════════════════════════════════════════════════════
async def backtest(db, fecha_desde, fecha_hasta, max_sorteos=100) -> dict:
    try:
        pesos  = await _obtener_pesos_globales(db)
        config = await cargar_config_dinamica(db)
        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
            ORDER BY fecha DESC, hora DESC LIMIT :lim
        """), {"desde": fecha_desde, "hasta": fecha_hasta, "lim": max_sorteos})
        sorteos = res.fetchall()
        if not sorteos:
            return {"error": "Sin datos en ese rango"}

        ac1 = ac3 = total = 0
        ac1_c = ac3_c = total_c = 0
        por_hora = {}
        detalle  = []
        umbral   = config.get("umbral_confianza", _UMBRAL_CONFIANZA_DEFAULT)

        for s in sorteos:
            fecha_s, hora_s, real, dia_s = s
            dia_s  = int(dia_s)
            real_n = _normalizar(real)
            d   = await calcular_deuda(db, hora_s, fecha_s)
            r   = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
            p   = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
            a   = await calcular_anti_racha(db, hora_s, fecha_s)
            m   = await calcular_markov_hora(db, hora_s, fecha_s)
            ce  = await calcular_ciclo_exacto(db, hora_s, fecha_s)
            pr  = await calcular_penalizacion_reciente(db, hora_s, fecha_s)
            ps  = await calcular_penalizacion_sobreprediccion(db, hora_s, fecha_s)
            pfe = await calcular_patron_fecha_exacta(db, hora_s, dia_s, fecha_s.month, fecha_s)
            sc  = combinar_señales_v10(d,r,p,a,m,ce,pr,ps,hora_s,pesos,config,patron_fecha=pfe)
            if not sc:
                continue

            confianza_idx, _, operar = calcular_indice_confianza_v10(sc, config, hora_s)
            rank   = sorted(sc.items(), key=lambda x:x[1], reverse=True)
            pred1  = _normalizar(rank[0][0]) if rank else ""
            top3_l = [_normalizar(x[0]) for x in rank[:3]]

            a1 = pred1 == real_n
            a3 = real_n in top3_l
            total += 1
            if a1: ac1 += 1
            if a3: ac3 += 1
            if operar:
                total_c += 1
                if a1: ac1_c += 1
                if a3: ac3_c += 1

            por_hora.setdefault(hora_s, {"total":0,"ac1":0,"ac3":0})
            por_hora[hora_s]["total"] += 1
            if a1: por_hora[hora_s]["ac1"] += 1
            if a3: por_hora[hora_s]["ac3"] += 1

            detalle.append({
                "fecha": str(fecha_s), "hora": hora_s,
                "pred1": pred1,
                "pred2": top3_l[1] if len(top3_l)>1 else "",
                "pred3": top3_l[2] if len(top3_l)>2 else "",
                "real": real_n, "acierto_top1": a1, "acierto_top3": a3,
                "confianza": confianza_idx, "operar": operar,
            })

        ef1  = round(ac1/total*100,1)     if total   > 0 else 0
        ef3  = round(ac3/total*100,1)     if total   > 0 else 0
        ef1c = round(ac1_c/total_c*100,1) if total_c > 0 else 0
        ef3c = round(ac3_c/total_c*100,1) if total_c > 0 else 0

        resumen_horas = {
            h: {
                "total":             d["total"],
                "efectividad_top1":  round(d["ac1"]/d["total"]*100,1) if d["total"]>0 else 0,
                "efectividad_top3":  round(d["ac3"]/d["total"]*100,1) if d["total"]>0 else 0,
                "es_rentable":       (d["ac3"]/d["total"]*100 >= config.get("umbral_rentabilidad",10.0))
                                     if d["total"]>0 else False,
            }
            for h,d in por_hora.items()
        }

        return {
            "total_sorteos":        total,
            "efectividad_top1":     ef1,
            "efectividad_top3":     ef3,
            "filtrado_conf_top1":   ef1c,
            "filtrado_conf_top3":   ef3c,
            "filtrado_conf_n":      total_c,
            "umbral_confianza":     umbral,
            "resumen_por_hora":     resumen_horas,
            "mensaje": (
                f"V10 Dinámico: Top1 {ef1}% | Top3 {ef3}% | "
                f"Filtrado conf≥{umbral} → Top1 {ef1c}% | Top3 {ef3c}%"
            ),
            "detalle": detalle,
        }
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════
# LLENAR AUDITORÍA RETROACTIVA
# ══════════════════════════════════════════════════════
async def llenar_auditoria_retroactiva(db, fecha_desde=None, fecha_hasta=None, dias=30) -> dict:
    try:
        hoy = date.today()
        if fecha_desde is None:
            fecha_desde = date(2018, 1, 1)
        if fecha_hasta is None:
            fecha_hasta = hoy - timedelta(days=1)

        pesos  = await _obtener_pesos_globales(db)
        config = await cargar_config_dinamica(db)

        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int,
                   EXTRACT(MONTH FROM fecha)::int
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
            ORDER BY fecha ASC, hora ASC
        """), {"desde": fecha_desde, "hasta": fecha_hasta})
        sorteos = res.fetchall()

        if not sorteos:
            return {"status":"ok","procesados":0,
                    "message":f"Sin sorteos entre {fecha_desde} y {fecha_hasta}"}

        insertados = 0; omitidos = 0; aciertos1 = 0; aciertos3 = 0
        señales_insertadas = 0

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s, mes_s = sorteo
            dia_s  = int(dia_s)
            mes_s  = int(mes_s)
            real_n = _normalizar(real)
            try:
                res_e = await db.execute(text(
                    "SELECT 1 FROM auditoria_ia "
                    "WHERE fecha=:f AND hora=:h AND acierto IS NOT NULL "
                    "AND prediccion_1 IS NOT NULL LIMIT 1"
                ), {"f": fecha_s, "h": hora_s})
                ya_existe_ia = res_e.fetchone() is not None

                ya_existe_sig = False
                try:
                    res_sig = await db.execute(text(
                        "SELECT 1 FROM auditoria_señales WHERE fecha=:f AND hora=:h LIMIT 1"
                    ), {"f": fecha_s, "h": hora_s})
                    ya_existe_sig = res_sig.fetchone() is not None
                except Exception:
                    await db.rollback()

                if ya_existe_sig:
                    omitidos += 1
                    continue

                if ya_existe_ia:
                    res_pred = await db.execute(text("""
                        SELECT prediccion_1, prediccion_2, prediccion_3, acierto, confianza_pct
                        FROM auditoria_ia WHERE fecha=:f AND hora=:h LIMIT 1
                    """), {"f": fecha_s, "h": hora_s})
                    row_ia = res_pred.fetchone()
                    if not row_ia or not row_ia[0]:
                        omitidos += 1
                        continue
                    pred1 = _normalizar(row_ia[0])
                    pred2 = _normalizar(row_ia[1]) if row_ia[1] else None
                    pred3 = _normalizar(row_ia[2]) if row_ia[2] else None
                    acerto1 = bool(row_ia[3])
                    acerto3 = real_n in [x for x in [pred1,pred2,pred3] if x]
                    confianza_idx = int(row_ia[4] or 0)
                    d={}; r={}; p={}; a={}; m={}; ce={}; pfe={}
                    sc = {pred1: 1.0}
                else:
                    d   = await calcular_deuda(db, hora_s, fecha_s)
                    r   = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                    p   = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                    a   = await calcular_anti_racha(db, hora_s, fecha_s)
                    m   = await calcular_markov_hora(db, hora_s, fecha_s)
                    ce  = await calcular_ciclo_exacto(db, hora_s, fecha_s)
                    pr  = await calcular_penalizacion_reciente(db, hora_s, fecha_s)
                    ps  = await calcular_penalizacion_sobreprediccion(db, hora_s, fecha_s)
                    pfe = await calcular_patron_fecha_exacta(db, hora_s, dia_s, mes_s, fecha_s)

                    sc = combinar_señales_v10(d,r,p,a,m,ce,pr,ps,hora_s,pesos,config,
                                              patron_fecha=pfe)
                    if not sc:
                        continue

                    confianza_idx, _, _ = calcular_indice_confianza_v10(sc, config, hora_s)
                    ranking = sorted(sc.items(), key=lambda x:x[1], reverse=True)
                    pred1 = _normalizar(ranking[0][0]) if len(ranking)>0 else None
                    pred2 = _normalizar(ranking[1][0]) if len(ranking)>1 else None
                    pred3 = _normalizar(ranking[2][0]) if len(ranking)>2 else None
                    acerto1 = (pred1 == real_n)
                    acerto3 = real_n in [x for x in [pred1,pred2,pred3] if x]

                if not ya_existe_ia:
                    await db.execute(text("""
                        INSERT INTO auditoria_ia
                            (fecha,hora,animal_predicho,prediccion_1,prediccion_2,
                             prediccion_3,confianza_pct,resultado_real,acierto)
                        VALUES (:f,:h,:a,:p1,:p2,:p3,:c,:r,:ac)
                        ON CONFLICT (fecha,hora) DO UPDATE SET
                            animal_predicho=EXCLUDED.animal_predicho,
                            prediccion_1=EXCLUDED.prediccion_1,
                            prediccion_2=EXCLUDED.prediccion_2,
                            prediccion_3=EXCLUDED.prediccion_3,
                            confianza_pct=EXCLUDED.confianza_pct,
                            resultado_real=EXCLUDED.resultado_real,
                            acierto=EXCLUDED.acierto
                    """), {
                        "f":fecha_s,"h":hora_s,"a":pred1,
                        "p1":pred1,"p2":pred2,"p3":pred3,
                        "c":float(confianza_idx),"r":real_n,"ac":acerto1,
                    })
                    insertados += 1
                    if acerto1: aciertos1 += 1
                    if acerto3: aciertos3 += 1

                if not ya_existe_sig and pred1:
                    try:
                        await db.execute(text("""
                            INSERT INTO auditoria_señales (
                                fecha,hora,animal_predicho,resultado_real,
                                acierto_top1,acierto_top3,confianza,
                                score_deuda,score_reciente,score_patron_dia,
                                score_anti_racha,score_markov,score_ciclo_exacto,
                                score_patron_fecha,score_final,
                                peso_deuda,peso_reciente,peso_patron,
                                peso_anti,peso_markov
                            ) VALUES (
                                :f,:h,:animal,:real,
                                :ac1,:ac3,:conf,
                                :s_deuda,:s_rec,:s_patron,
                                :s_anti,:s_markov,:s_ciclo,
                                :s_fecha,:s_final,
                                :p_deuda,:p_rec,:p_patron,
                                :p_anti,:p_markov
                            )
                            ON CONFLICT (fecha,hora) DO UPDATE SET
                                resultado_real=EXCLUDED.resultado_real,
                                acierto_top1=EXCLUDED.acierto_top1,
                                acierto_top3=EXCLUDED.acierto_top3,
                                score_deuda=EXCLUDED.score_deuda,
                                score_reciente=EXCLUDED.score_reciente,
                                score_patron_dia=EXCLUDED.score_patron_dia,
                                score_anti_racha=EXCLUDED.score_anti_racha,
                                score_markov=EXCLUDED.score_markov,
                                score_ciclo_exacto=EXCLUDED.score_ciclo_exacto,
                                score_patron_fecha=EXCLUDED.score_patron_fecha,
                                score_final=EXCLUDED.score_final
                        """), {
                            "f":fecha_s,"h":hora_s,"animal":pred1,"real":real_n,
                            "ac1":acerto1,"ac3":acerto3,"conf":int(confianza_idx),
                            "s_deuda":  round(d.get(pred1,{}).get("score",0)*pesos["deuda"],4),
                            "s_rec":    round(r.get(pred1,{}).get("score",0)*pesos["reciente"],4),
                            "s_patron": round(p.get(pred1,{}).get("score",0)*pesos["patron"],4),
                            "s_anti":   round(a.get(pred1,{}).get("score",0)*pesos["anti"],4),
                            "s_markov": round(m.get(pred1,{}).get("score",0)*pesos["secuencia"],4),
                            "s_ciclo":  round(ce.get(pred1,{}).get("score",0)*0.15,4),
                            "s_fecha":  round(pfe.get(pred1,{}).get("score",0)*0.12,4),
                            "s_final":  round(sc.get(pred1,0),6),
                            "p_deuda":pesos["deuda"],"p_rec":pesos["reciente"],
                            "p_patron":pesos["patron"],"p_anti":pesos["anti"],
                            "p_markov":pesos["secuencia"],
                        })
                        señales_insertadas += 1
                    except Exception:
                        await db.rollback()

                if (insertados+señales_insertadas) % 200 == 0:
                    await db.commit()

            except Exception as _err:
                await db.rollback()
                continue

        await db.commit()
        ef1 = round(aciertos1/max(insertados,1)*100,1) if insertados > 0 else 0
        ef3 = round(aciertos3/max(insertados,1)*100,1) if insertados > 0 else 0

        return {
            "status":                 "success",
            "procesados":             insertados,
            "señales_insertadas":     señales_insertadas,
            "omitidos_ya_existian":   omitidos,
            "aciertos_top1":          aciertos1,
            "aciertos_top3":          aciertos3,
            "efectividad_top1":       ef1,
            "efectividad_top3":       ef3,
            "message": (
                f"✅ Retroactivo {fecha_desde}→{fecha_hasta}: "
                f"{insertados} nuevas | {señales_insertadas} señales | "
                f"{omitidos} omitidos"
            ),
        }
    except Exception as e:
        await db.rollback()
        return {"status":"error","message":str(e)}


# ══════════════════════════════════════════════════════
# APRENDIZAJE POR SORTEO — PASO 2
# Micro-ajuste de pesos después de cada resultado real
# Tasa pequeña (0.02) para no sobreajustar
# Se ejecuta automáticamente tras cada sorteo capturado
# ══════════════════════════════════════════════════════

def _micro_ajuste_pesos(pesos: dict, señal_ganadora: str,
                         acerto_top1: bool, acerto_top3: bool,
                         tasa: float = 0.02) -> dict:
    """
    Ajusta pesos de las señales en base al resultado de UN sorteo.
    - Acierto top1  → refuerza señal ganadora +tasa, penaliza resto
    - Acierto top3  → refuerzo moderado (+tasa*0.5)
    - Fallo total   → penaliza señal dominante (-tasa*0.5)
    Renormaliza para que sumen 1.0. Límites: min 0.05, max 0.60.
    """
    nuevos = pesos.copy()

    if acerto_top1:
        factor       =  tasa
        penalizacion =  tasa / max(len(pesos) - 1, 1)
        signo_otros  = -1
    elif acerto_top3:
        factor       =  tasa * 0.5
        penalizacion =  tasa * 0.5 / max(len(pesos) - 1, 1)
        signo_otros  = -1
    else:
        factor       = -tasa * 0.5
        penalizacion =  tasa * 0.5 / max(len(pesos) - 1, 1)
        signo_otros  = +1   # fallo → las otras señales suben un poco

    for señal in nuevos:
        if señal == señal_ganadora:
            nuevos[señal] = max(0.05, min(0.60, nuevos[señal] + factor))
        else:
            nuevos[señal] = max(0.05, min(0.60, nuevos[señal] + signo_otros * penalizacion))

    total = sum(nuevos.values()) or 1.0
    return {k: round(v / total, 4) for k, v in nuevos.items()}


async def aprender_sorteo(db, fecha, hora: str, animal_real: str) -> dict:
    """
    Micro-ajuste de pesos para UN sorteo específico.
    Flujo:
      1. Verifica que no fue procesado ya
      2. Lee predicción guardada en auditoria_ia
      3. Determina acierto top1 / top3
      4. Identifica señal dominante en auditoria_señales
      5. Aplica micro-ajuste dinámico (tasa varía por confianza)
      6. Guarda en aprendizaje_sorteo y actualiza motor_pesos_hora
    """
    import json
    try:
        animal_real = _normalizar(animal_real)
        if not animal_real:
            return {"status": "error", "message": "animal_real vacío"}

        # 1. ¿Ya procesado?
        res_ya = await db.execute(text("""
            SELECT id FROM aprendizaje_sorteo
            WHERE fecha = :f AND hora = :h
        """), {"f": fecha, "h": hora})
        if res_ya.fetchone():
            return {"status": "skip", "message": f"Ya aprendido: {fecha} {hora}"}

        # 2. Predicción guardada
        res_pred = await db.execute(text("""
            SELECT prediccion_1, prediccion_2, prediccion_3, confianza_pct
            FROM auditoria_ia
            WHERE fecha = :f AND hora = :h
            LIMIT 1
        """), {"f": fecha, "h": hora})
        row_pred = res_pred.fetchone()

        pred1 = _normalizar(row_pred[0]) if row_pred and row_pred[0] else None
        pred2 = _normalizar(row_pred[1]) if row_pred and row_pred[1] else None
        pred3 = _normalizar(row_pred[2]) if row_pred and row_pred[2] else None

        acerto_top1 = (pred1 == animal_real) if pred1 else False
        acerto_top3 = animal_real in [x for x in [pred1, pred2, pred3] if x]

        # 3. Señal dominante
        señal_dominante = "secuencia"
        mapa_señal_a_peso = {
            "deuda": "deuda", "reciente": "reciente", "patron": "patron",
            "anti": "anti",   "secuencia": "secuencia",
            "ciclo": "deuda", "patron_fecha": "patron",
            "intraday": "secuencia", "pares": "secuencia",
        }
        try:
            res_s = await db.execute(text("""
                SELECT score_deuda, score_reciente, score_patron_dia,
                       score_anti_racha, score_markov, score_ciclo_exacto,
                       score_patron_fecha,
                       COALESCE(score_intraday, 0),
                       COALESCE(score_pares, 0)
                FROM auditoria_señales
                WHERE fecha = :f AND hora = :h
                LIMIT 1
            """), {"f": fecha, "h": hora})
            row_s = res_s.fetchone()
            if row_s:
                nombres = ["deuda","reciente","patron","anti","secuencia",
                           "ciclo","patron_fecha","intraday","pares"]
                sc_map = {nombres[i]: float(row_s[i] or 0) for i in range(len(nombres))}
                mejor_raw = max(sc_map, key=sc_map.get)
                señal_dominante = mapa_señal_a_peso.get(mejor_raw, "secuencia")
        except Exception:
            pass

        # 4. Pesos actuales para esta hora
        pesos_antes = await obtener_pesos_para_hora(db, hora)

        # 5. Tasa dinámica según confianza
        confianza = float(row_pred[3] or 0) if row_pred else 0
        if confianza >= 50 and not acerto_top3:
            tasa = 0.03   # alta confianza + fallo → aprender más
        elif confianza < 20:
            tasa = 0.01   # baja confianza → aprender despacio
        else:
            tasa = 0.02

        # 6. Micro-ajuste
        pesos_despues = _micro_ajuste_pesos(
            pesos_antes, señal_dominante, acerto_top1, acerto_top3, tasa
        )

        # 7. Generación actual
        res_gen = await db.execute(text(
            "SELECT COALESCE(MAX(generacion), 1) FROM motor_pesos"
        ))
        generacion = res_gen.scalar() or 1

       # 8. Guardar registro
        await db.execute(text("""
            INSERT INTO aprendizaje_sorteo (
                fecha, hora, animal_real,
                animal_pred1, animal_pred2, animal_pred3,
                acerto_top1, acerto_top3,
                señal_dominante, peso_antes, peso_despues,
                tasa_aprendizaje, generacion
            ) VALUES (
                :f, :h, :real, :p1, :p2, :p3,
                :at1, :at3, :señal,
                cast(:pa as jsonb), cast(:pd as jsonb), :tasa, :gen
            )
            ON CONFLICT (fecha, hora) DO UPDATE SET
                animal_real      = EXCLUDED.animal_real,
                acerto_top1      = EXCLUDED.acerto_top1,
                acerto_top3      = EXCLUDED.acerto_top3,
                señal_dominante  = EXCLUDED.señal_dominante,
                peso_antes       = EXCLUDED.peso_antes,
                peso_despues     = EXCLUDED.peso_despues,
                tasa_aprendizaje = EXCLUDED.tasa_aprendizaje
        """), {
            "f": fecha, "h": hora, "real": animal_real,
            "p1": pred1, "p2": pred2, "p3": pred3,
            "at1": acerto_top1, "at3": acerto_top3,
            "señal": señal_dominante,
            "pa": json.dumps(pesos_antes),
            "pd": json.dumps(pesos_despues),
            "tasa": tasa, "gen": generacion,
        })
      
        # 9. Actualizar motor_pesos_hora
        await db.execute(text("""
            INSERT INTO motor_pesos_hora
                (hora, generacion, peso_decay, peso_markov, peso_gap, peso_reciente,
                 efectividad, total_evaluados, aciertos_top3)
            VALUES (:hora, :gen, :anti, :markov, :deuda, :rec, 0, 0, 0)
            ON CONFLICT (hora, generacion) DO UPDATE SET
                peso_decay    = EXCLUDED.peso_decay,
                peso_markov   = EXCLUDED.peso_markov,
                peso_gap      = EXCLUDED.peso_gap,
                peso_reciente = EXCLUDED.peso_reciente,
                fecha         = NOW()
        """), {
            "hora":   hora, "gen": generacion,
            "anti":   pesos_despues.get("anti",      0.22),
            "markov": pesos_despues.get("secuencia", 0.24),
            "deuda":  pesos_despues.get("deuda",     0.20),
            "rec":    pesos_despues.get("reciente",  0.20),
        })
        await db.commit()

        return {
            "status":          "success",
            "fecha":           str(fecha),
            "hora":            hora,
            "animal_real":     animal_real,
            "pred_top3":       [pred1, pred2, pred3],
            "acerto_top1":     acerto_top1,
            "acerto_top3":     acerto_top3,
            "señal_dominante": señal_dominante,
            "tasa_usada":      tasa,
            "pesos_antes":     pesos_antes,
            "pesos_despues":   pesos_despues,
            "delta": {k: round(pesos_despues.get(k,0) - pesos_antes.get(k,0), 4)
                      for k in pesos_antes},
            "message": (
                f"✅ {hora} | Real: {animal_real} | "
                f"{'TOP1 ✅' if acerto_top1 else 'TOP3 ✅' if acerto_top3 else 'FALLO ❌'} | "
                f"Señal: {señal_dominante} | Δ={tasa}"
            )
        }
    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


async def aprender_ultimos_n(db, n: int = 50) -> dict:
    """
    Procesa los últimos N sorteos con resultado real
    que aún no tienen micro-ajuste registrado.
    """
    try:
        res = await db.execute(text("""
            SELECT h.fecha, h.hora, h.animalito
            FROM historico h
            WHERE h.loteria = 'Lotto Activo'
              AND NOT EXISTS (
                  SELECT 1 FROM aprendizaje_sorteo a
                  WHERE a.fecha = h.fecha AND a.hora = h.hora
              )
              AND EXISTS (
                  SELECT 1 FROM auditoria_ia ai
                  WHERE ai.fecha = h.fecha AND ai.hora = h.hora
                    AND ai.prediccion_1 IS NOT NULL
              )
            ORDER BY h.fecha DESC, h.hora DESC
            LIMIT :n
        """), {"n": n})
        sorteos = res.fetchall()

        if not sorteos:
            return {"status": "ok", "procesados": 0,
                    "message": "✅ Sistema al día — nada nuevo que aprender"}

        procesados = aciertos1 = aciertos3 = fallos = 0
        log = []
        for row in sorteos:
            fecha_s, hora_s, animal_s = row
            r = await aprender_sorteo(db, fecha_s, hora_s, animal_s)
            print(f"DEBUG: {fecha_s} {hora_s} → {r}")  # ← AGREGA ESTA LÍNEA
            if r.get("status") == "success":
                procesados += 1
                if r.get("acerto_top1"):     aciertos1 += 1
                elif r.get("acerto_top3"):   aciertos3 += 1
                else:                        fallos    += 1
                log.append({
                    "fecha":  str(fecha_s), "hora": hora_s,
                    "real":   animal_s,
                    "result": "top1" if r.get("acerto_top1")
                              else "top3" if r.get("acerto_top3") else "fallo",
                    "señal":  r.get("señal_dominante"),
                })

        ef_micro = round((aciertos1 + aciertos3) / max(procesados, 1) * 100, 1)
        return {
            "status":      "success",
            "procesados":  procesados,
            "aciertos_t1": aciertos1,
            "aciertos_t3": aciertos3,
            "fallos":      fallos,
            "ef_micro":    ef_micro,
            "log":         log[-10:],
            "message": (
                f"✅ Micro-aprendizaje: {procesados} sorteos | "
                f"Top1:{aciertos1} Top3:{aciertos3} Fallos:{fallos} | "
                f"Ef:{ef_micro}%"
            )
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


async def obtener_historial_aprendizaje(db, limit: int = 30) -> dict:
    """Historial de micro-ajustes para auditoría y dashboard."""
    try:
        res = await db.execute(text("""
            SELECT fecha, hora, animal_real, animal_pred1,
                   acerto_top1, acerto_top3, señal_dominante,
                   tasa_aprendizaje, peso_antes, peso_despues, creado
            FROM aprendizaje_sorteo
            ORDER BY fecha DESC, hora DESC
            LIMIT :lim
        """), {"lim": limit})

        registros = []
        for r in res.fetchall():
            peso_a = r[8] if isinstance(r[8], dict) else {}
            peso_d = r[9] if isinstance(r[9], dict) else {}
            registros.append({
                "fecha":           str(r[0]),
                "hora":            r[1],
                "animal_real":     r[2],
                "pred1":           r[3],
                "acerto_top1":     bool(r[4]),
                "acerto_top3":     bool(r[5]),
                "señal_dominante": r[6],
                "tasa":            float(r[7] or 0.02),
                "delta_secuencia": round(
                    float(peso_d.get("secuencia",0)) - float(peso_a.get("secuencia",0)), 4),
                "delta_deuda": round(
                    float(peso_d.get("deuda",0)) - float(peso_a.get("deuda",0)), 4),
            })

        res_s = await db.execute(text("""
            SELECT COUNT(*),
                   SUM(CASE WHEN acerto_top1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN acerto_top3 THEN 1 ELSE 0 END),
                   AVG(tasa_aprendizaje), MAX(creado)
            FROM aprendizaje_sorteo
        """))
        s = res_s.fetchone()
        total_ap = int(s[0] or 0)
        ef_ap    = round(int(s[2] or 0) / max(total_ap, 1) * 100, 1)

        return {
            "total_aprendizajes": total_ap,
            "ef_top3_aprendido":  ef_ap,
            "tasa_promedio":      round(float(s[3] or 0.02), 4),
            "ultimo_aprendizaje": str(s[4]) if s[4] else None,
            "registros":          registros,
        }
    except Exception as e:
        return {"error": str(e), "registros": []}
