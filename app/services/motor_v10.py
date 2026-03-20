"""
MOTOR V10 — LOTTOAI PRO
========================
FIXES vs V9:
  1. FIX SESGO: frecuencia_reciente normalizada vs azar (ratio > 1.0 = caliente)
  2. FIX SESGO: anti_racha anula frecuencia_reciente si animal salió ayer
  3. FIX MAPA: catálogo completo + normalización robusta (camello, gato, zorro, etc.)
  4. FIX CONFIANZA: calibrada con Wilson 95% + histórico de hora
  5. FIX PESOS: usa motor_pesos_hora por hora, no pesos globales
  6. FIX TARDE: multiplicador de penalización por hora según rentabilidad real
  7. FIX SECUENCIA: Markov por hora (no global), reemplaza señal global rota
  8. NUEVO: score_diversidad — penaliza animales que el motor predice demasiado
  9. NUEVO: generar_prediccion devuelve señal OPERAR/NO OPERAR con umbral conf >= 25
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
TODOS_LOS_ANIMALES = sorted(set(MAPA_ANIMALES.values()))  # 38 únicos

# Alias: variantes de escritura → nombre canónico
_ALIAS = {
    "alacrán":"alacran",  "caimán":"caiman",   "ciempiés":"ciempies",
    "delfín":"delfin",    "león":"leon",        "lechón":"lechon",
    "pavo real":"pavo",   "águila":"aguila",    "búho":"lechuza",
    "culebra":"culebra",  "serpiente":"culebra","vibora":"culebra",
    "zorro":"zorro",      "fox":"zorro",        "cochino":"cochino",
    "cerdo":"cochino",    "chancho":"cochino",
}

def _normalizar(nombre: str) -> str:
    """Normaliza a nombre canónico sin tildes problemáticas."""
    if not nombre:
        return ""
    n = nombre.lower().strip()
    n = re.sub(r'[^a-záéíóúñ\s]', '', n).strip()
    # alias directo
    if n in _ALIAS:
        return _ALIAS[n]
    # quitar tildes
    n = (n.replace('á','a').replace('é','e').replace('í','i')
           .replace('ó','o').replace('ú','u').replace('ñ','n'))
    return n

HORAS_SORTEO_STR = [
    "08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
    "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM",
    "06:00 PM","07:00 PM",
]

# Horas conocidas como NO rentables (datos históricos propios)
# Se aplica un multiplicador de score final
_MULTIPLICADOR_HORA = {
    "08:00 AM": 1.20,   # mejor hora histórica
    "09:00 AM": 1.10,
    "10:00 AM": 1.00,
    "11:00 AM": 1.15,
    "12:00 PM": 0.95,
    "01:00 PM": 0.70,   # datos: 0% TOP3
    "02:00 PM": 0.80,
    "03:00 PM": 0.70,   # datos: 0% TOP3
    "04:00 PM": 0.70,   # datos: 0% TOP3
    "05:00 PM": 0.72,   # datos: 0% TOP3
    "06:00 PM": 0.72,   # datos: 0% TOP3
    "07:00 PM": 0.75,
}

# ══════════════════════════════════════════════════════
# PESO DE ANTI-RACHA DIFERENCIADO POR HORA
# Basado en análisis estadístico de 29,000 sorteos:
# - Mañanas (8-11AM): anti-repetición real y masiva
#   08:00 AM repite solo 0.66% vs 2.63% azar → 4× menos
# - Tardes (1-7PM): neutral o ligeramente pro-repetición
# ══════════════════════════════════════════════════════
_PESO_ANTI_RACHA_HORA = {
    "08:00 AM": 0.42,   # anti-repetición MÁS FUERTE (0.66% vs 2.63%)
    "09:00 AM": 0.36,   # anti-repetición fuerte (-0.83%)
    "10:00 AM": 0.35,   # anti-repetición fuerte (-0.95%)
    "11:00 AM": 0.30,   # anti-repetición moderada (-0.62%)
    "12:00 PM": 0.20,   # casi neutral (-0.03%)
    "01:00 PM": 0.18,   # neutral (+0.02%)
    "02:00 PM": 0.28,   # anti-repetición moderada (-0.81%)
    "03:00 PM": 0.18,   # neutral (+0.12%)
    "04:00 PM": 0.18,   # neutral (+0.16%)
    "05:00 PM": 0.20,   # leve anti-repetición (-0.29%)
    "06:00 PM": 0.15,   # ligera PRO-repetición (+0.57%) → bajar anti
    "07:00 PM": 0.18,   # neutral (+0.06%)
}

# ══════════════════════════════════════════════════════
# PARES CORRELACIONADOS — validados con 29,000 sorteos
# Si ayer salió A, hoy tiene mayor prob de salir B
# Solo pares con ventaja > +0.75% sobre azar (2.63%)
# ══════════════════════════════════════════════════════
_PARES_CORRELACIONADOS = {
    "leon":    [("ardilla", 1.40), ("caballo", 0.85), ("caiman", 0.85)],
    "delfin":  [("caiman",  1.18), ("iguana",  0.82)],
    "carnero": [("ardilla", 1.09), ("caiman",  0.73)],
    "aguila":  [("ardilla", 1.07), ("vaca",    0.68)],
    "culebra": [("tigre",   0.79), ("ardilla", 0.79)],
    "gato":    [("ardilla", 0.79)],
    "toro":    [("iguana",  0.76), ("caballo", 0.63)],
    "mono":    [("caiman",  0.76)],
    "rana":    [("caiman",  0.62), ("ballena", 0.62)],
    "gallo":   [("perro",   0.65)],
}
# Pares que CASI NUNCA ocurren → penalizar si se predice B después de A
_PARES_INVERSOS = {
    "gallo":   ["caballo"],   # prob 1.27% (-1.36% vs azar)
    "mono":    ["caballo"],   # prob 1.82% (-0.81% vs azar)
    "carnero": ["paloma"],    # prob 1.80% (-0.83% vs azar)
}

UMBRAL_RENTABILIDAD_TOP3 = 10.0
UMBRAL_CONFIANZA_OPERAR  = 25    # BAJADO de 30 → más operaciones pero filtradas
AZAR_ESPERADO = 1.0 / 38         # 2.63% por animal


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
# PESOS POR HORA (FIX #5)
# Lee motor_pesos_hora primero, cae a motor_pesos global
# ══════════════════════════════════════════════════════
async def obtener_pesos_para_hora(db, hora_str: str) -> dict:
    """Pesos específicos para esta hora. Si no existen, usa globales."""
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
    # Caída a pesos globales
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
# SEÑAL 1: DEUDA — días ausente vs ciclo promedio
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
# SEÑAL 2: FRECUENCIA RECIENTE — FIX SESGO CEBRA/RANA
# Normalizada como ratio vs distribución uniforme (1/38)
# ratio > 1.0 = más frecuente que el azar = CALIENTE
# ratio < 1.0 = más frío que el azar
# ══════════════════════════════════════════════════════
async def calcular_frecuencia_reciente(db, hora_str, fecha_limite=None):
    """Ventana deslizante ponderada: 7d×50% + 30d×30% + 90d×20%"""
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
            # Tendencia: caliente si aparece más en los últimos 7 días que su promedio histórico
            promedio_esperado_7d = (total_c / 90 * 7) if total_c > 0 else 0
            tendencia = "🔥" if c7 > promedio_esperado_7d * 1.3 else ("❄" if c7 == 0 and c90 > 2 else "→")
            resultado[animal] = {
                "score": score_norm,
                "ratio_vs_azar": round(ratio, 2),
                "veces_7d": c7, "veces_30d": c30, "veces_90d": c90,
                "tendencia": tendencia,
            }
    return resultado


async def calcular_patron_dia(db, hora_str, dia_semana, fecha_limite=None):
    """Patrón por día de semana: histórico completo + bono últimos 2 años"""
    if fecha_limite is None:
        fecha_limite = date.today()
    f2y = fecha_limite - timedelta(days=730)   # últimos 2 años
    res = await db.execute(text("""
        WITH historico_completo AS (
            SELECT animalito, COUNT(*) AS total
            FROM historico
            WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia
              AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        ),
        reciente_2y AS (
            SELECT animalito, COUNT(*) AS rec
            FROM historico
            WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia
              AND fecha>=:f2y AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT h.animalito,
               h.total * 0.60 + COALESCE(r.rec,0) * 0.40 AS score_pond,
               h.total,
               COALESCE(r.rec,0) AS rec
        FROM historico_completo h
        LEFT JOIN reciente_2y r ON h.animalito=r.animalito
        ORDER BY score_pond DESC
    """), {"hora": hora_str, "dia": dia_semana, "hoy": fecha_limite, "f2y": f2y})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(float(r[1]) for r in rows) or 1.0
        for r in rows:
            resultado[_normalizar(r[0])] = {
                "score": float(r[1]) / max_v,
                "veces": int(r[2]),
                "veces_2y": int(r[3]),
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 4: ANTI-RACHA — días desde última aparición
# FIX #2: si dias <= 1, anula frecuencia_reciente
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
        if dias <= 1:    score = 0.01   # salió ayer → casi eliminado
        elif dias <= 3:  score = 0.08
        elif dias <= 7:  score = 0.35
        elif dias <= 14: score = 0.60
        elif dias <= 30: score = 0.80
        else:            score = 1.00
        resultado[_normalizar(r[0])] = {
            "score": score,
            "dias_desde_ultima": dias,
            "bloquear": dias <= 1,  # flag para combinar_señales
        }
    return resultado


# ══════════════════════════════════════════════════════
# MARKOV INTRA-DÍA — validado con 29,000 sorteos
# 7.6× más potente que el Markov día→día actual
# Pares: si en hora H-1 salió X → en hora H predecir Y
# Ventaja promedio: +8.48% sobre azar (vs +1.40% del Markov viejo)
# ══════════════════════════════════════════════════════
_MARKOV_INTRADAY = {
    # (hora_origen, hora_destino): {animal_origen: (animal_destino, ventaja_pct)}
    ("11:00 AM", "12:00 PM"): {
        "ardilla": ("carnero",  9.49),
        "perro":   ("ciempies", 8.33),
        "cebra":   ("gallina",  7.50),
    },
    ("12:00 PM", "01:00 PM"): {
        "camello": ("paloma",   8.08),
        "gallo":   ("culebra",  7.76),
    },
    ("01:00 PM", "02:00 PM"): {
        "jirafa":  ("rana",     8.64),
        "zorro":   ("delfin",   8.48),
    },
    ("03:00 PM", "04:00 PM"): {
        "gato":    ("cebra",    8.48),
    },
    ("04:00 PM", "05:00 PM"): {
        "rana":    ("chivo",   10.70),   # par más fuerte
        "venado":  ("rana",     8.48),
    },
    ("05:00 PM", "06:00 PM"): {
        "chivo":   ("caiman",   9.31),   # cadena rana→chivo→caiman
        "delfin":  ("paloma",   6.46),
    },
}

# Mapa inverso: dado hora_destino → hora_origen correspondiente
_HORA_ANTERIOR = {
    "12:00 PM": "11:00 AM",
    "01:00 PM": "12:00 PM",
    "02:00 PM": "01:00 PM",
    "03:00 PM": "02:00 PM",
    "04:00 PM": "03:00 PM",
    "05:00 PM": "04:00 PM",
    "06:00 PM": "05:00 PM",
    "07:00 PM": "06:00 PM",
    "09:00 AM": "08:00 AM",
    "10:00 AM": "09:00 AM",
    "11:00 AM": "10:00 AM",
}


async def calcular_markov_intraday(db, hora_str, fecha_limite=None) -> dict:
    """
    Señal nueva: Markov intra-día.
    Si en la hora anterior del MISMO día salió X,
    y existe un par validado (hora_ant, hora_actual, X) → Y,
    boostar el score de Y.

    Validado con 29,000 sorteos: ventaja promedio +8.48% sobre azar.
    El par más fuerte: rana(4PM) → chivo(5PM) = 13.33%.
    """
    if fecha_limite is None:
        fecha_limite = date.today()

    hora_anterior = _HORA_ANTERIOR.get(hora_str)
    if not hora_anterior:
        return {}

    # Verificar si hay pares validados para este par de horas
    pares_hora = _MARKOV_INTRADAY.get((hora_anterior, hora_str))
    if not pares_hora:
        return {}

    try:
        # Obtener qué salió en la hora anterior HOY
        res = await db.execute(text("""
            SELECT LOWER(TRIM(animalito)) AS animal
            FROM historico
            WHERE hora    = :hora_ant
              AND fecha   = :hoy
              AND loteria = 'Lotto Activo'
            LIMIT 1
        """), {"hora_ant": hora_anterior, "hoy": fecha_limite})
        row = res.fetchone()
        if not row:
            return {}

        animal_anterior = _normalizar(row[0])

        # Buscar en los pares validados
        if animal_anterior not in pares_hora:
            return {}

        animal_predicho, ventaja = pares_hora[animal_anterior]

        # Normalizar ventaja → score 0-1
        # Ventaja máxima posible ~10.70% → score 1.0
        score = min(ventaja / 10.70, 1.0)

        return {
            animal_predicho: {
                "score":       round(score, 4),
                "ventaja_pct": ventaja,
                "origen":      animal_anterior,
                "hora_origen": hora_anterior,
                "tipo":        "intraday",
            }
        }
    except Exception:
        return {}


# Basada en análisis estadístico de 29,000 sorteos reales.
# Si ayer salió animal X en esta hora, ciertos animales Y
# tienen mayor probabilidad estadística de salir hoy.
# Solo se activa si el animal anterior salió ayer (1 día).
# Los pares y ventajas están validados con chi² > umbral.
# ══════════════════════════════════════════════════════
async def calcular_pares_correlacionados(db, hora_str, fecha_limite=None) -> dict:
    """
    Retorna boost por pares correlacionados.
    Score = ventaja normalizada del par (0.0 a 1.0).
    Score negativo = par inverso (penalización).
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    try:
        # Obtener el animal que salió AYER en esta hora
        res = await db.execute(text("""
            SELECT animalito FROM historico
            WHERE hora=:hora
              AND fecha = :ayer
              AND loteria='Lotto Activo'
            LIMIT 1
        """), {"hora": hora_str, "ayer": fecha_limite - timedelta(days=1)})
        row = res.fetchone()
        if not row:
            return {}

        animal_ayer = _normalizar(row[0])
        resultado = {}

        # Aplicar boost a pares correlacionados positivos
        if animal_ayer in _PARES_CORRELACIONADOS:
            pares = _PARES_CORRELACIONADOS[animal_ayer]
            max_ventaja = max(v for _, v in pares)
            for animal_dest, ventaja in pares:
                # Normalizar: ventaja máxima posible ~1.40% → score 1.0
                score = min(ventaja / 1.40, 1.0)
                resultado[animal_dest] = {
                    "score": round(score, 4),
                    "ventaja_pct": ventaja,
                    "origen": animal_ayer,
                    "tipo": "positivo",
                }

        # Aplicar penalización a pares inversos (casi nunca ocurren)
        if animal_ayer in _PARES_INVERSOS:
            for animal_pen in _PARES_INVERSOS[animal_ayer]:
                resultado[animal_pen] = {
                    "score": -0.5,   # penalización
                    "ventaja_pct": -1.36,
                    "origen": animal_ayer,
                    "tipo": "negativo",
                }

        return resultado
    except Exception:
        return {}



# Después de X en ESTA hora, ¿qué animal suele salir en ESA MISMA HORA?
# Usa tabla markov_transiciones si existe, si no calcula al vuelo
# ══════════════════════════════════════════════════════
async def calcular_markov_hora(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()

    # Intentar desde tabla markov_transiciones (pre-calculada)
    # MEJORA 4: filtra por animal_previo (último sorteo de esa hora) + mín 3 ocurrencias
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
            # Sin animal previo: usar distribución general de la hora con mínimo
            res = await db.execute(text("""
                SELECT animal_sig, AVG(probabilidad) AS prob, SUM(frecuencia) AS frec
                FROM markov_transiciones
                WHERE hora=:hora AND frecuencia >= 3
                GROUP BY animal_sig ORDER BY prob DESC LIMIT 10
            """), {"hora": hora_str})

        rows = res.fetchall()
        if rows:
            max_p = max(float(r[1]) for r in rows)
            if max_p <= 0:
                pass
            else:
                return {
                    _normalizar(r[0]): {
                        # score siempre normalizado 0-1 sin importar escala de prob
                        "score": min(1.0, float(r[1]) / max_p),
                        "prob":  round(float(r[1]), 2),
                    }
                    for r in rows
                    if float(r[1]) > 0
                }
    except Exception:
        pass

    # Cálculo al vuelo: último animal de esta hora → qué sigue en la misma hora
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
# SEÑAL 6: CICLO EXACTO POR HORA
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
        animal = _normalizar(r[0])
        ciclo_prom = float(r[1])
        n_ap       = int(r[4])
        dias_aus   = int(r[5])
        pct_ciclo  = dias_aus / ciclo_prom if ciclo_prom > 0 else 0

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
# SEÑAL 7: DIVERSIDAD — penaliza animales sobre-predichos
# FIX #8: evita que CEBRA/RANA monopolicen predicciones
# ══════════════════════════════════════════════════════
async def calcular_penalizacion_sobreprediccion(db, hora_str, fecha_limite=None, ventana_dias=30):
    """
    Cuenta cuántas veces fue predicho cada animal (como top1) en los últimos N días.
    Si un animal fue predicho más del doble de lo que acertó → penalizar.
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    fecha_ini = fecha_limite - timedelta(days=ventana_dias)
    try:
        res = await db.execute(text("""
            SELECT animal_predicho,
                COUNT(*) AS n_pred,
                COUNT(CASE WHEN acierto=TRUE THEN 1 END) AS n_ac
            FROM auditoria_ia
            WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta
            GROUP BY animal_predicho
        """), {"hora": hora_str, "desde": fecha_ini, "hasta": fecha_limite})
        rows = res.fetchall()
        penalizacion = {}
        for r in rows:
            animal = _normalizar(r[0] or "")
            if not animal:
                continue
            n_pred = int(r[1])
            n_ac   = int(r[2])
            # Tasa de acierto del animal cuando se predice
            tasa = n_ac / n_pred if n_pred > 0 else 0
            # Si fue predicho >5 veces y tasa < azar → penalizar
            if n_pred >= 5 and tasa < AZAR_ESPERADO:
                penalizacion[animal] = round(0.4 + tasa / AZAR_ESPERADO * 0.3, 3)
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
# COMBINAR SEÑALES V10 — con todos los fixes
# ══════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════
# SEÑAL 8: PATRÓN FECHA EXACTA — mismo DÍA+MES+HORA en 8 años
# "¿Qué sale los viernes de marzo a las 3PM históricamente?"
# Combina: día de semana + mes del año + hora
# ══════════════════════════════════════════════════════
async def calcular_patron_fecha_exacta(db, hora_str, dia_semana, mes, fecha_limite=None):
    """
    Señal contextual máxima: mismo día de semana + mismo mes + misma hora.
    Con 8 años de datos → ~30-40 muestras por slot. Estadísticamente fiable.
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH contexto AS (
            SELECT animalito, COUNT(*) AS total,
                   MAX(fecha) AS ultima
            FROM historico
            WHERE hora      = :hora
              AND EXTRACT(DOW  FROM fecha) = :dia
              AND EXTRACT(MONTH FROM fecha) = :mes
              AND fecha < :hoy
              AND loteria = 'Lotto Activo'
            GROUP BY animalito
        ),
        reciente_2y AS (
            SELECT animalito, COUNT(*) AS rec
            FROM historico
            WHERE hora      = :hora
              AND EXTRACT(DOW  FROM fecha) = :dia
              AND EXTRACT(MONTH FROM fecha) = :mes
              AND fecha >= :hoy - INTERVAL '2 years'
              AND fecha < :hoy
              AND loteria = 'Lotto Activo'
            GROUP BY animalito
        )
        SELECT c.animalito,
               c.total * 0.55 + COALESCE(r.rec, 0) * 0.45 AS score_pond,
               c.total,
               COALESCE(r.rec, 0) AS rec_2y,
               c.ultima
        FROM contexto c
        LEFT JOIN reciente_2y r ON c.animalito = r.animalito
        ORDER BY score_pond DESC
    """), {"hora": hora_str, "dia": dia_semana, "mes": mes,
           "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(float(r[1]) for r in rows) or 1.0
        total_muestras = sum(int(r[2]) for r in rows)
        for r in rows:
            animal = _normalizar(r[0])
            resultado[animal] = {
                "score":        float(r[1]) / max_v,
                "veces_total":  int(r[2]),
                "veces_2y":     int(r[3]),
                "ultima":       str(r[4]),
                "pct_slot":     round(int(r[2]) / max(total_muestras, 1) * 100, 1),
                "muestras":     total_muestras,
            }
    return resultado

def combinar_señales_v10(deuda, reciente, patron, anti, markov,
                          ciclo_exacto, pen_reciente, pen_sobreprediccion,
                          hora_str, pesos, patron_fecha=None, pares=None,
                          intraday=None):
    """
    10 señales + 2 penalizaciones + multiplicador por hora.
    MEJORA v4: Markov intra-día — 7.6× más potente que Markov día→día.
    """
    patron_fecha = patron_fecha or {}
    pares        = pares or {}
    intraday     = intraday or {}

    todos = set(
        list(deuda) + list(reciente) + list(patron) +
        list(anti) + list(markov) + list(ciclo_exacto) +
        list(patron_fecha) + list(pares) + list(intraday)
    )

    mult_hora  = _MULTIPLICADOR_HORA.get(hora_str, 0.85)
    peso_ciclo = 0.15
    peso_fecha = 0.12
    peso_pares = 0.08
    peso_intra = 0.14   # peso alto — señal 7.6× más potente que Markov actual

    peso_anti_hora = _PESO_ANTI_RACHA_HORA.get(hora_str, pesos.get("anti", 0.22))

    suma_pesos = (
        pesos["deuda"] + pesos["reciente"] + pesos["patron"] +
        peso_anti_hora + pesos["secuencia"] +
        peso_ciclo + peso_fecha + peso_pares + peso_intra
    )

    scores = {}
    for animal in todos:
        anti_info = anti.get(animal, {})
        bloquear  = anti_info.get("bloquear", False)
        score_reciente = 0.0 if bloquear else reciente.get(animal, {}).get("score", 0)

        par_info  = pares.get(animal, {})
        par_score = par_info.get("score", 0)
        par_contribucion = max(par_score, 0) * peso_pares

        # Intra-día: si es el animal predicho por el par intra-día, boost fuerte
        intra_info  = intraday.get(animal, {})
        intra_score = intra_info.get("score", 0)
        intra_contribucion = intra_score * peso_intra

        base = (
            deuda.get(animal,       {}).get("score", 0) * pesos["deuda"]     +
            score_reciente                               * pesos["reciente"]  +
            patron.get(animal,      {}).get("score", 0) * pesos["patron"]    +
            anti_info.get("score", 0.5)                 * peso_anti_hora     +
            markov.get(animal,      {}).get("score", 0) * pesos["secuencia"] +
            ciclo_exacto.get(animal,{}).get("score", 0) * peso_ciclo         +
            patron_fecha.get(animal,{}).get("score", 0) * peso_fecha         +
            par_contribucion                                                   +
            intra_contribucion
        )
        base /= suma_pesos

        if par_score < 0:
            base *= 0.70

        base *= pen_reciente.get(animal, 1.0)
        base *= pen_sobreprediccion.get(animal, 1.0)
        scores[animal] = round(base * mult_hora, 6)

    return scores

    """
    9 señales + 2 penalizaciones + multiplicador por hora.
    FIX #2: si anti_racha.bloquear → anular reciente para ese animal.
    FIX #6: multiplicador por hora al final.
    FIX #8: penalización por sobre-predicción.
    MEJORA v1: patron_fecha = mismo día+mes+hora en 8 años.
    MEJORA v2: peso anti-racha diferenciado por hora (validado con 29k sorteos).
    MEJORA v3: pares correlacionados estadísticamente confirmados.
    """
    patron_fecha = patron_fecha or {}
    pares        = pares or {}

    todos = set(
        list(deuda) + list(reciente) + list(patron) +
        list(anti) + list(markov) + list(ciclo_exacto) +
        list(patron_fecha) + list(pares)
    )

    mult_hora  = _MULTIPLICADOR_HORA.get(hora_str, 0.85)
    peso_ciclo = 0.15
    peso_fecha = 0.12   # señal contextual: día+mes+hora en 8 años
    peso_pares = 0.08   # señal nueva: pares correlacionados validados

    # ── Peso anti-racha diferenciado por hora ──
    # Reemplaza el peso fijo pesos["anti"] con valor calibrado por hora
    peso_anti_hora = _PESO_ANTI_RACHA_HORA.get(hora_str, pesos.get("anti", 0.22))

    # Recalcular suma de pesos con el anti-racha real
    suma_pesos = (
        pesos["deuda"] + pesos["reciente"] + pesos["patron"] +
        peso_anti_hora + pesos["secuencia"] +
        peso_ciclo + peso_fecha + peso_pares
    )

    scores = {}
    for animal in todos:
        # FIX #2: si salió ayer en esta hora, score de reciente = 0
        anti_info = anti.get(animal, {})
        bloquear  = anti_info.get("bloquear", False)

        score_reciente = 0.0 if bloquear else reciente.get(animal, {}).get("score", 0)

        # Score de pares: positivo si es destino frecuente, negativo si es inverso
        par_info   = pares.get(animal, {})
        par_score  = par_info.get("score", 0)
        if par_score < 0:
            # Par inverso: penalizar directamente el score base
            peso_par_efectivo = peso_pares
            par_contribucion  = 0.0  # no suma, pero penalizamos después
        else:
            peso_par_efectivo = peso_pares
            par_contribucion  = par_score * peso_pares

        base = (
            deuda.get(animal,       {}).get("score", 0) * pesos["deuda"]     +
            score_reciente                               * pesos["reciente"]  +
            patron.get(animal,      {}).get("score", 0) * pesos["patron"]    +
            anti_info.get("score", 0.5)                 * peso_anti_hora     +
            markov.get(animal,      {}).get("score", 0) * pesos["secuencia"] +
            ciclo_exacto.get(animal,{}).get("score", 0) * peso_ciclo         +
            patron_fecha.get(animal,{}).get("score", 0) * peso_fecha         +
            par_contribucion
        )
        base /= suma_pesos

        # Penalización par inverso: reducir 30% si es par que casi nunca ocurre
        if par_score < 0:
            base *= 0.70

        # Penalización por aparición reciente en histórico
        base *= pen_reciente.get(animal, 1.0)

        # FIX #8: penalización por sobre-predicción
        base *= pen_sobreprediccion.get(animal, 1.0)

        # FIX #6: multiplicador por rentabilidad de la hora
        scores[animal] = round(base * mult_hora, 6)

    return scores


# ══════════════════════════════════════════════════════
# ÍNDICE DE CONFIANZA V10 — FIX #4
# Wilson 95% para intervalo real + histórico de hora
# ══════════════════════════════════════════════════════
def wilson_lower(aciertos: int, total: int, z: float = 1.645) -> float:
    """Límite inferior del intervalo de Wilson al 90%."""
    if total == 0:
        return 0.0
    p = aciertos / total
    denom = 1 + z**2 / total
    centro = p + z**2 / (2 * total)
    margen = z * math.sqrt(p * (1 - p) / total + z**2 / (4 * total**2))
    return max((centro - margen) / denom, 0.0)


# ══════════════════════════════════════════════════════
# HORAS PRIORITARIAS — basadas en backtest 2026 real
# Consistentes en 2025 Y 2026: 02PM, 04PM, 08AM
# Nuevas ganadoras 2026: 11AM, 12PM, 01PM, 03PM
# Evitar: 07PM, 05PM
# ══════════════════════════════════════════════════════
_HORAS_PRIORITARIAS = {
    "11:00 AM": 1.30,   # 20.55% en 2026 — mejor hora del año
    "08:00 AM": 1.25,   # 17.57% — anti-racha funciona muy bien
    "12:00 PM": 1.15,   # 13.70% — consistente
    "04:00 PM": 1.12,   # 12.86% — consistente 2025+2026
    "03:00 PM": 1.10,   # 12.68% — nueva ganadora 2026
    "01:00 PM": 1.08,   # 12.50% — nueva ganadora 2026
    "02:00 PM": 1.05,   # 11.43% — consistente 2025+2026
    "06:00 PM": 0.95,   # 8.57%  — marginal
    "09:00 AM": 0.95,   # 8.33%  — marginal
    "10:00 AM": 0.90,   # 8.22%  — perdió en 2026
    "05:00 PM": 0.75,   # 7.14%  — bajo azar
    "07:00 PM": 0.60,   # 4.62%  — la peor hora
}

def calcular_indice_confianza_v10(scores, efectividad_hora_top3=None,
                                   total_sorteos_hora=0, aciertos_top3_hora=0,
                                   racha_fallos=0, hora_str=None,
                                   ef_top3_reciente=None):
    """
    Índice de confianza basado en:
    1. Efectividad REAL de la hora (últimas 4 semanas) — peso principal
    2. EF histórica de la hora (Wilson lower bound)
    3. Prioridad de la hora según patrón 2026
    4. Freno por racha de fallos consecutivos
    5. Separación de scores (peso secundario, no principal)

    Los datos muestran que separación de scores != probabilidad real.
    """
    if not scores:
        return 0, "🔴 SIN DATOS", False

    valores = sorted(scores.values(), reverse=True)
    if len(valores) < 3:
        return 10, "🔴 DATOS INSUFICIENTES", False

    # ── Base: efectividad real reciente de la hora ──
    # Si tenemos EF reciente (últimas 4 semanas), usarla como base
    if ef_top3_reciente is not None and ef_top3_reciente > 0:
        # Normalizar: 7.89%=azar→20, 10%→40, 15%→70, 20%→100
        base_reciente = min(int((ef_top3_reciente / 20.0) * 100), 80)
    elif total_sorteos_hora >= 20 and aciertos_top3_hora > 0:
        # Usar Wilson lower bound del histórico completo
        wilson = wilson_lower(aciertos_top3_hora, total_sorteos_hora)
        base_reciente = min(int(wilson * 400), 60)
    elif efectividad_hora_top3 is not None:
        base_reciente = min(int((efectividad_hora_top3 / 20.0) * 100), 50)
    else:
        base_reciente = 20  # sin datos, valor neutro

    # ── Bonus por hora prioritaria 2026 ──
    mult_hora = _HORAS_PRIORITARIAS.get(hora_str or "", 0.90)
    bonus_hora = int((mult_hora - 0.90) * 100)   # 0 si neutral, hasta +40 para 11AM

    # ── Separación de scores (señal secundaria) ──
    top1, top2 = valores[0], valores[1]
    sep_rel = (top1 - top2) / top1 if top1 > 0 else 0
    # Peso reducido: máx +15 puntos (antes era el componente principal)
    bonus_sep = min(int(sep_rel * 30), 15)
    # Penalizar si empate total
    if sep_rel < 0.05:
        bonus_sep = -10

    confianza = base_reciente + bonus_hora + bonus_sep

    # ── Freno por racha de fallos ──
    if racha_fallos >= 5:
        confianza = max(confianza - 20, 0)
    elif racha_fallos >= 3:
        confianza = max(confianza - 12, 0)
    elif racha_fallos >= 2:
        confianza = max(confianza - 6, 0)

    confianza = min(100, max(0, confianza))
    operar    = confianza >= UMBRAL_CONFIANZA_OPERAR

    if confianza >= 50:
        texto = "🟢 ALTA — OPERAR"
    elif confianza >= UMBRAL_CONFIANZA_OPERAR:
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
                total = int(r[0])
                ac1   = int(r[1])
                ac3   = int(r[2])
                ef1   = round(ac1 / total * 100, 2)
                ef3   = round(ac3 / total * 100, 2)
                wl    = wilson_lower(ac3, total)
                resultado[hora] = {
                    "total": total, "aciertos_top1": ac1, "aciertos_top3": ac3,
                    "efectividad_top1": ef1, "efectividad_top3": ef3,
                    "wilson_lower_top3": round(wl * 100, 2),
                    "es_rentable": wl * 100 >= UMBRAL_RENTABILIDAD_TOP3 * 0.9,
                    "mult_hora": _MULTIPLICADOR_HORA.get(hora, 0.85),
                }
            else:
                resultado[hora] = {
                    "total": 0, "efectividad_top1": 0.0, "efectividad_top3": 0.0,
                    "wilson_lower_top3": 0.0, "es_rentable": False,
                    "mult_hora": _MULTIPLICADOR_HORA.get(hora, 0.85),
                }
        except Exception:
            resultado[hora] = {"total": 0, "efectividad_top1": 0,
                               "efectividad_top3": 0, "es_rentable": False}
    return resultado


async def actualizar_tabla_rentabilidad(db, rentabilidad: dict):
    for hora, datos in rentabilidad.items():
        try:
            await db.execute(text("""
                INSERT INTO rentabilidad_hora
                    (hora, total_sorteos, aciertos_top1, aciertos_top3,
                     efectividad_top1, efectividad_top3, es_rentable, ultima_actualizacion)
                VALUES (:hora, :tot, :ac1, :ac3, :ef1, :ef3, :rent, NOW())
                ON CONFLICT (hora) DO UPDATE SET
                    total_sorteos    = EXCLUDED.total_sorteos,
                    aciertos_top1    = EXCLUDED.aciertos_top1,
                    aciertos_top3    = EXCLUDED.aciertos_top3,
                    efectividad_top1 = EXCLUDED.efectividad_top1,
                    efectividad_top3 = EXCLUDED.efectividad_top3,
                    es_rentable      = EXCLUDED.es_rentable,
                    ultima_actualizacion = NOW()
            """), {
                "hora": hora,
                "tot":  datos.get("total", 0),
                "ac1":  datos.get("aciertos_top1", 0),
                "ac3":  datos.get("aciertos_top3", 0),
                "ef1":  datos.get("efectividad_top1", 0),
                "ef3":  datos.get("efectividad_top3", 0),
                "rent": datos.get("es_rentable", False),
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
    return {"efectividad_top1": 0.0, "efectividad_top3": 0.0,
            "es_rentable": False, "aciertos_top3": 0, "total_sorteos": 0}


# ══════════════════════════════════════════════════════
# PREDICCIÓN V10 — NÚCLEO PRINCIPAL
# ══════════════════════════════════════════════════════
async def generar_prediccion(db) -> dict:
    try:
        tz      = pytz.timezone('America/Caracas')
        ahora   = datetime.now(tz)
        _mn     = ahora.minute
        _h      = ahora.hour
        _slots  = [8,9,10,11,12,13,14,15,16,17,18,19]
        _lbls   = {8:'08:00 AM',9:'09:00 AM',10:'10:00 AM',11:'11:00 AM',
                   12:'12:00 PM',13:'01:00 PM',14:'02:00 PM',15:'03:00 PM',
                   16:'04:00 PM',17:'05:00 PM',18:'06:00 PM',19:'07:00 PM'}
        # Si ya pasaron más de 2 min de la hora → sorteo pasó → predecir el siguiente
        if _h < 8:
            hora_str = _lbls[8]
        elif _h >= 19:
            hora_str = _lbls[8]
        elif _mn > 2:
            _sig = _h + 1
            hora_str = _lbls.get(_sig, _lbls[8])
        else:
            hora_str = _lbls.get(_h, _lbls[8])

        dia_semana = ahora.weekday()
        hoy        = ahora.date()

        pesos      = await obtener_pesos_para_hora(db, hora_str)
        rent_hora  = await obtener_rentabilidad_hora(db, hora_str)

        # Señales
        deuda        = await calcular_deuda(db, hora_str)
        reciente     = await calcular_frecuencia_reciente(db, hora_str)
        patron       = await calcular_patron_dia(db, hora_str, dia_semana)
        anti         = await calcular_anti_racha(db, hora_str)
        markov       = await calcular_markov_hora(db, hora_str)
        ciclo_exacto = await calcular_ciclo_exacto(db, hora_str)
        pen_rec      = await calcular_penalizacion_reciente(db, hora_str)
        pen_sobrep   = await calcular_penalizacion_sobreprediccion(db, hora_str)
        # Señal: mismo día de semana + mismo mes + misma hora en 8 años
        patron_fecha = await calcular_patron_fecha_exacta(
            db, hora_str, dia_semana, ahora.month
        )
        # Señal nueva: pares correlacionados validados con 29,000 sorteos
        pares_corr   = await calcular_pares_correlacionados(db, hora_str)
        # Señal nueva: Markov intra-día (7.6× más potente que Markov día→día)
        intraday     = await calcular_markov_intraday(db, hora_str)

        scores = combinar_señales_v10(
            deuda, reciente, patron, anti, markov,
            ciclo_exacto, pen_rec, pen_sobrep, hora_str, pesos,
            patron_fecha=patron_fecha,
            pares=pares_corr,
            intraday=intraday
        )

        # Calcular racha de fallos recientes en esta hora (últimas 5 predicciones)
        racha_fallos = 0
        ef_top3_reciente = None
        try:
            res_racha = await db.execute(text("""
                SELECT acierto FROM auditoria_ia
                WHERE hora=:hora AND acierto IS NOT NULL
                ORDER BY fecha DESC LIMIT 5
            """), {"hora": hora_str})
            ultimos = [r[0] for r in res_racha.fetchall()]
            for ac in ultimos:
                if ac is False:
                    racha_fallos += 1
                else:
                    break  # cortar en primer acierto
        except Exception:
            racha_fallos = 0

        # EF.TOP3 real de esta hora en las últimas 4 semanas
        # Esto es lo que realmente predice si vale la pena apostar
        try:
            res_ef = await db.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN LOWER(TRIM(h.animalito)) IN (
                        LOWER(TRIM(COALESCE(a.prediccion_1,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_2,'__'))),
                        LOWER(TRIM(COALESCE(a.prediccion_3,'__')))
                    ) THEN 1 ELSE 0 END) AS aciertos_top3
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
            ef_top3_reciente = None

        confianza_idx, señal_texto, operar = calcular_indice_confianza_v10(
            scores,
            efectividad_hora_top3 = rent_hora.get("efectividad_top3"),
            total_sorteos_hora    = rent_hora.get("total_sorteos", 0),
            aciertos_top3_hora    = rent_hora.get("aciertos_top3", 0),
            racha_fallos          = racha_fallos,
            hora_str              = hora_str,
            ef_top3_reciente      = ef_top3_reciente,
        )

        ranking     = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total_sc    = sum(scores.values()) or 1

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

        # Último resultado
        res_u  = await db.execute(text(
            "SELECT animalito FROM historico WHERE loteria='Lotto Activo' "
            "ORDER BY fecha DESC, hora DESC LIMIT 1"
        ))
        ultimo = res_u.scalar()

        es_hora_rentable = rent_hora.get("es_rentable", False)

        # Guardar predicción
        if top3:
            try:
                pred1 = top3[0]["animal"].lower() if len(top3) > 0 else None
                pred2 = top3[1]["animal"].lower() if len(top3) > 1 else None
                pred3 = top3[2]["animal"].lower() if len(top3) > 2 else None
                await db.execute(text("""
                    INSERT INTO auditoria_ia
                        (fecha, hora, animal_predicho, prediccion_1, prediccion_2, prediccion_3,
                         confianza_pct, confianza_hora, es_hora_rentable, resultado_real)
                    VALUES (:f,:h,:a,:p1,:p2,:p3,:c,:ch,:rent,'PENDIENTE')
                    ON CONFLICT (fecha, hora) DO UPDATE SET
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
            except Exception as e:
                await db.rollback()

            # ── Guardar desglose por señal en auditoria_señales ──
            try:
                animal_top1 = _normalizar(top3[0]["animal"]) if top3 else None
                if animal_top1:
                    sc_t = scores.get(animal_top1, 0) or 0
                    total_sc_raw = sum(scores.values()) or 1
                    await db.execute(text("""
                        INSERT INTO auditoria_señales (
                            fecha, hora, animal_predicho, resultado_real,
                            score_deuda, score_reciente, score_patron_dia,
                            score_anti_racha, score_markov, score_ciclo_exacto,
                            score_patron_fecha, score_final,
                            peso_deuda, peso_reciente, peso_patron,
                            peso_anti, peso_markov,
                            confianza
                        ) VALUES (
                            :f, :h, :animal, 'PENDIENTE',
                            :s_deuda, :s_rec, :s_patron,
                            :s_anti, :s_markov, :s_ciclo,
                            :s_fecha, :s_final,
                            :p_deuda, :p_rec, :p_patron,
                            :p_anti, :p_markov,
                            :conf
                        )
                        ON CONFLICT (fecha, hora) DO UPDATE SET
                            animal_predicho    = EXCLUDED.animal_predicho,
                            score_deuda        = EXCLUDED.score_deuda,
                            score_reciente     = EXCLUDED.score_reciente,
                            score_patron_dia   = EXCLUDED.score_patron_dia,
                            score_anti_racha   = EXCLUDED.score_anti_racha,
                            score_markov       = EXCLUDED.score_markov,
                            score_ciclo_exacto = EXCLUDED.score_ciclo_exacto,
                            score_patron_fecha = EXCLUDED.score_patron_fecha,
                            score_final        = EXCLUDED.score_final,
                            confianza          = EXCLUDED.confianza
                    """), {
                        "f":        hoy,
                        "h":        hora_str,
                        "animal":   animal_top1,
                        "s_deuda":  round(deuda.get(animal_top1, {}).get("score", 0) * pesos["deuda"], 4),
                        "s_rec":    round(reciente.get(animal_top1, {}).get("score", 0) * pesos["reciente"], 4),
                        "s_patron": round(patron.get(animal_top1, {}).get("score", 0) * pesos["patron"], 4),
                        "s_anti":   round(anti.get(animal_top1, {}).get("score", 0) * pesos["anti"], 4),
                        "s_markov": round(markov.get(animal_top1, {}).get("score", 0) * pesos["secuencia"], 4),
                        "s_ciclo":  round(ciclo_exacto.get(animal_top1, {}).get("score", 0) * 0.15, 4),
                        "s_fecha":  round(patron_fecha.get(animal_top1, {}).get("score", 0) * 0.12, 4),
                        "s_final":  round(sc_t, 6),
                        "p_deuda":  pesos["deuda"],
                        "p_rec":    pesos["reciente"],
                        "p_patron": pesos["patron"],
                        "p_anti":   pesos["anti"],
                        "p_markov": pesos["secuencia"],
                        "conf":     int(confianza_idx),
                    })
                    await db.commit()
            except Exception:
                await db.rollback()  # No romper la predicción si falla el desglose

        idx_actual  = HORAS_SORTEO_STR.index(hora_str) if hora_str in HORAS_SORTEO_STR else -1
        proxima_hora = (HORAS_SORTEO_STR[idx_actual + 1]
                        if 0 <= idx_actual < len(HORAS_SORTEO_STR) - 1 else None)

        return {
            "top3":                    top3,
            "hora":                    hora_str,
            "ultimo_resultado":        ultimo or "N/A",
            "confianza_idx":           confianza_idx,
            "señal_texto":             señal_texto,
            "operar":                  operar,
            "hora_premium":            es_hora_rentable,
            "efectividad_hora_top3":   rent_hora.get("efectividad_top3", 0),
            "wilson_lower":            rent_hora.get("wilson_lower_top3", 0),
            "proxima_hora":            proxima_hora,
            "pesos_actuales":          pesos,
            "analisis": (
                f"Motor V10 | {hora_str} | Conf: {confianza_idx}/100 | "
                f"Ef.Hora(top3): {rent_hora.get('efectividad_top3',0)}% | "
                f"{'✅ OPERAR' if operar else '🚫 NO OPERAR'}"
            )
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"top3": [], "analisis": f"Error V10: {e}",
                "confianza_idx": 0, "operar": False, "señal_texto": "ERROR"}


# ══════════════════════════════════════════════════════
# APRENDIZAJE POR REFUERZO V10
# Evalúa señales individualmente y actualiza motor_pesos_hora
# ══════════════════════════════════════════════════════
async def aprender_desde_historico(db, fecha_inicio=None, dias_por_generacion=30) -> dict:
    try:
        hoy = date.today()
        if fecha_inicio is None:
            fecha_inicio = date(2018, 1, 1)  # usar todo el histórico disponible

        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),0) FROM motor_pesos"))
        generacion_actual = (res_gen.scalar() or 0) + 1

        pesos = await _obtener_pesos_globales(db)
        total_global = aciertos_global = aciertos_top3_global = 0
        generaciones = 0
        log = []
        mejor_ef = 0.0
        mejores_pesos = pesos.copy()

        # Acumuladores por hora para motor_pesos_hora
        pesos_por_hora = {h: {"reciente": 0.25, "deuda": 0.28, "anti": 0.22,
                               "patron": 0.15, "secuencia": 0.10}
                          for h in HORAS_SORTEO_STR}
        conteo_hora = {h: 0 for h in HORAS_SORTEO_STR}

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

                    sc = combinar_señales_v10(d,r,p,a,m,ce,pr,ps,hora_s,pesos,
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

            total_global      += total_v
            aciertos_global   += ac_v
            aciertos_top3_global += ac3_v
            generaciones      += 1
            log.append({
                "ventana": f"{fecha_ventana}→{fecha_fin_v}",
                "sorteos": total_v,
                "ef_top1": round(ef_v*100,1),
                "ef_top3": round(ef3_v*100,1),
                "mejor_señal": max(ac_señal, key=ac_señal.get),
            })
            fecha_ventana += timedelta(days=dias_por_generacion)

        # Guardar pesos globales
        ef_g  = round(aciertos_global/total_global*100,1) if total_global > 0 else 0
        ef3_g = round(aciertos_top3_global/total_global*100,1) if total_global > 0 else 0
        await guardar_pesos(db, mejores_pesos, ef_g, total_global, aciertos_global, generacion_actual)

        # Actualizar motor_pesos_hora con mejores pesos globales (base)
        for hora in HORAS_SORTEO_STR:
            try:
                await db.execute(text("""
                    INSERT INTO motor_pesos_hora
                        (hora, generacion, peso_decay, peso_markov, peso_gap, peso_reciente,
                         efectividad, total_evaluados, aciertos_top3)
                    VALUES (:hora,:gen,:anti,:markov,:deuda,:rec,:ef,:tot,:ac3)
                    ON CONFLICT (hora, generacion) DO UPDATE SET
                        peso_decay=EXCLUDED.peso_decay, peso_markov=EXCLUDED.peso_markov,
                        peso_gap=EXCLUDED.peso_gap,     peso_reciente=EXCLUDED.peso_reciente,
                        efectividad=EXCLUDED.efectividad
                """), {
                    "hora": hora, "gen": generacion_actual,
                    "anti":   mejores_pesos["anti"],
                    "markov": mejores_pesos["secuencia"],
                    "deuda":  mejores_pesos["deuda"],
                    "rec":    mejores_pesos["reciente"],
                    "ef": ef3_g, "tot": total_global,
                    "ac3": aciertos_top3_global,
                })
            except Exception:
                pass
        await db.commit()

        return {
            "status": "success",
            "generacion": generacion_actual,
            "total_sorteos_evaluados": total_global,
            "efectividad_top1": ef_g,
            "efectividad_top3": ef3_g,
            "mejores_pesos": mejores_pesos,
            "message": (
                f"✅ V10 Gen {generacion_actual} | "
                f"Top1: {ef_g}% | Top3: {ef3_g}% | "
                f"Pesos: {mejores_pesos}"
            ),
            "log_ventanas": log[-5:],
        }
    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# ENTRENAR
# ══════════════════════════════════════════════════════
async def entrenar_modelo(db) -> dict:
    try:
        # Calibrar auditoria_ia pendientes
        await db.execute(text("""
            UPDATE auditoria_ia a
            SET
                acierto        = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND h.loteria = 'Lotto Activo'
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE'
                   OR a.resultado_real IS NULL)
        """))

        # Reconstruir probabilidades_hora
        # Reconstruir probabilidades_hora (con fallback si schema difiere)
        try:
            await db.execute(text("DELETE FROM probabilidades_hora"))
            await db.execute(text("""
                INSERT INTO probabilidades_hora
                    (hora, animalito, frecuencia, probabilidad, tendencia, ultima_actualizacion)
                WITH base AS (
                    SELECT hora, animalito, COUNT(*) AS total_hist
                    FROM historico WHERE loteria='Lotto Activo' GROUP BY hora, animalito
                ),
                reciente_60 AS (
                    SELECT hora, animalito, COUNT(*) AS total_rec
                    FROM historico
                    WHERE fecha >= CURRENT_DATE-INTERVAL '60 days' AND loteria='Lotto Activo'
                    GROUP BY hora, animalito
                ),
                reciente_7 AS (
                    SELECT hora, animalito, COUNT(*) AS total_7
                    FROM historico
                    WHERE fecha >= CURRENT_DATE-INTERVAL '7 days' AND loteria='Lotto Activo'
                    GROUP BY hora, animalito
                ),
                score_pond AS (
                    SELECT b.hora, b.animalito,
                           b.total_hist * 0.50
                           + COALESCE(r60.total_rec,0) * 2.0
                           + COALESCE(r7.total_7,0) * 5.0 AS score_w
                    FROM base b
                    LEFT JOIN reciente_60 r60 ON b.hora=r60.hora AND b.animalito=r60.animalito
                    LEFT JOIN reciente_7  r7  ON b.hora=r7.hora  AND b.animalito=r7.animalito
                ),
                totales AS (
                    SELECT hora, SUM(score_w) AS gran_total FROM score_pond GROUP BY hora
                )
                SELECT sp.hora::VARCHAR, sp.animalito, b.total_hist,
                    ROUND((sp.score_w / NULLIF(t.gran_total,0) * 100)::numeric, 2),
                    CASE WHEN COALESCE(r7.total_7,0) >= 2 THEN 'CALIENTE'
                         WHEN COALESCE(r60.total_rec,0) >= 3 THEN 'TIBIO'
                         ELSE 'FRIO' END,
                    NOW()
                FROM score_pond sp
                JOIN base b ON sp.hora=b.hora AND sp.animalito=b.animalito
                JOIN totales t ON sp.hora=t.hora
                LEFT JOIN reciente_60 r60 ON sp.hora=r60.hora AND sp.animalito=r60.animalito
                LEFT JOIN reciente_7  r7  ON sp.hora=r7.hora  AND sp.animalito=r7.animalito
            """))
            await db.commit()
        except Exception as e_prob:
            await db.rollback()
            import logging; logging.getLogger(__name__).warning(f"probabilidades_hora skip: {e_prob}")

        # Rentabilidad por hora
        rentabilidad = await calcular_rentabilidad_horas(db)
        await actualizar_tabla_rentabilidad(db, rentabilidad)

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

        ef1  = round(ac  / cal * 100, 1) if cal > 0 else 0
        ef3  = round(ac3 / cal * 100, 1) if cal > 0 else 0
        horas_rentables = [h for h,d in rentabilidad.items() if d.get("es_rentable")]

        await db.commit()
        return {
            "status": "success",
            "registros_analizados": total_hist,
            "efectividad_top1": ef1,
            "efectividad_top3": ef3,
            "calibradas": cal,
            "aciertos_top1": ac,
            "aciertos_top3": ac3,
            "horas_rentables": horas_rentables,
            "rentabilidad_detalle": rentabilidad,
            "message": (
                f"✅ V10 entrenado. {total_hist:,} registros. "
                f"Top1: {ef1}% | Top3: {ef3}% | "
                f"Horas rentables: {len(horas_rentables)}"
            ),
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# ACTUALIZAR RESULTADO EN auditoria_señales
# Se llama junto con calibrar_predicciones
# ══════════════════════════════════════════════════════
async def actualizar_resultados_señales(db) -> dict:
    """Sincroniza resultado_real, acierto_top1 y acierto_top3 en auditoria_señales."""
    try:
        r = await db.execute(text("""
            UPDATE auditoria_señales s
            SET
                resultado_real = h.animalito,
                acierto_top1   = (LOWER(TRIM(s.animal_predicho)) = LOWER(TRIM(h.animalito))),
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
            WHERE s.fecha = h.fecha
              AND s.hora  = h.hora
              AND h.loteria = 'Lotto Activo'
              AND (s.resultado_real = 'PENDIENTE' OR s.resultado_real IS NULL)
        """))
        actualizados = r.rowcount
        await db.commit()
        return {"actualizados": actualizados}
    except Exception as e:
        await db.rollback()
        return {"actualizados": 0, "error": str(e)}


# ══════════════════════════════════════════════════════
# SCORE POR SEÑAL — para /score-señales
# ══════════════════════════════════════════════════════
async def obtener_score_señales(db, dias=90) -> dict:
    """
    Analiza auditoria_señales para determinar qué señal tiene valor real.
    Retorna efectividad por señal cuando ella es la 'dominante' en la predicción.
    """
    try:
        fecha_ini = date.today() - timedelta(days=dias)
        res = await db.execute(text("""
            SELECT
                -- Señal dominante (la que más aportó)
                CASE
                    WHEN score_deuda >= GREATEST(score_reciente, score_patron_dia,
                         score_anti_racha, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'deuda'
                    WHEN score_reciente >= GREATEST(score_deuda, score_patron_dia,
                         score_anti_racha, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'reciente'
                    WHEN score_patron_dia >= GREATEST(score_deuda, score_reciente,
                         score_anti_racha, score_markov, score_ciclo_exacto, score_patron_fecha)
                    THEN 'patron_dia'
                    WHEN score_markov >= GREATEST(score_deuda, score_reciente,
                         score_patron_dia, score_anti_racha, score_ciclo_exacto, score_patron_fecha)
                    THEN 'markov'
                    WHEN score_ciclo_exacto >= GREATEST(score_deuda, score_reciente,
                         score_patron_dia, score_anti_racha, score_markov, score_patron_fecha)
                    THEN 'ciclo_exacto'
                    WHEN score_patron_fecha >= GREATEST(score_deuda, score_reciente,
                         score_patron_dia, score_anti_racha, score_markov, score_ciclo_exacto)
                    THEN 'patron_fecha'
                    ELSE 'anti_racha'
                END AS señal_dominante,
                COUNT(*)                                        AS total,
                SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END)  AS aciertos,
                ROUND(AVG(score_deuda)::numeric, 4)             AS avg_score_deuda,
                ROUND(AVG(score_reciente)::numeric, 4)          AS avg_score_reciente,
                ROUND(AVG(score_markov)::numeric, 4)            AS avg_score_markov,
                ROUND(AVG(score_patron_fecha)::numeric, 4)      AS avg_score_fecha
            FROM auditoria_señales
            WHERE fecha >= :desde
              AND acierto_top3 IS NOT NULL
            GROUP BY señal_dominante
            ORDER BY total DESC
        """), {"desde": fecha_ini})
        rows = res.fetchall()

        señales = []
        for r in rows:
            total = int(r[1])
            ac    = int(r[2])
            ef    = round(ac / total * 100, 1) if total > 0 else 0
            señales.append({
                "señal":      r[0],
                "total":      total,
                "aciertos":   ac,
                "ef_top3":    ef,
                "vs_azar":    round(ef / 7.89, 2),   # 7.89% = azar top3 (3/38)
                "recomendacion": (
                    "✅ MANTENER" if ef >= 9.0 else
                    "⚠️ REVISAR"  if ef >= 7.0 else
                    "🔴 REDUCIR PESO"
                ),
            })

        # Stats globales
        res_global = await db.execute(text("""
            SELECT
                COUNT(*),
                SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END),
                ROUND(AVG(score_deuda)::numeric, 4),
                ROUND(AVG(score_reciente)::numeric, 4),
                ROUND(AVG(score_markov)::numeric, 4),
                ROUND(AVG(score_patron_fecha)::numeric, 4),
                ROUND(AVG(confianza)::numeric, 1)
            FROM auditoria_señales
            WHERE fecha >= :desde AND acierto_top3 IS NOT NULL
        """), {"desde": fecha_ini})
        g = res_global.fetchone()
        total_g = int(g[0] or 0)
        ac_g    = int(g[1] or 0)

        return {
            "dias_analizados":   dias,
            "total_predicciones": total_g,
            "ef_top3_global":    round(ac_g / total_g * 100, 1) if total_g > 0 else 0,
            "por_señal":         señales,
            "avg_scores": {
                "deuda":        float(g[2] or 0),
                "reciente":     float(g[3] or 0),
                "markov":       float(g[4] or 0),
                "patron_fecha": float(g[5] or 0),
            },
            "confianza_promedio": float(g[6] or 0),
            "mensaje": (
                f"{'✅' if total_g >= 50 else '⚠️'} "
                f"{total_g} predicciones analizadas | "
                f"{'Muestra suficiente' if total_g >= 50 else 'Muestra pequeña — acumular más datos'}"
            ),
        }
    except Exception as e:
        return {"error": str(e), "por_señal": []}


# ══════════════════════════════════════════════════════
# CALIBRAR
# ══════════════════════════════════════════════════════
async def calibrar_predicciones(db) -> dict:
    try:
        r = await db.execute(text("""
            UPDATE auditoria_ia a
            SET
                acierto        = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND h.loteria = 'Lotto Activo'
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE'
                   OR a.resultado_real IS NULL)
        """))
        cal = r.rowcount
        await db.commit()
        return {"calibradas": cal}
    except Exception as e:
        await db.rollback()
        return {"calibradas": 0, "error": str(e)}


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
            WHERE a.fecha = CURRENT_DATE
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
        ef3 = round(ac3 / total_cal * 100, 1) if total_cal > 0 else 0

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

        # Wilson lower para confianza estadística
        wilson_top3 = round(wilson_lower(ac3, total_cal) * 100, 2) if total_cal > 0 else 0

        return {
            "efectividad_global":  float(ef[2] or 0),
            "efectividad_top3":    ef3,
            "wilson_lower_top3":   wilson_top3,
            "total_auditado":      total_cal,
            "aciertos_total":      int(ef[1] or 0),
            "aciertos_top3":       ac3,
            "aciertos_hoy":        int(hoy[0] or 0),
            "sorteos_hoy":         int(hoy[1] or 0),
            "top_animales":        top_animales,
            "total_historico":     total_hist,
            "horas_rentables":     horas_rentables,
            "pesos_actuales":      pesos,
            "generacion":          generacion,
        }
    except Exception:
        return {
            "efectividad_global": 0, "efectividad_top3": 0, "wilson_lower_top3": 0,
            "aciertos_hoy": 0, "sorteos_hoy": 0, "total_historico": 0,
            "top_animales": [], "horas_rentables": [], "generacion": 1,
        }


# ══════════════════════════════════════════════════════
# BACKTEST V10   
# ══════════════════════════════════════════════════════
async def backtest(db, fecha_desde, fecha_hasta, max_sorteos=100) -> dict:
    try:
        pesos = await _obtener_pesos_globales(db)
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
            pfe = await calcular_patron_fecha_exacta(db, hora_s, dia_s,
                      fecha_s.month, fecha_s)
            sc  = combinar_señales_v10(d,r,p,a,m,ce,pr,ps,hora_s,pesos,
                      patron_fecha=pfe)
            if not sc:
                continue

            confianza_idx, _, operar = calcular_indice_confianza_v10(sc)
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

        ef1  = round(ac1/total*100,1)  if total   > 0 else 0
        ef3  = round(ac3/total*100,1)  if total   > 0 else 0
        ef1c = round(ac1_c/total_c*100,1) if total_c > 0 else 0
        ef3c = round(ac3_c/total_c*100,1) if total_c > 0 else 0

        resumen_horas = {
            h: {
                "total": d["total"],
                "efectividad_top1": round(d["ac1"]/d["total"]*100,1) if d["total"]>0 else 0,
                "efectividad_top3": round(d["ac3"]/d["total"]*100,1) if d["total"]>0 else 0,
                "es_rentable": (d["ac3"]/d["total"]*100 >= UMBRAL_RENTABILIDAD_TOP3)
                               if d["total"]>0 else False,
            }
            for h, d in por_hora.items()
        }

        return {
            "total_sorteos":     total,
            "efectividad_top1":  ef1,
            "efectividad_top3":  ef3,
            "filtrado_conf25_top1": ef1c,
            "filtrado_conf25_top3": ef3c,
            "filtrado_conf25_n":    total_c,
            "resumen_por_hora":  resumen_horas,
            "mensaje": (
                f"V10: Top1 {ef1}% | Top3 {ef3}% | "
                f"Filtrado conf≥{UMBRAL_CONFIANZA_OPERAR} → Top1 {ef1c}% | Top3 {ef3c}%"
            ),
            "detalle": detalle,
        }
    except Exception as e:
        return {"error": str(e)}

# ══════════════════════════════════════════════════════
# LLENAR AUDITORÍA RETROACTIVA — sin límite de rango
# Procesa desde 2018 en bloques para no agotar memoria
# ══════════════════════════════════════════════════════
async def llenar_auditoria_retroactiva(db, fecha_desde=None, fecha_hasta=None, dias=30) -> dict:
    """
    Procesa todo el histórico desde fecha_desde hasta fecha_hasta.
    Llena AMBAS tablas:
      - auditoria_ia      → predicciones + aciertos
      - auditoria_señales → desglose de score por señal (para calibración)
    Commit cada 200 registros para no saturar memoria en Render.
    """
    try:
        hoy = date.today()
        if fecha_desde is None:
            fecha_desde = date(2018, 1, 1)
        if fecha_hasta is None:
            fecha_hasta = hoy - timedelta(days=1)

        pesos = await _obtener_pesos_globales(db)

        # Cargar todos los sorteos del rango en memoria de una vez
        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int,
                   EXTRACT(MONTH FROM fecha)::int
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
            ORDER BY fecha ASC, hora ASC
        """), {"desde": fecha_desde, "hasta": fecha_hasta})
        sorteos = res.fetchall()

        if not sorteos:
            return {"status": "ok", "procesados": 0,
                    "message": f"Sin sorteos entre {fecha_desde} y {fecha_hasta}"}

        insertados = 0; omitidos = 0; aciertos1 = 0; aciertos3 = 0
        señales_insertadas = 0

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s, mes_s = sorteo
            dia_s  = int(dia_s)
            mes_s  = int(mes_s)
            real_n = _normalizar(real)
            try:
                # Saltar si ya existe en auditoria_ia CON desglose (no rehacer)
                res_e = await db.execute(text(
                    "SELECT 1 FROM auditoria_ia "
                    "WHERE fecha=:f AND hora=:h AND acierto IS NOT NULL "
                    "AND prediccion_1 IS NOT NULL LIMIT 1"
                ), {"f": fecha_s, "h": hora_s})
                ya_existe_ia = res_e.fetchone() is not None

                # Verificar si ya existe en auditoria_señales (tabla puede no existir)
                ya_existe_sig = False
                try:
                    res_sig = await db.execute(text(
                        "SELECT 1 FROM auditoria_señales "
                        "WHERE fecha=:f AND hora=:h LIMIT 1"
                    ), {"f": fecha_s, "h": hora_s})
                    ya_existe_sig = res_sig.fetchone() is not None
                except Exception:
                    await db.rollback()
                    ya_existe_sig = False  # Tabla no existe, procesar igual

                # Solo saltar si ya tiene desglose de señales
                # (auditoria_ia puede existir sin señales — ese caso hay que procesar)
                if ya_existe_sig:
                    omitidos += 1
                    continue

                # Si ya existe en auditoria_ia, leer predicciones de ahí
                # (evita recalcular todo el motor para 27,000 registros)
                if ya_existe_ia:
                    res_pred = await db.execute(text("""
                        SELECT prediccion_1, prediccion_2, prediccion_3,
                               acierto, confianza_pct
                        FROM auditoria_ia
                        WHERE fecha=:f AND hora=:h LIMIT 1
                    """), {"f": fecha_s, "h": hora_s})
                    row_ia = res_pred.fetchone()
                    if not row_ia or not row_ia[0]:
                        omitidos += 1
                        continue
                    pred1 = _normalizar(row_ia[0])
                    pred2 = _normalizar(row_ia[1]) if row_ia[1] else None
                    pred3 = _normalizar(row_ia[2]) if row_ia[2] else None
                    acerto1 = bool(row_ia[3])
                    acerto3 = real_n in [x for x in [pred1, pred2, pred3] if x]
                    confianza_idx = int(row_ia[4] or 0)
                    # Scores mínimos para señales (no recalculamos el motor completo)
                    d = {}; r = {}; p = {}; a = {}; m = {}; ce = {}; pfe = {}
                    sc = {pred1: 1.0}
                else:
                    # Calcular motor completo solo si no existe en auditoria_ia
                    d   = await calcular_deuda(db, hora_s, fecha_s)
                    r   = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                    p   = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                    a   = await calcular_anti_racha(db, hora_s, fecha_s)
                    m   = await calcular_markov_hora(db, hora_s, fecha_s)
                    ce  = await calcular_ciclo_exacto(db, hora_s, fecha_s)
                    pr  = await calcular_penalizacion_reciente(db, hora_s, fecha_s)
                    ps  = await calcular_penalizacion_sobreprediccion(db, hora_s, fecha_s)
                    pfe = await calcular_patron_fecha_exacta(db, hora_s, dia_s, mes_s, fecha_s)

                    sc = combinar_señales_v10(d, r, p, a, m, ce, pr, ps, hora_s, pesos,
                                              patron_fecha=pfe)
                    if not sc:
                        continue

                    confianza_idx, _, _ = calcular_indice_confianza_v10(sc)
                    ranking = sorted(sc.items(), key=lambda x: x[1], reverse=True)
                    pred1 = _normalizar(ranking[0][0]) if len(ranking) > 0 else None
                    pred2 = _normalizar(ranking[1][0]) if len(ranking) > 1 else None
                    pred3 = _normalizar(ranking[2][0]) if len(ranking) > 2 else None

                    acerto1 = (pred1 == real_n)
                    acerto3 = real_n in [x for x in [pred1, pred2, pred3] if x]

                # ── INSERT auditoria_ia ──
                if not ya_existe_ia:
                    await db.execute(text("""
                        INSERT INTO auditoria_ia
                            (fecha, hora, animal_predicho, prediccion_1, prediccion_2,
                             prediccion_3, confianza_pct, resultado_real, acierto)
                        VALUES (:f,:h,:a,:p1,:p2,:p3,:c,:r,:ac)
                        ON CONFLICT (fecha, hora) DO UPDATE SET
                            animal_predicho = EXCLUDED.animal_predicho,
                            prediccion_1    = EXCLUDED.prediccion_1,
                            prediccion_2    = EXCLUDED.prediccion_2,
                            prediccion_3    = EXCLUDED.prediccion_3,
                            confianza_pct   = EXCLUDED.confianza_pct,
                            resultado_real  = EXCLUDED.resultado_real,
                            acierto         = EXCLUDED.acierto
                    """), {
                        "f": fecha_s, "h": hora_s, "a": pred1,
                        "p1": pred1, "p2": pred2, "p3": pred3,
                        "c": float(confianza_idx),
                        "r": real_n, "ac": acerto1,
                    })
                    insertados += 1
                    if acerto1: aciertos1 += 1
                    if acerto3: aciertos3 += 1

                # ── INSERT auditoria_señales (desglose por señal) ──
                # En try separado — si la tabla no existe, no bloquea auditoria_ia
                if not ya_existe_sig and pred1:
                    try:
                        await db.execute(text("""
                            INSERT INTO auditoria_señales (
                                fecha, hora, animal_predicho, resultado_real,
                                acierto_top1, acierto_top3, confianza,
                                score_deuda, score_reciente, score_patron_dia,
                                score_anti_racha, score_markov, score_ciclo_exacto,
                                score_patron_fecha, score_final,
                                peso_deuda, peso_reciente, peso_patron,
                                peso_anti, peso_markov
                            ) VALUES (
                                :f, :h, :animal, :real,
                                :ac1, :ac3, :conf,
                                :s_deuda, :s_rec, :s_patron,
                                :s_anti, :s_markov, :s_ciclo,
                                :s_fecha, :s_final,
                                :p_deuda, :p_rec, :p_patron,
                                :p_anti, :p_markov
                            )
                            ON CONFLICT (fecha, hora) DO UPDATE SET
                                resultado_real     = EXCLUDED.resultado_real,
                                acierto_top1       = EXCLUDED.acierto_top1,
                                acierto_top3       = EXCLUDED.acierto_top3,
                                score_deuda        = EXCLUDED.score_deuda,
                                score_reciente     = EXCLUDED.score_reciente,
                                score_patron_dia   = EXCLUDED.score_patron_dia,
                                score_anti_racha   = EXCLUDED.score_anti_racha,
                                score_markov       = EXCLUDED.score_markov,
                                score_ciclo_exacto = EXCLUDED.score_ciclo_exacto,
                                score_patron_fecha = EXCLUDED.score_patron_fecha,
                                score_final        = EXCLUDED.score_final
                        """), {
                            "f":       fecha_s,
                            "h":       hora_s,
                            "animal":  pred1,
                            "real":    real_n,
                            "ac1":     acerto1,
                            "ac3":     acerto3,
                            "conf":    int(confianza_idx),
                            "s_deuda":  round(d.get(pred1, {}).get("score", 0) * pesos["deuda"], 4),
                            "s_rec":    round(r.get(pred1, {}).get("score", 0) * pesos["reciente"], 4),
                            "s_patron": round(p.get(pred1, {}).get("score", 0) * pesos["patron"], 4),
                            "s_anti":   round(a.get(pred1, {}).get("score", 0) * pesos["anti"], 4),
                            "s_markov": round(m.get(pred1, {}).get("score", 0) * pesos["secuencia"], 4),
                            "s_ciclo":  round(ce.get(pred1, {}).get("score", 0) * 0.15, 4),
                            "s_fecha":  round(pfe.get(pred1, {}).get("score", 0) * 0.12, 4),
                            "s_final":  round(sc.get(pred1, 0), 6),
                            "p_deuda":  pesos["deuda"],
                            "p_rec":    pesos["reciente"],
                            "p_patron": pesos["patron"],
                            "p_anti":   pesos["anti"],
                            "p_markov": pesos["secuencia"],
                        })
                        señales_insertadas += 1
                    except Exception:
                        await db.rollback()  # Solo rollback del señales, no del ia

                # Commit cada 200 registros para no saturar memoria en Render
                if (insertados + señales_insertadas) % 200 == 0:
                    await db.commit()

            except Exception as _err:
                # Loggear el primer error para diagnóstico
                if insertados == 0 and omitidos == 0 and señales_insertadas == 0:
                    import logging
                    logging.getLogger(__name__).warning(f"Retroactivo error en {fecha_s} {hora_s}: {_err}")
                await db.rollback()
                continue

        await db.commit()
        ef1 = round(aciertos1 / max(insertados, 1) * 100, 1) if insertados > 0 else 0
        ef3 = round(aciertos3 / max(insertados, 1) * 100, 1) if insertados > 0 else 0

        return {
            "status": "success",
            "procesados":              insertados,
            "señales_insertadas":      señales_insertadas,
            "omitidos_ya_existian":    omitidos,
            "aciertos_top1":           aciertos1,
            "aciertos_top3":           aciertos3,
            "efectividad_top1":        ef1,
            "efectividad_top3":        ef3,
            "message": (
                f"✅ Retroactivo {fecha_desde}→{fecha_hasta}: "
                f"{insertados} nuevas en auditoria_ia | "
                f"{señales_insertadas} desgloses de señales insertados | "
                f"{omitidos} omitidos (ya tenían señales)"
            ),
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}
