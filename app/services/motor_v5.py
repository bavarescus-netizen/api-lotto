"""
MOTOR V9 — LOTTOAI PRO — INTELIGENCIA AVANZADA
================================================
Mejoras sobre V8:
1. Guarda TOP 3 predicciones en auditoria_ia (pred1, pred2, pred3)
2. Detección de ciclos por hora específica (cada hora tiene su propio patrón)
3. Análisis de repetición: penaliza animales que salieron hace <3 sorteos
4. Score de rentabilidad: identifica horas donde el sistema supera 3.33%
5. Memoria de ventana corta: últimos 5 sorteos de la MISMA hora
6. Anti-sequencia: si X salió en sorteo anterior mismo día, reducir score
7. Señal de confianza calibrada con lógica de ganancia real 1:30

ESQUEMA REQUERIDO (ejecutar si no existe):
------------------------------------------
ALTER TABLE auditoria_ia 
    ADD COLUMN IF NOT EXISTS prediccion_1 VARCHAR(50),
    ADD COLUMN IF NOT EXISTS prediccion_2 VARCHAR(50),
    ADD COLUMN IF NOT EXISTS prediccion_3 VARCHAR(50),
    ADD COLUMN IF NOT EXISTS confianza_hora FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS es_hora_rentable BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS rentabilidad_hora (
    hora VARCHAR(20) PRIMARY KEY,
    total_sorteos INT DEFAULT 0,
    aciertos_top1 INT DEFAULT 0,
    aciertos_top3 INT DEFAULT 0,
    efectividad_top1 FLOAT DEFAULT 0,
    efectividad_top3 FLOAT DEFAULT 0,
    es_rentable BOOLEAN DEFAULT FALSE,
    ultima_actualizacion TIMESTAMP DEFAULT NOW()
);
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
import pytz, re

# ══════════════════════════════════════════════════════
# CATÁLOGO DE ANIMALES
# ══════════════════════════════════════════════════════
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
TODOS_LOS_ANIMALES = list(MAPA_ANIMALES.values())

HORAS_SORTEO_STR = [
    "08:00 AM","09:00 AM","10:00 AM","11:00 AM","12:00 PM",
    "01:00 PM","02:00 PM","03:00 PM","04:00 PM","05:00 PM","06:00 PM","07:00 PM"
]

# Umbral de rentabilidad: pago 1:30 con 3 animales = necesitas >3.33% por sorteo
# Con top3, si aciertas 1 de 3 en un sorteo: 1/30 - (2 apuestas perdidas) → rentable si >3.33%
UMBRAL_RENTABILIDAD_TOP1 = 3.33   # % mínimo top1 para ser rentable
UMBRAL_RENTABILIDAD_TOP3 = 10.0   # % mínimo top3 (una de las 3) para ser rentable
UMBRAL_CONFIANZA_OPERAR  = 30     # índice mínimo para recomendar operar


# ══════════════════════════════════════════════════════
# MIGRACIÓN AUTOMÁTICA — añade columnas si faltan
# ══════════════════════════════════════════════════════
async def migrar_schema(db):
    """Ejecuta en startup: agrega columnas nuevas a auditoria_ia si no existen"""
    migraciones = [
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
    for sql in migraciones:
        try:
            await db.execute(text(sql))
        except Exception:
            pass
    try:
        await db.commit()
    except Exception:
        await db.rollback()


# ══════════════════════════════════════════════════════
# PESOS DINÁMICOS
# ══════════════════════════════════════════════════════
async def obtener_pesos_actuales(db) -> dict:
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
    return {"reciente":0.2799,"deuda":0.2811,"anti":0.2253,"patron":0.0919,"secuencia":0.1219}


async def guardar_pesos(db, pesos: dict, efectividad: float, total: int, aciertos_n: int, generacion: int):
    try:
        await db.execute(text("""
            INSERT INTO motor_pesos
                (peso_reciente,peso_deuda,peso_anti,peso_patron,peso_secuencia,
                 efectividad,total_evaluados,aciertos,generacion)
            VALUES (:r,:d,:a,:p,:s,:ef,:tot,:ac,:gen)
        """), {
            "r": pesos["reciente"], "d": pesos["deuda"], "a": pesos["anti"],
            "p": pesos["patron"],   "s": pesos["secuencia"],
            "ef": efectividad, "tot": total, "ac": aciertos_n, "gen": generacion
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        print(f"Error guardando pesos: {e}")


# ══════════════════════════════════════════════════════
# SEÑAL 1: DEUDA DE APARICIÓN POR HORA
# Detecta animales que llevan más tiempo sin salir
# en ESA hora específica, ponderado por su ciclo promedio
# ══════════════════════════════════════════════════════
async def calcular_deuda(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa
            FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        ),
        gaps AS (SELECT animalito,(fecha-fa) AS gap FROM apariciones WHERE fa IS NOT NULL),
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
            # Bonus por deuda alta, penalización por alta varianza (animal impredecible)
            score = min(d / max_d, 1.0)
            if d > 400:   score = min(score * 1.5, 1.0)
            elif d > 250: score = min(score * 1.25, 1.0)
            # Penalizar si el ciclo tiene mucha varianza (animal errático)
            if varianza > 15: score *= 0.85
            resultado[r[0]] = {
                "score": round(score, 4),
                "dias_ausente": int(r[1]),
                "ciclo_prom": float(r[2]),
                "pct_deuda": d,
                "varianza_ciclo": float(r[4]) if r[4] else 0,
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 2: FRECUENCIA RECIENTE EN ESA HORA
# Ventana de 60 días, normalizada
# ══════════════════════════════════════════════════════
async def calcular_frecuencia_reciente(db, hora_str, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    fecha_60 = fecha_limite - timedelta(days=60)
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces FROM historico
        WHERE hora=:hora AND fecha>=:desde AND fecha<:hasta AND loteria='Lotto Activo'
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora": hora_str, "desde": fecha_60, "hasta": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        total = sum(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {
                "score": r[1] / max_v,
                "veces": int(r[1]),
                "pct": round(r[1] / total * 100, 1)
            }
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 3: PATRÓN DÍA DE SEMANA × HORA
# Qué animals salen más en ese día+hora combinados
# ══════════════════════════════════════════════════════
async def calcular_patron_dia(db, hora_str, dia_semana, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, COUNT(*) AS veces FROM historico
        WHERE hora=:hora AND EXTRACT(DOW FROM fecha)=:dia
          AND fecha<:hoy AND loteria='Lotto Activo'
        GROUP BY animalito ORDER BY veces DESC
    """), {"hora": hora_str, "dia": dia_semana, "hoy": fecha_limite})
    rows = res.fetchall()
    resultado = {}
    if rows:
        max_v = max(r[1] for r in rows)
        for r in rows:
            resultado[r[0]] = {"score": r[1] / max_v, "veces": int(r[1])}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 4: ANTI-RACHA (días desde última aparición)
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
        if dias <= 1:   score = 0.02
        elif dias <= 3: score = 0.10
        elif dias <= 7: score = 0.35
        elif dias <= 14: score = 0.60
        elif dias <= 30: score = 0.80
        else:           score = 1.00
        resultado[r[0]] = {"score": score, "dias_desde_ultima": dias}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 5: SECUENCIA HISTÓRICA
# Después de X, ¿qué animal suele salir?
# ══════════════════════════════════════════════════════
async def calcular_secuencia(db, fecha_limite=None):
    if fecha_limite is None:
        fecha_limite = date.today()
    res_u = await db.execute(text(
        "SELECT animalito FROM historico WHERE fecha<:hoy AND loteria='Lotto Activo' "
        "ORDER BY fecha DESC, hora DESC LIMIT 1"
    ), {"hoy": fecha_limite})
    ultimo = res_u.scalar()
    if not ultimo:
        return {}
    res = await db.execute(text("""
        WITH seq AS (
            SELECT animalito, LEAD(animalito) OVER (ORDER BY fecha, hora) AS siguiente
            FROM historico WHERE fecha<:hoy AND loteria='Lotto Activo'
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
            resultado[r[0]] = {"score": r[1] / max_v, "veces": int(r[1])}
    return resultado


# ══════════════════════════════════════════════════════
# SEÑAL 6 (NUEVA): REPETICIÓN RECIENTE EN ESA HORA
# Penaliza animales que salieron en los últimos N sorteos
# de ESA misma hora (evitar predecir lo que acaba de salir)
# ══════════════════════════════════════════════════════
async def calcular_penalizacion_reciente(db, hora_str, fecha_limite=None, ventana=5):
    """
    Devuelve dict: animal → factor_penalizacion (0.0 a 1.0)
    Animales que salieron recientemente en esa hora reciben penalización.
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        SELECT animalito, fecha FROM historico
        WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        ORDER BY fecha DESC LIMIT :ventana
    """), {"hora": hora_str, "hoy": fecha_limite, "ventana": ventana})
    rows = res.fetchall()
    penalizacion = {}
    for i, r in enumerate(rows):
        # Más reciente = penalización más alta
        factor = 1.0 - (0.15 * (ventana - i) / ventana)
        penalizacion[r[0]] = round(max(factor, 0.3), 3)
    return penalizacion


# ══════════════════════════════════════════════════════
# SEÑAL 7 (NUEVA): CICLO EXACTO POR HORA
# Calcula si hoy es el día "esperado" según el ciclo histórico
# de aparición en ESA hora específica
# ══════════════════════════════════════════════════════
async def calcular_ciclo_exacto(db, hora_str, fecha_limite=None):
    """
    Para cada animal, calcula:
    - Su ciclo promedio en esa hora (cada cuántos días aparece)
    - Días desde su última aparición en esa hora
    - Score: 1.0 si está "vencido" según el ciclo, 0 si acaba de salir
    """
    if fecha_limite is None:
        fecha_limite = date.today()
    res = await db.execute(text("""
        WITH apariciones AS (
            SELECT animalito, fecha,
                LAG(fecha) OVER (PARTITION BY animalito ORDER BY fecha) AS fa
            FROM historico
            WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
        ),
        gaps AS (
            SELECT animalito, (fecha-fa) AS gap
            FROM apariciones WHERE fa IS NOT NULL
        ),
        estadisticas AS (
            SELECT animalito,
                AVG(gap) AS ciclo_prom,
                MIN(gap) AS ciclo_min,
                MAX(gap) AS ciclo_max,
                COUNT(*) AS n_apariciones
            FROM gaps GROUP BY animalito HAVING COUNT(*)>=5
        ),
        ultima_vez AS (
            SELECT animalito, MAX(fecha) AS ultima_fecha
            FROM historico WHERE hora=:hora AND fecha<:hoy AND loteria='Lotto Activo'
            GROUP BY animalito
        )
        SELECT e.animalito,
            e.ciclo_prom,
            e.ciclo_min,
            e.ciclo_max,
            e.n_apariciones,
            (:hoy - u.ultima_fecha) AS dias_ausente
        FROM estadisticas e JOIN ultima_vez u ON e.animalito=u.animalito
        ORDER BY e.animalito
    """), {"hora": hora_str, "hoy": fecha_limite})

    rows = res.fetchall()
    resultado = {}
    for r in rows:
        animal = r[0]
        ciclo_prom = float(r[1])
        ciclo_min  = float(r[2])
        ciclo_max  = float(r[3])
        n_ap       = int(r[4])
        dias_aus   = int(r[5])

        # ¿En qué punto del ciclo estamos?
        pct_ciclo = dias_aus / ciclo_prom if ciclo_prom > 0 else 0

        # Score basado en posición en el ciclo
        if pct_ciclo < 0.5:
            # Demasiado temprano — acaba de aparecer
            score = 0.05
        elif pct_ciclo < 0.8:
            # Antes del ciclo promedio
            score = 0.3 + (pct_ciclo - 0.5) * 1.5
        elif pct_ciclo < 1.2:
            # En la ventana óptima del ciclo
            score = 0.85 + (pct_ciclo - 0.8) * 0.5
            score = min(score, 1.0)
        elif pct_ciclo < 2.0:
            # Sobrepassó el ciclo — alta deuda
            score = 1.0
        else:
            # Muy tarde — puede ser animal raro/descontinuado
            score = 0.7

        # Bonus por mayor cantidad de apariciones (animal más predecible)
        confiabilidad = min(n_ap / 50.0, 1.0)

        resultado[animal] = {
            "score": round(score * (0.7 + 0.3 * confiabilidad), 4),
            "ciclo_prom_dias": round(ciclo_prom, 1),
            "dias_ausente": dias_aus,
            "pct_ciclo": round(pct_ciclo * 100, 1),
            "n_apariciones": n_ap,
            "ventana": f"{round(ciclo_min,0)}-{round(ciclo_max,0)} días",
        }
    return resultado


# ══════════════════════════════════════════════════════
# COMBINAR SEÑALES con pesos dinámicos + penalización
# ══════════════════════════════════════════════════════
def combinar_señales_v9(deuda, reciente, patron, anti, secuencia, ciclo_exacto,
                         penalizacion, hora_str, pesos):
    """
    Combina 7 señales con pesos aprendidos.
    Aplica penalización por aparición reciente.
    """
    todos = set(
        list(deuda) + list(reciente) + list(patron) +
        list(anti) + list(secuencia) + list(ciclo_exacto)
    )

    scores = {}
    for animal in todos:
        # Score base ponderado
        base = (
            deuda.get(animal, {}).get("score", 0)        * pesos["deuda"] +
            reciente.get(animal, {}).get("score", 0)     * pesos["reciente"] +
            patron.get(animal, {}).get("score", 0)       * pesos["patron"] +
            anti.get(animal, {}).get("score", 0.5)       * pesos["anti"] +
            secuencia.get(animal, {}).get("score", 0)    * pesos["secuencia"] +
            ciclo_exacto.get(animal, {}).get("score", 0) * 0.15  # señal extra
        )
        # Normalizar suma de pesos (incluye el 0.15 extra de ciclo)
        suma_pesos = sum(pesos.values()) + 0.15
        base = base / suma_pesos

        # Aplicar penalización por aparición reciente en esa hora
        factor_pen = penalizacion.get(animal, 1.0)
        scores[animal] = round(base * factor_pen, 6)

    return scores


# ══════════════════════════════════════════════════════
# ÍNDICE DE CONFIANZA V9
# Incluye efectividad histórica de ESA hora
# ══════════════════════════════════════════════════════
def calcular_indice_confianza_v9(scores, efectividad_hora=None):
    if not scores:
        return 0, "🔴 SIN DATOS"
    valores = sorted(scores.values(), reverse=True)
    if len(valores) < 3:
        return 10, "🔴 DATOS INSUFICIENTES"

    top1, top2, top3 = valores[0], valores[1], valores[2]
    promedio = sum(valores) / len(valores)

    separacion_rel = (top1 - top2) / top1 if top1 > 0 else 0
    dominio = top1 / promedio if promedio > 0 else 1
    brecha_grupo = (top2 - top3) / top2 if top2 > 0 else 0

    confianza = int(
        separacion_rel * 55 +
        min(dominio - 1, 1) * 30 +
        brecha_grupo * 15
    )

    # Bonus si la hora tiene efectividad histórica probada (>3.5%)
    if efectividad_hora and efectividad_hora > 3.5:
        confianza = min(confianza + 8, 100)
    elif efectividad_hora and efectividad_hora < 2.5:
        confianza = max(confianza - 10, 0)

    confianza = min(100, max(0, confianza))

    if confianza >= 45:
        return confianza, "🟢 ALTA CONFIANZA — OPERAR"
    elif confianza >= 30:
        return confianza, "🟡 MEDIA CONFIANZA — OPERAR CON CAUTELA"
    else:
        return confianza, "🔴 BAJA CONFIANZA — NO OPERAR"


# ══════════════════════════════════════════════════════
# ANÁLISIS DE RENTABILIDAD POR HORA
# El corazón del nuevo sistema: saber CUÁNDO operar
# ══════════════════════════════════════════════════════
async def calcular_rentabilidad_horas(db) -> dict:
    """
    Para cada hora de sorteo, calcula:
    - Efectividad top1 (% de veces que #1 acertó)
    - Efectividad top3 (% de veces que uno de los 3 acertó)
    - Si es rentable (top3 > 10% = aprox. 3x el umbral mínimo)
    Requiere que auditoria_ia tenga prediccion_1/2/3 guardadas
    """
    resultado = {}
    for hora in HORAS_SORTEO_STR:
        try:
            res = await db.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN acierto=TRUE THEN 1 END) AS ac_top1,
                    COUNT(CASE WHEN
                        h.animalito IN (
                            COALESCE(a.prediccion_1,'__'),
                            COALESCE(a.prediccion_2,'__'),
                            COALESCE(a.prediccion_3,'__')
                        ) THEN 1 END) AS ac_top3
                FROM auditoria_ia a
                JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora
                    AND h.loteria='Lotto Activo'
                WHERE a.hora=:hora AND a.acierto IS NOT NULL
            """), {"hora": hora})
            r = res.fetchone()
            if r and r[0] > 0:
                total = int(r[0])
                ac1 = int(r[1])
                ac3 = int(r[2])
                ef1 = round(ac1 / total * 100, 2)
                ef3 = round(ac3 / total * 100, 2)
                es_rentable = ef3 >= UMBRAL_RENTABILIDAD_TOP3
                resultado[hora] = {
                    "total": total,
                    "efectividad_top1": ef1,
                    "efectividad_top3": ef3,
                    "es_rentable": es_rentable,
                    "ventaja": round(ef3 - UMBRAL_RENTABILIDAD_TOP1, 2),
                }
            else:
                resultado[hora] = {
                    "total": 0,
                    "efectividad_top1": 0.0,
                    "efectividad_top3": 0.0,
                    "es_rentable": False,
                    "ventaja": -UMBRAL_RENTABILIDAD_TOP1,
                }
        except Exception:
            resultado[hora] = {"total": 0, "efectividad_top1": 0, "efectividad_top3": 0,
                               "es_rentable": False, "ventaja": 0}
    return resultado


async def actualizar_tabla_rentabilidad(db, rentabilidad: dict):
    """Persiste la rentabilidad por hora en la BD"""
    for hora, datos in rentabilidad.items():
        try:
            await db.execute(text("""
                INSERT INTO rentabilidad_hora
                    (hora, total_sorteos, aciertos_top1, aciertos_top3,
                     efectividad_top1, efectividad_top3, es_rentable, ultima_actualizacion)
                VALUES (:hora, :tot, :ac1, :ac3, :ef1, :ef3, :rent, NOW())
                ON CONFLICT (hora) DO UPDATE SET
                    total_sorteos=EXCLUDED.total_sorteos,
                    efectividad_top1=EXCLUDED.efectividad_top1,
                    efectividad_top3=EXCLUDED.efectividad_top3,
                    es_rentable=EXCLUDED.es_rentable,
                    ultima_actualizacion=NOW()
            """), {
                "hora": hora,
                "tot": datos["total"],
                "ac1": int(datos["total"] * datos["efectividad_top1"] / 100),
                "ac3": int(datos["total"] * datos["efectividad_top3"] / 100),
                "ef1": datos["efectividad_top1"],
                "ef3": datos["efectividad_top3"],
                "rent": datos["es_rentable"],
            })
        except Exception:
            pass
    try:
        await db.commit()
    except Exception:
        await db.rollback()


async def obtener_rentabilidad_hora(db, hora_str) -> dict:
    """Lee la rentabilidad de una hora desde la BD"""
    try:
        res = await db.execute(text(
            "SELECT efectividad_top1, efectividad_top3, es_rentable "
            "FROM rentabilidad_hora WHERE hora=:hora"
        ), {"hora": hora_str})
        r = res.fetchone()
        if r:
            return {
                "efectividad_top1": float(r[0]),
                "efectividad_top3": float(r[1]),
                "es_rentable": bool(r[2]),
            }
    except Exception:
        pass
    return {"efectividad_top1": 0.0, "efectividad_top3": 0.0, "es_rentable": False}


# ══════════════════════════════════════════════════════
# PREDICCIÓN V9 — NÚCLEO PRINCIPAL
# ══════════════════════════════════════════════════════
async def generar_prediccion(db) -> dict:
    try:
        tz = pytz.timezone('America/Caracas')
        ahora = datetime.now(tz)
        hora_str = ahora.strftime("%I:00 %p").upper()
        dia_semana = ahora.weekday()
        hoy = ahora.date()

        pesos = await obtener_pesos_actuales(db)
        rent_hora = await obtener_rentabilidad_hora(db, hora_str)

        # Calcular todas las señales
        deuda       = await calcular_deuda(db, hora_str)
        reciente    = await calcular_frecuencia_reciente(db, hora_str)
        patron      = await calcular_patron_dia(db, hora_str, dia_semana)
        anti        = await calcular_anti_racha(db, hora_str)
        secuencia   = await calcular_secuencia(db)
        ciclo_exacto= await calcular_ciclo_exacto(db, hora_str)
        penalizacion= await calcular_penalizacion_reciente(db, hora_str)

        scores = combinar_señales_v9(
            deuda, reciente, patron, anti, secuencia,
            ciclo_exacto, penalizacion, hora_str, pesos
        )

        ef_hora = rent_hora.get("efectividad_top1", 0)
        confianza_idx, señal_texto = calcular_indice_confianza_v9(scores, ef_hora)

        ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total_scores = sum(scores.values()) or 1

        top3 = []
        for animal, score in ranking[:3]:
            nombre = re.sub(r'[^a-z]', '', animal.lower())
            num = NUMERO_POR_ANIMAL.get(nombre, "--")
            pct = round(score / total_scores * 100, 1)
            info_d = deuda.get(animal, {})
            info_c = ciclo_exacto.get(animal, {})
            top3.append({
                "numero":       num,
                "animal":       nombre.upper(),
                "imagen":       f"{nombre}.png",
                "porcentaje":   f"{pct}%",
                "score_raw":    round(score, 4),
                "dias_ausente": info_d.get("dias_ausente", 0),
                "pct_deuda":    info_d.get("pct_deuda", 0),
                "pct_ciclo":    info_c.get("pct_ciclo", 0),
                "ciclo_ventana": info_c.get("ventana", ""),
            })

        # Obtener último resultado
        res_u = await db.execute(text(
            "SELECT animalito FROM historico WHERE loteria='Lotto Activo' "
            "ORDER BY fecha DESC, hora DESC LIMIT 1"
        ))
        ultimo = res_u.scalar()

        # Señal de rentabilidad para esta hora
        es_hora_rentable = rent_hora.get("es_rentable", False)
        ef_top3_hora = rent_hora.get("efectividad_top3", 0)

        if es_hora_rentable:
            señal_texto = "⭐ " + señal_texto

        # Guardar predicción con top1+top2+top3 en auditoria_ia
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
                    "ch": float(ef_hora),
                    "rent": es_hora_rentable,
                })
                await db.commit()
            except Exception as e:
                await db.rollback()
                print(f"Error guardando predicción: {e}")

        # Próximo sorteo
        idx_actual = HORAS_SORTEO_STR.index(hora_str) if hora_str in HORAS_SORTEO_STR else -1
        proxima_hora = HORAS_SORTEO_STR[idx_actual + 1] if idx_actual < len(HORAS_SORTEO_STR) - 1 else None

        return {
            "top3":            top3,
            "hora":            hora_str,
            "ultimo_resultado": ultimo or "N/A",
            "confianza_idx":   confianza_idx,
            "señal_texto":     señal_texto,
            "hora_premium":    es_hora_rentable,
            "efectividad_hora_top3": ef_top3_hora,
            "proxima_hora":    proxima_hora,
            "pesos_actuales":  pesos,
            "analisis": (
                f"Motor V9 | {hora_str} | Confianza: {confianza_idx}/100 | "
                f"Ef.Hora(top3): {ef_top3_hora}% | {señal_texto}"
            )
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"top3": [], "analisis": f"Error V9: {e}", "confianza_idx": 0, "señal_texto": "ERROR"}


# ══════════════════════════════════════════════════════
# APRENDIZAJE POR REFUERZO V9
# Ahora evalúa top3 además de top1
# ══════════════════════════════════════════════════════
async def aprender_desde_historico(db, fecha_inicio=None, dias_por_generacion=30) -> dict:
    try:
        hoy = date.today()
        if fecha_inicio is None:
            fecha_inicio = hoy - timedelta(days=365)

        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),0) FROM motor_pesos"))
        generacion_actual = (res_gen.scalar() or 0) + 1

        pesos = await obtener_pesos_actuales(db)
        mejor_efectividad = 0.0
        mejores_pesos = pesos.copy()
        total_global = 0
        aciertos_global = 0
        aciertos_top3_global = 0
        generaciones_completadas = 0
        log = []

        fecha_ventana = fecha_inicio
        while fecha_ventana < hoy - timedelta(days=7):
            fecha_fin_ventana = min(
                fecha_ventana + timedelta(days=dias_por_generacion),
                hoy - timedelta(days=1)
            )
            res = await db.execute(text("""
                SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
                FROM historico
                WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
                ORDER BY fecha ASC, hora ASC LIMIT 500
            """), {"desde": fecha_ventana, "hasta": fecha_fin_ventana})
            sorteos = res.fetchall()

            if not sorteos:
                fecha_ventana += timedelta(days=dias_por_generacion)
                continue

            aciertos_por_señal = {
                "reciente": 0, "deuda": 0, "anti": 0,
                "patron": 0, "secuencia": 0
            }
            total_ventana = 0
            aciertos_ventana = 0
            aciertos_top3_ventana = 0

            for sorteo in sorteos[:60]:
                fecha_s, hora_s, real, dia_s = sorteo
                dia_s = int(dia_s)
                try:
                    d  = await calcular_deuda(db, hora_s, fecha_s)
                    r  = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                    p  = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                    a  = await calcular_anti_racha(db, hora_s, fecha_s)
                    s  = await calcular_secuencia(db, fecha_s)
                    ce = await calcular_ciclo_exacto(db, hora_s, fecha_s)
                    pen= await calcular_penalizacion_reciente(db, hora_s, fecha_s)

                    # Correlación individual de cada señal
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

                    sc = combinar_señales_v9(d, r, p, a, s, ce, pen, hora_s, pesos)
                    if sc:
                        ranking = sorted(sc.items(), key=lambda x: x[1], reverse=True)
                        top1_pred = ranking[0][0].lower() if ranking else ""
                        top3_preds = [x[0].lower() for x in ranking[:3]]

                        if top1_pred == real.lower():
                            aciertos_ventana += 1
                        if real.lower() in top3_preds:
                            aciertos_top3_ventana += 1
                    total_ventana += 1
                except Exception:
                    continue

            if total_ventana == 0:
                fecha_ventana += timedelta(days=dias_por_generacion)
                continue

            ef_ventana      = aciertos_ventana / total_ventana
            ef_top3_ventana = aciertos_top3_ventana / total_ventana

            # Ajuste de pesos por correlación de señales
            total_señal = sum(aciertos_por_señal.values()) or 1
            nuevos_pesos = {}
            for señal, ac_s in aciertos_por_señal.items():
                peso_señal    = ac_s / total_señal
                peso_suavizado = 0.65 * pesos[señal] + 0.35 * peso_señal
                nuevos_pesos[señal] = peso_suavizado

            # Normalizar
            total_nuevo = sum(nuevos_pesos.values())
            nuevos_pesos = {k: round(v / total_nuevo, 4) for k, v in nuevos_pesos.items()}

            # Actualizar si mejora
            if ef_ventana >= mejor_efectividad or generaciones_completadas == 0:
                if ef_ventana > mejor_efectividad:
                    mejor_efectividad = ef_ventana
                    mejores_pesos = nuevos_pesos.copy()
                pesos = nuevos_pesos

            total_global           += total_ventana
            aciertos_global        += aciertos_ventana
            aciertos_top3_global   += aciertos_top3_ventana
            generaciones_completadas += 1

            log.append({
                "ventana":       f"{fecha_ventana} → {fecha_fin_ventana}",
                "sorteos":       total_ventana,
                "ef_top1":       round(ef_ventana * 100, 1),
                "ef_top3":       round(ef_top3_ventana * 100, 1),
                "mejor_señal":   max(aciertos_por_señal, key=aciertos_por_señal.get),
                "pesos_nuevos":  nuevos_pesos,
            })

            fecha_ventana += timedelta(days=dias_por_generacion)

        ef_global      = round(aciertos_global / total_global * 100, 1) if total_global > 0 else 0
        ef_top3_global = round(aciertos_top3_global / total_global * 100, 1) if total_global > 0 else 0
        await guardar_pesos(db, mejores_pesos, ef_global, total_global, aciertos_global, generacion_actual)

        return {
            "status":                   "success",
            "generacion":               generacion_actual,
            "fecha_inicio":             str(fecha_inicio),
            "fecha_fin":                str(hoy),
            "generaciones_completadas": generaciones_completadas,
            "total_sorteos_evaluados":  total_global,
            "aciertos_top1":            aciertos_global,
            "aciertos_top3":            aciertos_top3_global,
            "efectividad_top1":         ef_global,
            "efectividad_top3":         ef_top3_global,
            "mejores_pesos":            mejores_pesos,
            "message": (
                f"✅ Gen {generacion_actual} | "
                f"Top1: {ef_global}% | Top3: {ef_top3_global}% | "
                f"Pesos: {mejores_pesos}"
            ),
            "log_ventanas": log[-5:]
        }
    except Exception as e:
        await db.rollback()
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# ENTRENAR — calibra + calcula rentabilidad por hora
# ══════════════════════════════════════════════════════
async def entrenar_modelo(db) -> dict:
    try:
        # Calibrar predicciones pendientes: ahora verifica top1, top2 y top3
        await db.execute(text("""
            UPDATE auditoria_ia a
            SET
                acierto = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND h.loteria = 'Lotto Activo'
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE')
        """))

        # Actualizar probabilidades_hora
        await db.execute(text("DELETE FROM probabilidades_hora"))
        await db.execute(text("""
            INSERT INTO probabilidades_hora
                (hora, animalito, frecuencia, probabilidad, tendencia, ultima_actualizacion)
            WITH base AS (
                SELECT CASE
                    WHEN hora LIKE '12:%AM' THEN 0
                    WHEN hora LIKE '12:%PM' THEN 12
                    WHEN hora LIKE '%PM'    THEN CAST(SPLIT_PART(hora,':',1) AS INT)+12
                    ELSE                        CAST(SPLIT_PART(hora,':',1) AS INT)
                END AS hora_int,
                animalito, COUNT(*) AS total_hist
                FROM historico WHERE loteria='Lotto Activo' GROUP BY 1,2
            ),
            reciente AS (
                SELECT CASE
                    WHEN hora LIKE '12:%AM' THEN 0
                    WHEN hora LIKE '12:%PM' THEN 12
                    WHEN hora LIKE '%PM'    THEN CAST(SPLIT_PART(hora,':',1) AS INT)+12
                    ELSE                        CAST(SPLIT_PART(hora,':',1) AS INT)
                END AS hora_int,
                animalito, COUNT(*) AS total_rec
                FROM historico
                WHERE fecha >= CURRENT_DATE-INTERVAL '60 days' AND loteria='Lotto Activo'
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

        # Calcular y persistir rentabilidad por hora
        rentabilidad = await calcular_rentabilidad_horas(db)
        await actualizar_tabla_rentabilidad(db, rentabilidad)

        # Métricas globales
        res_hist = await db.execute(text(
            "SELECT COUNT(*) FROM historico WHERE loteria='Lotto Activo'"))
        total_hist = res_hist.scalar() or 0

        res_cal = await db.execute(text(
            "SELECT COUNT(*) FROM auditoria_ia WHERE acierto IS NOT NULL"))
        cal = res_cal.scalar() or 0

        res_ac = await db.execute(text(
            "SELECT COUNT(*) FROM auditoria_ia WHERE acierto=TRUE"))
        ac = res_ac.scalar() or 0

        # Top3 aciertos (cuando el resultado real está en alguna de las 3 predicciones)
        res_ac3 = await db.execute(text("""
            SELECT COUNT(*) FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora AND h.loteria='Lotto Activo'
            WHERE h.animalito IN (
                COALESCE(a.prediccion_1,'__'),
                COALESCE(a.prediccion_2,'__'),
                COALESCE(a.prediccion_3,'__')
            ) AND a.acierto IS NOT NULL
        """))
        ac3 = res_ac3.scalar() or 0

        ef1  = round(ac  / cal * 100, 1) if cal > 0 else 0
        ef3  = round(ac3 / cal * 100, 1) if cal > 0 else 0

        # Horas rentables
        horas_rentables = [h for h, d in rentabilidad.items() if d["es_rentable"]]

        await db.commit()
        return {
            "status":              "success",
            "registros_analizados": total_hist,
            "efectividad_top1":    ef1,
            "efectividad_top3":    ef3,
            "calibradas":          cal,
            "aciertos_top1":       ac,
            "aciertos_top3":       ac3,
            "horas_rentables":     horas_rentables,
            "rentabilidad_detalle": rentabilidad,
            "message": (
                f"✅ V9 calibrado. {total_hist:,} registros. "
                f"Top1: {ef1}% | Top3: {ef3}% | "
                f"Horas rentables: {len(horas_rentables)}"
            ),
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# CALIBRAR — actualiza aciertos vs histórico
# ══════════════════════════════════════════════════════
async def calibrar_predicciones(db) -> dict:
    try:
        result = await db.execute(text("""
            UPDATE auditoria_ia a
            SET
                acierto        = (LOWER(TRIM(a.animal_predicho)) = LOWER(TRIM(h.animalito))),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha AND a.hora = h.hora
              AND h.loteria = 'Lotto Activo'
              AND (a.acierto IS NULL OR a.resultado_real = 'PENDIENTE')
        """))
        cal = result.rowcount
        await db.commit()
        return {"calibradas": cal}
    except Exception as e:
        await db.rollback()
        return {"calibradas": 0, "error": str(e)}


# ══════════════════════════════════════════════════════
# LLENAR AUDITORÍA RETROACTIVA — guarda top3
# ══════════════════════════════════════════════════════
async def llenar_auditoria_retroactiva(db, fecha_desde=None, fecha_hasta=None, dias=30) -> dict:
    try:
        hoy = date.today()
        if fecha_desde is None: fecha_desde = hoy - timedelta(days=dias)
        if fecha_hasta is None: fecha_hasta = hoy - timedelta(days=1)
        if (fecha_hasta - fecha_desde).days > 366:
            return {"status": "error", "message": "Rango máximo 1 año"}

        pesos = await obtener_pesos_actuales(db)
        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
            ORDER BY fecha ASC, hora ASC
        """), {"desde": fecha_desde, "hasta": fecha_hasta})
        sorteos = res.fetchall()

        if not sorteos:
            return {"status": "ok", "procesados": 0,
                    "message": f"Sin sorteos entre {fecha_desde} y {fecha_hasta}"}

        insertados = 0; omitidos = 0; aciertos1 = 0; aciertos3 = 0

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s = sorteo
            dia_s = int(dia_s)
            try:
                res_e = await db.execute(text(
                    "SELECT 1 FROM auditoria_ia "
                    "WHERE fecha=:f AND hora=:h AND acierto IS NOT NULL AND prediccion_1 IS NOT NULL LIMIT 1"
                ), {"f": fecha_s, "h": hora_s})
                if res_e.fetchone():
                    omitidos += 1
                    continue

                d  = await calcular_deuda(db, hora_s, fecha_s)
                r  = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
                p  = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
                a  = await calcular_anti_racha(db, hora_s, fecha_s)
                s  = await calcular_secuencia(db, fecha_s)
                ce = await calcular_ciclo_exacto(db, hora_s, fecha_s)
                pen= await calcular_penalizacion_reciente(db, hora_s, fecha_s)

                sc = combinar_señales_v9(d, r, p, a, s, ce, pen, hora_s, pesos)
                if not sc:
                    continue

                confianza_idx, _ = calcular_indice_confianza_v9(sc)
                ranking = sorted(sc.items(), key=lambda x: x[1], reverse=True)
                pred1 = ranking[0][0].lower() if len(ranking) > 0 else None
                pred2 = ranking[1][0].lower() if len(ranking) > 1 else None
                pred3 = ranking[2][0].lower() if len(ranking) > 2 else None

                acerto1 = (pred1 == real.lower()) if pred1 else False
                acerto3 = real.lower() in [x for x in [pred1, pred2, pred3] if x]

                await db.execute(text("""
                    INSERT INTO auditoria_ia
                        (fecha, hora, animal_predicho, prediccion_1, prediccion_2, prediccion_3,
                         confianza_pct, resultado_real, acierto)
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
                    "r": real.lower(), "ac": acerto1,
                })
                insertados += 1
                if acerto1: aciertos1 += 1
                if acerto3: aciertos3 += 1

            except Exception:
                continue

        await db.commit()
        ef1 = round(aciertos1 / insertados * 100, 1) if insertados > 0 else 0
        ef3 = round(aciertos3 / insertados * 100, 1) if insertados > 0 else 0

        return {
            "status":               "success",
            "procesados":           insertados,
            "omitidos_ya_existian": omitidos,
            "aciertos_top1":        aciertos1,
            "aciertos_top3":        aciertos3,
            "efectividad_top1":     ef1,
            "efectividad_top3":     ef3,
            "message": (
                f"✅ {insertados} pred V9. "
                f"Top1: {ef1}% | Top3: {ef3}% ({aciertos3}/{insertados})"
            ),
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════
# BACKTEST V9
# ══════════════════════════════════════════════════════
async def backtest(db, fecha_desde, fecha_hasta, max_sorteos=100) -> dict:
    try:
        pesos = await obtener_pesos_actuales(db)
        res = await db.execute(text("""
            SELECT fecha, hora, animalito, EXTRACT(DOW FROM fecha)::int
            FROM historico
            WHERE fecha BETWEEN :desde AND :hasta AND loteria='Lotto Activo'
            ORDER BY fecha DESC, hora DESC LIMIT :lim
        """), {"desde": fecha_desde, "hasta": fecha_hasta, "lim": max_sorteos})
        sorteos = res.fetchall()
        if not sorteos:
            return {"error": "Sin datos en ese rango"}

        aciertos1 = 0; aciertos3 = 0; total = 0
        conf_alta_total = 0; conf_alta_ac1 = 0; conf_alta_ac3 = 0
        por_hora = {}
        detalle = []

        for sorteo in sorteos:
            fecha_s, hora_s, real, dia_s = sorteo
            dia_s = int(dia_s)
            d  = await calcular_deuda(db, hora_s, fecha_s)
            r  = await calcular_frecuencia_reciente(db, hora_s, fecha_s)
            p  = await calcular_patron_dia(db, hora_s, dia_s, fecha_s)
            a  = await calcular_anti_racha(db, hora_s, fecha_s)
            s  = await calcular_secuencia(db, fecha_s)
            ce = await calcular_ciclo_exacto(db, hora_s, fecha_s)
            pen= await calcular_penalizacion_reciente(db, hora_s, fecha_s)
            sc = combinar_señales_v9(d, r, p, a, s, ce, pen, hora_s, pesos)
            if not sc:
                continue

            confianza_idx, _ = calcular_indice_confianza_v9(sc)
            ranking = sorted(sc.items(), key=lambda x: x[1], reverse=True)
            pred1  = ranking[0][0].lower() if len(ranking) > 0 else ""
            top3_l = [x[0].lower() for x in ranking[:3]]

            acerto1 = (pred1 == real.lower())
            acerto3 = real.lower() in top3_l
            alta_c  = confianza_idx >= 30

            total += 1
            if acerto1: aciertos1 += 1
            if acerto3: aciertos3 += 1
            if alta_c:
                conf_alta_total += 1
                if acerto1: conf_alta_ac1 += 1
                if acerto3: conf_alta_ac3 += 1

            # Acumulado por hora
            if hora_s not in por_hora:
                por_hora[hora_s] = {"total": 0, "ac1": 0, "ac3": 0}
            por_hora[hora_s]["total"] += 1
            if acerto1: por_hora[hora_s]["ac1"] += 1
            if acerto3: por_hora[hora_s]["ac3"] += 1

            detalle.append({
                "fecha": str(fecha_s), "hora": hora_s,
                "pred1": pred1, "pred2": top3_l[1] if len(top3_l) > 1 else "",
                "pred3": top3_l[2] if len(top3_l) > 2 else "",
                "real": real, "acierto_top1": acerto1, "acierto_top3": acerto3,
                "confianza": confianza_idx,
            })

        ef1 = round(aciertos1 / total * 100, 1) if total > 0 else 0
        ef3 = round(aciertos3 / total * 100, 1) if total > 0 else 0
        ef1_alta = round(conf_alta_ac1 / conf_alta_total * 100, 1) if conf_alta_total > 0 else 0
        ef3_alta = round(conf_alta_ac3 / conf_alta_total * 100, 1) if conf_alta_total > 0 else 0

        # Resumen por hora con señal de rentabilidad
        resumen_horas = {}
        for hora, datos in por_hora.items():
            ef_h1 = round(datos["ac1"] / datos["total"] * 100, 1) if datos["total"] > 0 else 0
            ef_h3 = round(datos["ac3"] / datos["total"] * 100, 1) if datos["total"] > 0 else 0
            resumen_horas[hora] = {
                "total": datos["total"],
                "efectividad_top1": ef_h1,
                "efectividad_top3": ef_h3,
                "es_rentable": ef_h3 >= UMBRAL_RENTABILIDAD_TOP3,
                "ganancia_estimada_x100": round(ef_h3 / 100 * 30 - (1 - ef_h3 / 100), 2),
            }

        return {
            "fecha_desde":  str(fecha_desde),
            "fecha_hasta":  str(fecha_hasta),
            "total_sorteos": total,
            "efectividad_top1": ef1,
            "efectividad_top3": ef3,
            "conf_alta_top1": ef1_alta,
            "conf_alta_top3": ef3_alta,
            "conf_alta_n":    conf_alta_total,
            "pesos_usados":   pesos,
            "resumen_por_hora": resumen_horas,
            "umbral_rentabilidad": UMBRAL_RENTABILIDAD_TOP3,
            "mensaje": (
                f"V9: Top1 {ef1}% | Top3 {ef3}% | "
                f"Alta confianza → Top1 {ef1_alta}% | Top3 {ef3_alta}%"
            ),
            "detalle": detalle,
        }
    except Exception as e:
        return {"error": str(e)}


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
            pred  = re.sub(r'[^a-z]', '', (r[1] or '').lower())
            pred2 = re.sub(r'[^a-z]', '', (r[2] or '').lower())
            pred3_v = re.sub(r'[^a-z]', '', (r[3] or '').lower())
            pred4 = re.sub(r'[^a-z]', '', (r[4] or '').lower())
            real  = re.sub(r'[^a-z]', '', (r[5] or '').lower())
            bitacora.append({
                "hora":            r[0],
                "animal_predicho": pred.upper() if pred else "PENDIENTE",
                "prediccion_2":    pred2.upper() if pred2 else "",
                "prediccion_3":    pred3_v.upper() if pred3_v else "",
                "resultado_real":  real.upper() if real and real != 'pendiente' else "PENDIENTE",
                "acierto":         r[6],
                "img_predicho":    f"{pred}.png" if pred else "pendiente.png",
                "img_real":        f"{real}.png" if real and real != 'pendiente' else "pendiente.png",
                "confianza":       int(round(float(r[7] or 0))),
                "es_hora_rentable": bool(r[8]) if r[8] is not None else False,
            })
        return bitacora
    except Exception:
        return []


async def obtener_estadisticas(db) -> dict:
    try:
        res_ef = await db.execute(text("""
            SELECT
                COUNT(*),
                COUNT(CASE WHEN acierto=TRUE THEN 1 END),
                ROUND(COUNT(CASE WHEN acierto=TRUE THEN 1 END)::NUMERIC /
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END),0)*100,1)
            FROM auditoria_ia
        """))
        ef = res_ef.fetchone()

        # Top3 efectividad global
        res_ef3 = await db.execute(text("""
            SELECT COUNT(*) FROM auditoria_ia a
            JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora AND h.loteria='Lotto Activo'
            WHERE h.animalito IN (
                COALESCE(a.prediccion_1,'__'),
                COALESCE(a.prediccion_2,'__'),
                COALESCE(a.prediccion_3,'__')
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

        # Horas rentables
        res_rent = await db.execute(text("""
            SELECT hora, efectividad_top3, es_rentable
            FROM rentabilidad_hora
            WHERE es_rentable=TRUE ORDER BY efectividad_top3 DESC
        """))
        horas_rentables = [{"hora": r[0], "ef_top3": float(r[1])} for r in res_rent.fetchall()]

        pesos = await obtener_pesos_actuales(db)
        res_gen = await db.execute(text("SELECT COALESCE(MAX(generacion),1) FROM motor_pesos"))
        generacion = res_gen.scalar() or 1

        return {
            "efectividad_global":   float(ef[2] or 0),
            "efectividad_top3":     ef3,
            "total_auditado":       total_cal,
            "aciertos_total":       int(ef[1] or 0),
            "aciertos_top3":        ac3,
            "aciertos_hoy":         int(hoy[0] or 0),
            "sorteos_hoy":          int(hoy[1] or 0),
            "top_animales":         top_animales,
            "total_historico":      total_hist,
            "horas_rentables":      horas_rentables,
            "pesos_actuales":       pesos,
            "generacion":           generacion,
        }
    except Exception:
        return {
            "efectividad_global": 0, "efectividad_top3": 0,
            "aciertos_hoy": 0, "sorteos_hoy": 0,
            "total_historico": 0, "top_animales": [],
            "horas_rentables": [], "generacion": 1,
        }
