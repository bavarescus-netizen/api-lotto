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

    # Recalcular efectividad y umbral de rentabilidad
    ef_values = []
    query = """
        SELECT hora, efectividad_top3, es_rentable, total_sorteos
        FROM rentabilidad_hora
        ORDER BY hora
    """
    res = await db.execute(text(query))
    rows = res.fetchall()
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

    # Umbral de rentabilidad = percentil 75 de ef_top3 real
    if len(ef_values) >= 4:
        ef_sorted = sorted(ef_values)
        p75_idx   = int(len(ef_sorted) * 0.75)
        config["umbral_rentabilidad"] = round(ef_sorted[p75_idx], 1)

    # Configuración de confianza dinámica
    res = await db.execute(text("""
        SELECT COUNT(*) AS total, SUM(CASE WHEN acierto_top3 THEN 1 ELSE 0 END) AS ac3
        FROM auditoria_señales
        WHERE fecha >= CURRENT_DATE - INTERVAL '90 days'
          AND acierto_top3 IS NOT NULL
    """))
    r = res.fetchone()
    if r and int(r[0] or 0) >= 50:
        ef_global = float(r[1] or 0) / float(r[0]) * 100
        config["umbral_confianza"] = max(int(ef_global * 0.85), 20)

    # Peso anti-racha calculado
    azar_rep = 2.63
    query = """
        WITH pares AS (
            SELECT h1.hora, COUNT(*) AS total,
                SUM(CASE WHEN LOWER(TRIM(h1.animalito)) = LOWER(TRIM(h2.animalito)) THEN 1 ELSE 0 END) AS repeticiones
            FROM historico h1
            JOIN historico h2
                ON h1.hora = h2.hora
                AND h2.fecha = h1.fecha + INTERVAL '1 day'
                AND h1.loteria = 'Lotto Activo'
                AND h2.loteria = 'Lotto Activo'
            WHERE h1.fecha >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY h1.hora
        )
        SELECT hora, total, repeticiones,
            ROUND((repeticiones::float / NULLIF(total,0) * 100)::numeric, 2) AS pct_rep
        FROM pares
        WHERE total >= 20
    """
    res = await db.execute(text(query))
    rows = res.fetchall()
    for r in rows:
        hora    = r[0]
        pct_rep = float(r[3] or azar_rep)
        ratio   = pct_rep / azar_rep
        if ratio <= 0.30: peso = 0.42
        elif ratio <= 0.50: peso = 0.36
        elif ratio <= 0.70: peso = 0.30
        elif ratio <= 0.90: peso = 0.22
        elif ratio <= 1.10: peso = 0.18
        elif ratio <= 1.30: peso = 0.15
        else:               peso = 0.12
        config["peso_anti_racha_hora"][hora] = peso

    return config
