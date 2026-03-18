"""
MOTOR V10.1 — LOTTOAI PRO (UNIFICADO)
=====================================
FIXES:
  1. MAPA UNIFICADO: Sincronizado con Scheduler (0-36).
  2. MARKOV V2: Analiza los últimos 2 resultados para predecir el 3ero.
  3. FEEDBACK LOOP: Consulta auditoria_ia para bajar confianza si hay mala racha.
  4. ESTABILIDAD: Optimizado para Neon + Render Free.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
import pytz, math

# ══════════════════════════════════════════════════════
# CATÁLOGO OFICIAL UNIFICADO (Lotto Activo)
# ══════════════════════════════════════════════════════
MAPA_ANIMALES = {
    "0": "delfin", "00": "ballena", "1": "carnero", "2": "toro", "3": "ciempies",
    "4": "alacran", "5": "leon", "6": "rana", "7": "perico", "8": "raton",
    "9": "caballo", "10": "tigre", "11": "gato", "12": "caballo", "13": "mono",
    "14": "paloma", "15": "zorro", "16": "oso", "17": "pavo", "18": "burro",
    "19": "chivo", "20": "cochino", "21": "gallo", "22": "camello", "23": "cebra",
    "24": "iguana", "25": "gallina", "26": "vaca", "27": "perro", "28": "zamuro",
    "29": "elefante", "30": "caiman", "31": "lapa", "32": "ardilla", "33": "pescado",
    "34": "venado", "35": "jirafa", "36": "culebra"
}

async def obtener_transiciones_markov_v2(db: AsyncSession, loteria: str):
    """Lógica de Markov de 2do Orden: ¿Qué sale después de los últimos DOS?"""
    query = text("""
        WITH secuencias AS (
            SELECT animal, 
                   LAG(animal, 1) OVER (ORDER BY fecha DESC, hora DESC) as ant1,
                   LAG(animal, 2) OVER (ORDER BY fecha DESC, hora DESC) as ant2
            FROM historico 
            WHERE loteria = :loteria
            LIMIT 1000
        )
        SELECT animal, COUNT(*) as peso
        FROM secuencias
        WHERE ant1 = (SELECT animal FROM historico WHERE loteria=:loteria ORDER BY fecha DESC, hora DESC LIMIT 1)
          AND ant2 = (SELECT animal FROM historico WHERE loteria=:loteria ORDER BY fecha DESC, hora DESC OFFSET 1 LIMIT 1)
        GROUP BY animal 
        ORDER BY peso DESC LIMIT 5
    """)
    res = await db.execute(query, {"loteria": loteria})
    return {row[0]: row[1] for row in res.fetchall()}

async def generar_prediccion(db: AsyncSession, loteria: str, hora_sorteo: int):
    # 1. Obtener datos básicos (Frecuencia y Deuda)
    # [Aquí iría tu lógica actual de Score base...]
    
    # 2. Integrar Markov V2
    markov = await obtener_transiciones_markov_v2(db, loteria)
    
    # 3. FILTRO DE APRENDIZAJE (Feedback Loop)
    query_efectividad = text("""
        SELECT COUNT(CASE WHEN acierto=TRUE THEN 1 END)::float / NULLIF(COUNT(*), 0) as ratio
        FROM auditoria_ia
        WHERE hora = :hora AND fecha > CURRENT_DATE - INTERVAL '7 days'
    """)
    res_ef = await db.execute(query_efectividad, {"hora": hora_sorteo})
    ratio_acierto = res_ef.scalar() or 0.05 # 5% por defecto si no hay datos

    # Lógica de Confianza Final
    puntuaciones = {}
    for num, nombre in MAPA_ANIMALES.items():
        score = 10.0 # Score base
        if nombre in markov:
            score += (markov[nombre] * 5) # Peso extra por secuencia
        
        # Penalizar si la IA ha fallado mucho en esta hora últimamente
        if ratio_acierto < 0.03: # Menos del 3% (azar puro)
            score *= 0.5
            
        puntuaciones[nombre] = score

    # Ordenar y sacar Top 3
    top = sorted(puntuaciones.items(), key=lambda x: x[1], reverse=True)[:3]
    
    confianza = min(int(top[0][1] + (ratio_acierto * 100)), 99)
    operar = "OPERAR" if confianza >= 25 else "NO OPERAR"

    return {
        "lotto": loteria,
        "hora": hora_sorteo,
        "predicciones": [t[0] for t in top],
        "confianza": confianza,
        "estado": operar,
        "metodo": "MarkovV2 + Feedback"
    }
