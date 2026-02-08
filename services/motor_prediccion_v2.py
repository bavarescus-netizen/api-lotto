# app/services/motor_prediccion_v2.py

from datetime import datetime, timedelta
from sqlalchemy import text
import pandas as pd

print("🚀 MOTOR V2 CARGADO")
print("🔥 MOTOR EJECUTANDOSE")

async def generar_prediccion(db):
    print("📊 CONSULTANDO BASE DE DATOS")

# ==============================
# CONFIGURACIÓN DE PESOS
# ==============================

PESO_LARGO   = 0.5   # histórico completo
PESO_SEMANA  = 0.3   # últimos 7 días
PESO_HOY     = 0.2   # hoy
PENALIZACION_RECIENTE = 0.6  # reduce 40%


# ==============================
# FUNCIÓN PRINCIPAL
# ==============================

async def generar_prediccion(db):

    ahora = datetime.now()
    hora_actual = ahora.strftime("%I:00 %p")

    hoy = ahora.date()
    hace_7 = hoy - timedelta(days=7)

    # ==============================
    # 1️⃣ CARGAR DATOS DESDE NEON
    # ==============================

    query = text("""
        SELECT fecha, hora, animalito
        FROM historico
        WHERE hora = :hora
    """)

    result = await db.execute(query, {"hora": hora_actual})
    rows = result.fetchall()

    if not rows:
        return {"error": "sin datos para esta hora"}

    df = pd.DataFrame(rows, columns=["fecha", "hora", "animalito"])
    df["fecha"] = pd.to_datetime(df["fecha"])

    # ==============================
    # 2️⃣ FRECUENCIAS
    # ==============================

    largo = df["animalito"].value_counts()

    semana = df[df["fecha"] >= pd.Timestamp(hace_7)]["animalito"].value_counts()

    hoy_df = df[df["fecha"] == pd.Timestamp(hoy)]
    hoy_freq = hoy_df["animalito"].value_counts()
print("📊 Consultando Neon...")

    # ==============================
    # 3️⃣ SCORE COMBINADO
    # ==============================

    animales = set(largo.index)

    scores = {}

    for a in animales:
        s = (
            largo.get(a, 0) * PESO_LARGO +
            semana.get(a, 0) * PESO_SEMANA +
            hoy_freq.get(a, 0) * PESO_HOY
        )
        scores[a] = s

    # ==============================
    # 4️⃣ PENALIZACIÓN RECIENTE
    # ==============================

    ultimos = await db.execute(text("""
        SELECT animalito
        FROM historico
        ORDER BY fecha DESC, hora DESC
        LIMIT 3
    """))

    recientes = [r[0] for r in ultimos]

    for r in recientes:
        if r in scores:
            scores[r] *= PENALIZACION_RECIENTE

    # ==============================
    # 5️⃣ PROBABILIDADES
    # ==============================

    total = sum(scores.values())

    probabilidades = {
        k: (v / total) for k, v in scores.items()
    }

    ranking = sorted(
        probabilidades.items(),
        key=lambda x: x[1],
        reverse=True
    )

    top3 = ranking[:3]

    confianza = round(top3[0][1] * 100, 2)

    # ==============================
    # 6️⃣ RESPUESTA FINAL
    # ==============================

    return {
        "hora": hora_actual,
        "top3": [x[0] for x in top3],
        "probabilidades": [round(x[1], 4) for x in top3],
        "confianza": confianza,
        "total_muestras": len(df)
    }
