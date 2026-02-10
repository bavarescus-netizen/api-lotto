import random
from collections import Counter
from datetime import datetime, timedelta
from sqlalchemy import text


async def generar_prediccion(db):

    ahora = datetime.now()
    hora_actual = ahora.strftime("%I:00 %p")

    # ===============================
    # 1️⃣ CREAR TABLAS SI NO EXISTEN
    # ===============================
    await db.execute(text("""
    CREATE TABLE IF NOT EXISTS predicciones(
        id SERIAL PRIMARY KEY,
        fecha DATE,
        hora TEXT,
        animal TEXT,
        score FLOAT,
        acertado BOOLEAN DEFAULT NULL
    )
    """))

    await db.execute(text("""
    CREATE TABLE IF NOT EXISTS metricas(
        id SERIAL PRIMARY KEY,
        total INT,
        aciertos INT,
        errores INT,
        precision FLOAT,
        fecha TIMESTAMP DEFAULT NOW()
    )
    """))

    # ===============================
    # 2️⃣ HISTÓRICO COMPLETO
    # ===============================
    res = await db.execute(text("""
        SELECT animalito FROM historico
    """))

    animales = [r[0] for r in res.fetchall()]

    # ===============================
    # 3️⃣ FRECUENCIA GLOBAL
    # ===============================
    conteo_global = Counter(animales)

    # ===============================
    # 4️⃣ FRECUENCIA POR HORA
    # ===============================
    res_hora = await db.execute(text("""
        SELECT animalito
        FROM historico
        WHERE hora = :hora
    """), {"hora": hora_actual})

    animales_hora = [r[0] for r in res_hora.fetchall()]
    conteo_hora = Counter(animales_hora)

    # ===============================
    # 5️⃣ NÚMEROS FRÍOS (últimos 50 sorteos)
    # ===============================
    res_frios = await db.execute(text("""
        SELECT animalito
        FROM historico
        ORDER BY fecha DESC, hora DESC
        LIMIT 50
    """))

    recientes = [r[0] for r in res_frios.fetchall()]
    set_recientes = set(recientes)

    todos = list(conteo_global.keys())

    scores = {}

    for animal in todos:

        score = 0

        # peso histórico
        score += conteo_global[animal] * 0.4

        # peso por hora
        score += conteo_hora.get(animal, 0) * 0.5

        # bonus frío
        if animal not in set_recientes:
            score += 5

        scores[animal] = score

    # ===============================
    # 6️⃣ TOP 3
    # ===============================
    top3 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:3]

    resultado = []

    for animal, score in top3:
        resultado.append({
            "animal": animal,
            "score": round(score, 2)
        })

        # guardar predicción
        await db.execute(text("""
            INSERT INTO predicciones (fecha, hora, animal, score)
            VALUES (:fecha, :hora, :animal, :score)
        """), {
            "fecha": ahora.date(),
            "hora": hora_actual,
            "animal": animal,
            "score": score
        })

    await db.commit()

    return {
        "hora": hora_actual,
        "top3": resultado
    }
