# services/backtest.py

from collections import Counter
from sqlalchemy import text
from datetime import timedelta

async def entrenar_modelo(db):

    print("🚀 INICIANDO BACKTEST CON 28K REGISTROS")

    res = await db.execute(text("""
        SELECT fecha, hora, animalito
        FROM historico
        ORDER BY fecha, hora
    """))

    rows = res.fetchall()

    aciertos = 0
    total = 0

    historial = []

    for fecha, hora, real in rows:

        if len(historial) < 200:
            historial.append(real)
            continue

        conteo = Counter(historial)

        pred = conteo.most_common(1)[0][0]

        if pred == real:
            aciertos += 1

        total += 1
        historial.append(real)

    precision = aciertos / total

    return {
        "total": total,
        "aciertos": aciertos,
        "precision": round(precision*100,2)
    }

