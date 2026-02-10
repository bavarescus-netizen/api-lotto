from collections import Counter
from sqlalchemy import text

async def entrenar_modelo(db):

    res = await db.execute(text("""
        SELECT animalito
        FROM historico
        ORDER BY fecha, hora
    """))

    rows = [r[0] for r in res.fetchall()]

    historial = []
    aciertos = 0
    total = 0

    for real in rows:

        if len(historial) < 200:
            historial.append(real)
            continue

        conteo = Counter(historial)
        pred = conteo.most_common(1)[0][0]

        if pred == real:
            aciertos += 1

        total += 1
        historial.append(real)

    precision = (aciertos / total) * 100

    return {
        "total": total,
        "aciertos": aciertos,
        "fallos": total - aciertos,
        "precision": round(precision, 2)
    }

