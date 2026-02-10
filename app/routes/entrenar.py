# app/services/backtest.py

from sqlalchemy import text
from datetime import datetime

async def entrenar_modelo(db):

    print("🔥 Entrenamiento iniciado...")

    # ejemplo simple (ajusta con tu lógica real)
    result = await db.execute(text("""
        SELECT animalito
        FROM historico
        ORDER BY fecha DESC
        LIMIT 500
    """))

    data = result.fetchall()

    total = len(data)

    # simulación de entrenamiento
    score = total * 0.1

    await db.execute(text("""
        INSERT INTO metricas(total, aciertos, errores, precision)
        VALUES (:t, :a, :e, :p)
    """), {
        "t": total,
        "a": int(score),
        "e": total - int(score),
        "p": score / total if total else 0
    })

    await db.commit()

    print("✅ Entrenamiento finalizado")
