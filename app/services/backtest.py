# app/services/backtest.py

from sqlalchemy import text

async def entrenar_modelo(db):

    # ejemplo simple
    await db.execute(text("""
        INSERT INTO metricas(total, aciertos, errores, precision)
        VALUES (0,0,0,0)
    """))

    await db.commit()

    return {"ok": True}
