from sqlalchemy import text
from db import engine


async def evaluar(data):

    async with engine.begin() as conn:

        pred = await conn.execute(text("""
            SELECT animalito FROM predicciones
            WHERE fecha=:fecha AND hora=:hora
        """), data)

        pred = pred.scalar()

        if not pred:
            return "sin_prediccion"

        acierto = pred == data["animalito"]

        await conn.execute(text("""
            INSERT INTO metricas(fecha,hora,predicho,real,acierto)
            VALUES (:fecha,:hora,:pred,:real,:acierto)
        """), {
            "fecha": data["fecha"],
            "hora": data["hora"],
            "pred": pred,
            "real": data["animalito"],
            "acierto": acierto
        })

    return "acierto" if acierto else "fallo"
