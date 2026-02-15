from sqlalchemy import text
from db import engine


async def guardar_sorteo(data):

    async with engine.begin() as conn:

        # evita duplicados
        existe = await conn.execute(text("""
            SELECT 1 FROM historico
            WHERE fecha=:fecha AND hora=:hora AND loteria=:loteria
        """), data)

        if existe.first():
            return False

        await conn.execute(text("""
            INSERT INTO historico (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animalito, :loteria)
        """), data)

    return True
