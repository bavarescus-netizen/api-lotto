import os
import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# 🔥 FIX NEON + ASYNCPG
DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
DATABASE_URL = DATABASE_URL.replace("sslmode=require", "ssl=true")

engine = create_async_engine(DATABASE_URL, echo=False)


async def cargar():

    print("📂 Leyendo historial.xlsx ...")
    df = pd.read_excel("historial.xlsx")

    async with engine.begin() as conn:

        print("🛠 Creando tabla si no existe...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS historico (
                fecha TIMESTAMP,
                hora TEXT,
                animalito TEXT,
                loteria TEXT
            )
        """))

        print("⬆ Insertando datos en Neon...")

        for _, row in df.iterrows():
            await conn.execute(
                text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:fecha, :hora, :animalito, :loteria)
                """),
                {
                    "fecha": row["fecha"],
                    "hora": row["hora"],
                    "animalito": row["animalito"],
                    "loteria": row["loteria"]
                }
            )

    print("✅ Carga completada correctamente")


asyncio.run(cargar())
