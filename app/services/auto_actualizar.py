import asyncio
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"ssl": True}
)

async def actualizar():
    # Aquí cambia la lógica de scraping / API real de tus datos
    datos_nuevos = {
        "fecha": datetime.now().date(),
        "hora": datetime.now().strftime("%I:%M %p"),
        "animalito": "???",   # remplazar con scraping real
        "loteria": "Lotto Activo"
    }

    async with engine.begin() as conn:
        await conn.execute(text("""
            INSERT INTO sorteos (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animalito, :loteria)
        """), datos_nuevos)

    print("🕐 Registro nuevo guardado:", datos_nuevos)

asyncio.run(actualizar())
