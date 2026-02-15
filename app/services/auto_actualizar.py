import os
from datetime import datetime
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL, echo=False)

async def actualizar_incremental():

    async with engine.begin() as conn:
        result = await conn.execute(
            text("SELECT MAX(fecha) FROM historico")
        )
        ultima_fecha = result.scalar()

    if not ultima_fecha:
        ultima_fecha = datetime(2024, 1, 1).date()

    df = pd.read_excel("data/historial.xlsx")

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    df = df.dropna(subset=["fecha"])

    df = df[df["fecha"] >= ultima_fecha]

    if df.empty:
        return 0

    df["hora"] = df["hora"].astype(str).str.strip()
    df["animalito"] = df["animalito"].astype(str).str.strip()
    df["loteria"] = df["loteria"].astype(str).str.strip()

    registros = df.to_dict(orient="records")

    async with engine.begin() as conn:
        await conn.execute(text("""
            INSERT INTO historico (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animalito, :loteria)
            ON CONFLICT (fecha, hora, loteria) DO NOTHING
        """), registros)

    return len(registros)
