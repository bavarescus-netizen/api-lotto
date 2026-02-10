
import asyncio
import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL, echo=False)


# =========================================
# FUNCION PRINCIPAL (INCREMENTAL REAL)
# =========================================
async def actualizar():

    print("🔎 Buscando última fecha en Neon...")

    async with engine.begin() as conn:

        # 1️⃣ última fecha guardada
        result = await conn.execute(
            text("SELECT MAX(fecha) FROM historico")
        )
        ultima_fecha = result.scalar()

    if not ultima_fecha:
        print("⚠️ BD vacía, usar histórico completo")
        ultima_fecha = datetime(2024, 1, 1).date()

    print("📅 Última fecha:", ultima_fecha)

    # =========================================
    # 2️⃣ DESCARGAR SOLO NUEVOS DATOS
    # =========================================
    # 👉 aquí conectas tu scraper real
    # Por ahora leemos el Excel actualizado

    df = pd.read_excel("data/historial.xlsx")

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    df = df.dropna(subset=["fecha"])

    # SOLO registros nuevos
    df = df[df["fecha"] >= ultima_fecha]

    if df.empty:
        print("✅ No hay datos nuevos")
        return

    print(f"⬆ Nuevos registros encontrados: {len(df)}")

    df["hora"] = df["hora"].astype(str).str.strip()
    df["animalito"] = df["animalito"].astype(str).str.strip()
    df["loteria"] = df["loteria"].astype(str).str.strip()

    registros = df.to_dict(orient="records")

    # =========================================
    # 3️⃣ INSERTAR SIN DUPLICADOS
    # =========================================
    async with engine.begin() as conn:

        await conn.execute(text("""
            INSERT INTO historico (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animalito, :loteria)
            ON CONFLICT (fecha, hora, loteria) DO NOTHING
        """), registros)

    print("🚀 Actualización completada")
