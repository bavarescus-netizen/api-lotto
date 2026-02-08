import asyncio
import os
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL, echo=False)


# =========================================
# 1️⃣ OBTENER ÚLTIMA FECHA EN NEON
# =========================================
async def obtener_ultima_fecha(conn):
    res = await conn.execute(text("SELECT MAX(fecha) FROM historico"))
    fecha = res.scalar()

    if fecha is None:
        return datetime(2025, 1, 1).date()  # inicio si está vacío

    return fecha


# =========================================
# 2️⃣ DESCARGAR DATOS WEB (histórico real)
# =========================================
def descargar_dia(fecha):
    fecha_str = fecha.strftime("%Y-%m-%d")

    url = f"https://loteriadehoy.com/animalito/lottoactivo/historico/{fecha_str}/{fecha_str}/"

    tablas = pd.read_html(url)

    df = tablas[0]
    df.columns = ["hora", "animalito"]

    df["fecha"] = fecha
    df["loteria"] = "Lotto Activo"

    return df[["fecha", "hora", "animalito", "loteria"]]


# =========================================
# 3️⃣ WORKER PRINCIPAL
# =========================================
async def actualizar():

    async with engine.begin() as conn:

        ultima = await obtener_ultima_fecha(conn)
        hoy = datetime.now().date()

        print("Última fecha:", ultima)

        fecha_actual = ultima + timedelta(days=1)

        todos = []

        while fecha_actual <= hoy:
            try:
                df = descargar_dia(fecha_actual)
                todos.append(df)
                print("✔ Descargado:", fecha_actual)
            except:
                print("⚠ Sin datos:", fecha_actual)

            fecha_actual += timedelta(days=1)

        if not todos:
            print("Nada nuevo para actualizar")
            return

        df_final = pd.concat(todos)

        registros = df_final.to_dict(orient="records")

        await conn.execute(text("""
            INSERT INTO historico (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animalito, :loteria)
            ON CONFLICT (fecha, hora, loteria) DO NOTHING
        """), registros)

        print("🚀 Nuevos registros insertados:", len(registros))


asyncio.run(actualizar())
