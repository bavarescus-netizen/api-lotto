from fastapi import APIRouter, Depends
from sqlalchemy import text
from db import engine

router = APIRouter()

@router.get("/datos-historico")
async def obtener_datos_historico():
    async with engine.connect() as conn:
        # Traemos los últimos 20 resultados
        result = await conn.execute(text("""
            SELECT fecha, hora, animalito 
            FROM historico 
            ORDER BY id DESC LIMIT 20
        """))
        # Convertimos a lista de diccionarios
        historial = [{"fecha": r[0], "hora": r[1], "animal": r[2]} for r in result]
        return historial
