from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v4 import generar_prediccion # <--- Solo V4

router = APIRouter()

@router.get("/prediccion")
async def obtener_prediccion(db: AsyncSession = Depends(get_db)):
    # Ejecutamos la lógica de alto rendimiento
    resultado = await generar_prediccion(db)
    return resultado


