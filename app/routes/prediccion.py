from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from bd import get_db
from app.services.motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/")
async def get_prediccion(db: AsyncSession = Depends(get_db)):
    # Pasamos la DB al motor para que analice los últimos sorteos
    return await generar_prediccion(db)
