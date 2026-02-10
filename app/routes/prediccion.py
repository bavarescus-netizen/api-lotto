from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_prediccion_v3 import generar_prediccion


router = APIRouter()

@router.get("/prediccion")
async def prediccion(db: AsyncSession = Depends(get_db)):
    return await generar_prediccion(db)


