from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from services.metricas_service import obtener_metricas

router = APIRouter()

@router.get("/metricas")
async def metricas(db: AsyncSession = Depends(get_db)):
    return await obtener_metricas(db)
