from fastapi import APIRouter, Depends
from db import get_db
from services.metricas_service import obtener_metricas

router = APIRouter()

@router.get("/metricas")
async def metricas(db=Depends(get_db)):
    return await obtener_metricas(db)
