from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from app.services.motor_v4 import entrenar_modelo_v4

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        return await entrenar_modelo_v4(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
