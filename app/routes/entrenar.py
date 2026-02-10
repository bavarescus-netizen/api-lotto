from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db

from app.services.backtest import entrenar_modelo

router = APIRouter()

@router.get("/entrenar")
async def entrenar(db: AsyncSession = Depends(get_db)):
    resultado = await entrenar_modelo(db)
    return resultado
