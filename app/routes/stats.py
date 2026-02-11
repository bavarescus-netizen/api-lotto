from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v4 import analizar_estadisticas

router = APIRouter(prefix="/stats", tags=["Stats"])


@router.get("/")
async def stats(db: AsyncSession = Depends(get_db)):
    return await analizar_estadisticas(db)
