from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

sys.path.append(os.getcwd())

try:
    from db import get_db
except ImportError:
    from db import get_db

router = APIRouter(prefix="/historico", tags=["Histórico"])

@router.get("/")
async def get_historico(db: AsyncSession = Depends(get_db)):
    return {"mensaje": "Ruta de histórico activa"}
