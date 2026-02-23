from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import JSONResponse
from db import get_db

router = APIRouter()

@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    # Versión segura para evitar el ImportError en Render
    return JSONResponse({
        "status": "success",
        "message": "Panel de estadísticas en mantenimiento"
    })
