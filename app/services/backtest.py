from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from app.services.backtest import entrenar_modelo

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])


@router.get("/")
async def entrenar(background_tasks: BackgroundTasks,
                   db: AsyncSession = Depends(get_db)):

    background_tasks.add_task(entrenar_modelo, db)

    return {
        "status": "Entrenamiento iniciado en background 🚀"
    }
