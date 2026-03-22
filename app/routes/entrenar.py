"""
ENTRENAR.PY — Ruta de entrenamiento
Actualizado V10 — /procesar movido a main.py con motor_v10
"""
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v10 import entrenar_modelo

router = APIRouter()

@router.get("/entrenar")
async def entrenar(db: AsyncSession = Depends(get_db)):
    resultado = await entrenar_modelo(db)
    status_code = 200 if resultado.get("status") == "success" else 500
    return JSONResponse(resultado, status_code=status_code)
