from fastapi import APIRouter
from app.services.motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/")
async def get_prediccion():
    return await generar_prediccion()
