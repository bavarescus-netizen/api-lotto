from fastapi import APIRouter
from app.services.motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/")
async def obtener_prediccion():
    resultado = await generar_prediccion()
    return resultado

