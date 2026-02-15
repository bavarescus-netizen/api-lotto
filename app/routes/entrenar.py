from fastapi import APIRouter
from app.services.motor_v4 import entrenar_modelo_v4

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.post("/")
async def post_entrenar():
    return await entrenar_modelo_v4()
