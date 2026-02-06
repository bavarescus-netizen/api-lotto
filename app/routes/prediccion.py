from fastapi import APIRouter

router = APIRouter(prefix="/prediccion", tags=["prediccion"])


@router.get("/")
async def prediccion():
    return {"mensaje": "predicción lista"}
