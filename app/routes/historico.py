from fastapi import APIRouter

router = APIRouter(prefix="/historico", tags=["Historial"])

@router.get("/")
async def get_historico():
    return {"mensaje": "Historial de resultados próximamente"}
