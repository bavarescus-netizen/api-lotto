from fastapi import APIRouter
from app.services.auto_actualizar import actualizar_incremental

router = APIRouter(prefix="/auto", tags=["Auto"])

@router.get("/actualizar")
async def actualizar():
    nuevos = await actualizar_incremental()
    return {"nuevos_registros": nuevos}
