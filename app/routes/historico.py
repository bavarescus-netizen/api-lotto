from fastapi import APIRouter

router = APIRouter(prefix="/historico", tags=["Historial"])

@router.get("/")
async def get_historico():
    # El frontend espera una LISTA [], no un diccionario
    return [
        {"hora": "09:00 AM", "animal": "DELFÍN", "numero": "0"},
        {"hora": "10:00 AM", "animal": "BALLENA", "numero": "00"}
    ]
