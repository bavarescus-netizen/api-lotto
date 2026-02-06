from fastapi import APIRouter

router = APIRouter(prefix="/historico", tags=["historico"])


@router.get("/")
async def historico():
    return {"mensaje": "histórico listo"}
