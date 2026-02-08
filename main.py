from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from db import get_db
from app.routes.prediccion import router as prediccion_router
from app.routes.historico import router as historico_router
from app.routes.metricas import router as metricas_router  # 👈 NUEVO

app = FastAPI(title="API Lotto Activo 🚀")

app.include_router(prediccion_router)
app.include_router(historico_router)
app.include_router(metricas_router)  # 👈 NUEVO


@app.get("/")
async def root(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("select 'API Lotto funcionando 🚀'"))
    return {"estado": result.scalar()}


@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT 1"))
    return {"db": "ok"}
