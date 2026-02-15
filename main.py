from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import asyncio

from db import get_db

from app.routes.prediccion import router as prediccion_router
from app.routes.historico import router as historico_router
from app.routes.metricas import router as metricas_router
from app.routes.entrenar import router as entrenar_router
from app.routes.dashboard import router as dashboard_router

# 👇 IMPORTAMOS EL RELOJ AUTOMATICO
from app.core.scheduler import ciclo_infinito

app = FastAPI(title="API Lotto Activo 🚀")

# ROUTERS
app.include_router(prediccion_router)
app.include_router(historico_router)
app.include_router(metricas_router)
app.include_router(entrenar_router)
app.include_router(dashboard_router)


# ============================================
# INICIO AUTOMATICO DEL BOT (LO IMPORTANTE)
# ============================================
@app.on_event("startup")
async def iniciar_bot():
    print("🧠 Iniciando cerebro automático...")
    asyncio.create_task(ciclo_infinito())


# ============================================
# ENDPOINTS BASE
# ============================================
@app.get("/")
async def root(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("select 'API Lotto funcionando 🚀'"))
    return {"estado": result.scalar()}


@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT 1"))
    return {"db": "ok"}
