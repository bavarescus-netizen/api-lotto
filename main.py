from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import asyncio

from db import get_db
from app.routes.prediccion import router as prediccion_router
from app.routes.historico import router as historico_router

# 👇 IMPORTAMOS TU SCRIPT AUTOMÁTICO
from service.actualizar import actualizar


app = FastAPI(title="API Lotto Activo 🚀")

app.include_router(prediccion_router)
app.include_router(historico_router)


# =========================
# ENDPOINTS NORMALES
# =========================

@app.get("/")
async def root(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("select 'API Lotto funcionando 🚀'"))
    return {"estado": result.scalar()}


@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT 1"))
    return {"db": "ok"}


# =========================
# WORKER AUTOMÁTICO (GRATIS)
# =========================

async def worker_loop():
    while True:
        try:
            print("🕐 Ejecutando actualización automática...")
            await actualizar()
            print("✅ Actualización completada")
        except Exception as e:
            print("❌ Error en actualización:", e)

        # esperar 1 hora
        await asyncio.sleep(3600)


@app.on_event("startup")
async def start_worker():
    asyncio.create_task(worker_loop())
