from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import engine
from app.services.motor_v4 import generar_prediccion
from app.services.scraper import obtener_ultimo_resultado
from app.routes import prediccion, entrenar

app = FastAPI()

# IMPORTANTE: Servir imágenes
app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(prediccion.router)
app.include_router(entrenar.router)

async def tarea_automatica():
    async with engine.begin() as conn:
        data = obtener_ultimo_resultado()
        if data:
            # Lógica de guardado y evaluación (resumida)
            pass

@app.on_event("startup")
async def startup():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(tarea_automatica, 'interval', minutes=5)
    scheduler.start()
