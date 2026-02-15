import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import engine

# Importamos tus rutas
from app.routes import prediccion, entrenar, historico, metricas, stats

app = FastAPI(title="Lotto AI - Ecosistema V4")

# 1. Montar Imágenes (Directorio raíz según tu esquema)
app.mount("/static/imagenes", StaticFiles(directory="imagenes"), name="imagenes")

# 2. Incluir todas tus rutas de la carpeta /routes
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(metricas.router)
app.include_router(stats.router)

# 3. Servir el Dashboard Principal
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    ruta = os.path.join("app", "routes", "dashboard.html")
    with open(ruta, "r", encoding="utf-8") as f:
        return f.read()

# 4. Configuración del Scheduler (Automatización)
# Importamos la tarea desde tu scraper o service
from app.services.scraper import obtener_ultimo_resultado
from sqlalchemy import text

async def ciclo_automatico():
    async with engine.begin() as conn:
        print("🤖 Ciclo automático iniciado...")
        data = obtener_ultimo_resultado()
        if data:
            # Lógica para guardar si no existe
            query = text("SELECT id FROM historico WHERE fecha=:fecha AND hora=:hora")
            check = await conn.execute(query, {"fecha": data["fecha"], "hora": data["hora"]})
            if not check.fetchone():
                await conn.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:fecha, :hora, :animalito, :loteria)
                """), data)
                await conn.commit()
                print(f"✅ Nuevo resultado guardado: {data['animalito']}")

@app.on_event("startup")
async def startup_event():
    scheduler = AsyncIOScheduler()
    # Ejecuta el scraper cada 5 minutos
    scheduler.add_job(ciclo_automatico, 'interval', minutes=5)
    scheduler.start()
    print("🚀 Sistema V4 Online: Scheduler y Rutas cargadas.")
