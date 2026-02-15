import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import text
from db import engine

# Importamos tus servicios
from app.services.scraper import obtener_ultimo_resultado
from app.routes import prediccion, entrenar

app = FastAPI(title="Lotto AI V4 - Sistema Unificado")

# 1. MONTAR IMÁGENES: Para que se vean en el HTML
app.mount("/static/imagenes", StaticFiles(directory="imagenes"), name="imagenes")

# 2. INCLUIR RUTAS DE DATOS (API)
app.include_router(prediccion.router)
app.include_router(entrenar.router)

# 3. SERVIR EL DASHBOARD (INDEX)
@app.get("/", response_class=HTMLResponse)
async def read_dashboard():
    # Buscamos el archivo HTML que creamos
    ruta_html = os.path.join("app", "routes", "dashboard.html")
    with open(ruta_html, "r", encoding="utf-8") as f:
        return f.read()

# 4. LÓGICA AUTOMÁTICA (SCRAPER)
async def tarea_automatica():
    async with engine.begin() as conn:
        print("🔍 Scraper: Buscando nuevo resultado...")
        data = obtener_ultimo_resultado()
        
        if data:
            # Verificar duplicados
            check = await conn.execute(text(
                "SELECT id FROM historico WHERE fecha=:fecha AND hora=:hora"
            ), {"fecha": data["fecha"], "hora": data["hora"]})
            
            if not check.fetchone():
                await conn.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:fecha, :hora, :animalito, :loteria)
                """), data)
                print(f"✅ Guardado: {data['animalito']} a las {data['hora']}")
                await conn.commit()
            else:
                print(f"⏳ Sin cambios. Sorteo {data['hora']} ya existe.")

# 5. INICIO DEL SISTEMA
@app.on_event("startup")
async def startup():
    # Iniciar el reloj automático
    scheduler = AsyncIOScheduler()
    scheduler.add_job(tarea_automatica, 'interval', minutes=5)
    scheduler.start()
    print("🚀 App V4 Iniciada: Dashboard listo y Scraper corriendo.")
