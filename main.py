from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import engine
from sqlalchemy import text
from app.services.motor_v4 import generar_prediccion
from app.services.scraper import obtener_ultimo_resultado
from app.routes import prediccion, entrenar

app = FastAPI(title="Lotto AI V4")

# CORRECCIÓN DE RUTA: Apuntamos a tu carpeta 'imagenes'
# Esto permite que /static/imagenes/perro.png funcione
app.mount("/static/imagenes", StaticFiles(directory="imagenes"), name="imagenes")

app.include_router(prediccion.router)
app.include_router(entrenar.router)

async def tarea_automatica():
    async with engine.begin() as conn:
        print("🔍 Scraper: Buscando nuevo resultado...")
        data = obtener_ultimo_resultado()
        
        if data:
            # 1. Verificar si ya existe para no duplicar
            check = await conn.execute(text(
                "SELECT id FROM historico WHERE fecha=:fecha AND hora=:hora"
            ), {"fecha": data["fecha"], "hora": data["hora"]})
            
            if not check.fetchone():
                # 2. Guardar nuevo resultado
                await conn.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:fecha, :hora, :animalito, :loteria)
                """), data)
                print(f"✅ Guardado: {data['animalito']} a las {data['hora']}")
                
                # 3. Commit manual para asegurar que los datos estén listos
                await conn.commit()
            else:
                print(f"⏳ Sin cambios. El sorteo de las {data['hora']} ya existe.")

@app.on_event("startup")
async def startup():
    scheduler = AsyncIOScheduler()
    # Ejecuta el scraper cada 5 minutos
    scheduler.add_job(tarea_automatica, 'interval', minutes=5)
    scheduler.start()
    print("🚀 Scheduler iniciado: Buscando sorteos cada 5 minutos.")

@app.get("/")
async def root():
    return {"status": "Sistema V4 Operativo", "base_datos": "29k registros"}
