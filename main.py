import asyncio
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import engine, get_db
from sqlalchemy import text

# Importamos tus módulos estrella
from app.services.scraper import obtener_ultimo_resultado
from app.services.motor_v4 import generar_prediccion
from app.services.evaluar_prediccion import evaluar
from app.routes import prediccion, entrenar

app = FastAPI(title="Lotto AI - Sistema Vivo V4")

# Incluimos las rutas para el Dashboard y Entrenamiento
app.include_router(prediccion.router)
app.include_router(entrenar.router)

async def ciclo_de_trabajo():
    """Este es el bucle que corre cada hora automáticamente"""
    async with engine.begin() as conn:
        print("🔍 Buscando nuevos sorteos...")
        resultado_web = obtener_ultimo_resultado()
        
        if not resultado_web:
            print("⚠️ No se pudo obtener datos de la web.")
            return

        # 1. Verificar si el resultado ya existe en la DB
        res = await conn.execute(text(
            "SELECT id FROM historico WHERE fecha=:f AND hora=:h"
        ), {"f": resultado_web["fecha"], "h": resultado_web["hora"]})
        
        if res.fetchone():
            print(f"✅ El sorteo de las {resultado_web['hora']} ya está registrado.")
            return

        # 2. Es un resultado NUEVO: Guardar en Histórico
        print(f"🆕 ¡Nuevo sorteo detectado! {resultado_web['animalito']} a las {resultado_web['hora']}")
        await conn.execute(text("""
            INSERT INTO historico (fecha, hora, animalito, loteria)
            VALUES (:fecha, :hora, :animalito, :loteria)
        """), resultado_web)

        # 3. Evaluar la predicción anterior (Cierre de ciclo)
        # Esto alimenta tus métricas para saber si el sistema está aprendiendo
        status_eval = await evaluar(resultado_web)
        print(f"📊 Evaluación de la jugada anterior: {status_eval}")

        # 4. Generar la Predicción para la PRÓXIMA HORA inmediatamente
        # Así, cuando abras el móvil, la predicción ya te estará esperando
        proxima_jugada = await generar_prediccion(conn)
        print(f"🔮 Predicción V4 lista para la próxima hora: {proxima_jugada['decision']}")

        await conn.commit()

# --- CONFIGURACIÓN DEL SCHEDULER (EL RELOJ) ---
@app.on_event("startup")
async def inicio_sistema():
    scheduler = AsyncIOScheduler()
    # Revisamos cada 5 minutos por si hay retrasos en la web de lotería
    scheduler.add_job(ciclo_de_trabajo, 'interval', minutes=5)
    scheduler.start()
    print("🚀 Sistema Vivo V4 Iniciado y Scheduler Corriendo...")

@app.get("/")
async def index():
    return {"status": "Online", "motor": "V4-Adaptive", "data_points": "29k+"}
