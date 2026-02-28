import os
import re
import asyncio # <--- Nuevo: Para manejar el bot en segundo plano
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Importamos tus módulos de la carpeta app.routes
from db import get_db
from app.routes import entrenar, stats, historico, metricas, prediccion
from app.core.scheduler import ciclo_infinito # <--- Nuevo: Importamos tu bot

app = FastAPI(title="LOTTOAI PRO")

# 1. Registro de Routers (Conexión de botones)
app.include_router(entrenar.router)
app.include_router(stats.router)
app.include_router(historico.router)
app.include_router(metricas.router)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Configuración de Archivos Estáticos e Imágenes
app.mount("/imagenes", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="imagenes")

# 3. Configuración de Templates (Ruta donde está tu HTML)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

# EVENTO DE ARRANQUE: Aquí es donde el bot se despierta
@app.on_event("startup")
async def iniciar_bot_vigilancia():
    # Lanzamos el ciclo infinito sin bloquear la web
    asyncio.create_task(ciclo_infinito())
    print("🚀 BOT DE VIGILANCIA: Activado y acechando resultados...")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Llamada a la lógica de predicción
        res_ia = await prediccion.generar_prediccion(db)
        
        # Consulta para la Auditoría (Últimos 12 sorteos)
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto 
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, h.hora DESC LIMIT 12
        """)
        res_db = await db.execute(query)
        
        ultimos_db = []
        for r in res_db.fetchall():
            # Limpiar nombre para la imagen (ej: "05 LEON" -> "leon.png")
            nombre_animal = re.sub(r'[^a-z]', '', r[2].lower())
            ultimos_db.append({
                "hora": r[1],
                "animal": r[2],
                "img": f"{nombre_animal}.png",
                "acierto": r[3]
            })

        # Obtener Efectividad de la tabla metrics (con seguro por si no existe)
        try:
            res_met = await db.execute(text("SELECT precision FROM metrics WHERE id = 1"))
            metric = res_met.fetchone()
            efectividad = round(metric[0], 1) if metric else 0.0
        except:
            efectividad = 0.0

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db,
            "efectividad": efectividad
        })
    except Exception as e:
        print(f"Error en Home: {e}")
        return HTMLResponse(content=f"Error en el servidor: {str(e)}", status_code=500)
