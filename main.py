import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date, timedelta
import pytz

# --- CONFIGURACIÓN DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
def encontrar_carpeta(nombre):
    for raiz, dirs, archivos in os.walk(BASE_DIR):
        if nombre in dirs: return os.path.join(raiz, nombre)
    return os.path.join(BASE_DIR, nombre)

app = FastAPI()

# Montar estáticos y templates
path_imgs = encontrar_carpeta("imagenes")
if os.path.exists(path_imgs):
    app.mount("/imagenes", StaticFiles(directory=path_imgs), name="imagenes")

path_routes = encontrar_carpeta("routes")
templates = Jinja2Templates(directory=path_routes)

from db import get_db
from app.services.motor_v4 import generar_prediccion
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# --- LÓGICA DE APRENDIZAJE AUTOMÁTICO ---
async def asegurar_apuestas_del_dia(db: AsyncSession):
    """Hace que la IA deje de estar en 0 cargando predicciones preventivas"""
    tz = pytz.timezone('America/Caracas')
    hoy = datetime.now(tz).date()
    horas = ["09:00 AM", "10:00 AM", "11:00 AM", "12:00 PM", "01:00 PM", "04:00 PM", "05:00 PM", "06:00 PM", "07:00 PM"]
    
    for h in horas:
        check = await db.execute(text("SELECT 1 FROM auditoria_ia WHERE fecha=:f AND hora=:h"), {"f": hoy, "h": h})
        if not check.scalar():
            res = await generar_prediccion(db)
            top = res['top3'][0]
            # Usamos 'porcentaje' para coincidir con el SQL nuevo
            await db.execute(text("""
                INSERT INTO auditoria_ia (fecha, hora, animal_predicho, porcentaje, resultado_real)
                VALUES (:f, :h, :a, :p, 'PENDIENTE')
                ON CONFLICT (fecha, hora) DO NOTHING
            """), {"f": hoy, "h": h, "a": top['animal'], "p": float(str(top['porcentaje']).replace('%',''))})
    await db.commit()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Sistema Vivo: Carga predicciones automáticamente
        await asegurar_apuestas_del_dia(db)
        
        # Predicción próxima
        res_ia = await generar_prediccion(db)
        
        # Datos para los 12 cuadros de auditoría
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto, a.animal_predicho
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, 
            CASE WHEN h.hora LIKE '%PM' AND h.hora NOT LIKE '12%' THEN 1 ELSE 0 END DESC, 
            h.hora DESC LIMIT 12
        """)
        res_db = await db.execute(query)
        
        tz = pytz.timezone('America/Caracas')
        hoy = datetime.now(tz).date()
        ultimos_db = []
        for r in res_db.fetchall():
            ultimos_db.append({
                "es_hoy": r[0] == hoy,
                "hora": r[1],
                "animal": r[2].upper(),
                "img": f"{re.sub(r'[^a-z]', '', r[2].lower())}.png",
                "acierto": r[3]
            })

        # Eficacia real de la semana
        res_efec = await db.execute(text("""
            SELECT COUNT(*), SUM(CASE WHEN acierto = true THEN 1 ELSE 0 END)
            FROM auditoria_ia WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
            AND resultado_real != 'PENDIENTE'
        """))
        st = res_efec.fetchone()
        efectividad = round((st[1]/st[0]*100) if st and st[0]>0 else 0.0, 1)

        return templates.TemplateResponse("dashboard.html", {
            "request": request, "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db, "total_data": 28917, "efectividad": efectividad
        })
    except Exception as e:
        return HTMLResponse(content=f"Error Crítico: {str(e)}", status_code=500)

@app.on_event("startup")
async def startup():
    asyncio.create_task(ciclo_infinito())
