import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date, timedelta
import pytz

# --- CONFIGURACIÓN DINÁMICA DE RUTAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def encontrar_carpeta(nombre):
    """Busca la carpeta en el proyecto para evitar errores de ruta en Render"""
    for raiz, dirs, archivos in os.walk(BASE_DIR):
        if nombre in dirs:
            return os.path.join(raiz, nombre)
    return os.path.join(BASE_DIR, nombre)

app = FastAPI(title="Lotto AI V4.5 PRO")

# 1. Montar Imágenes
path_imgs = encontrar_carpeta("imagenes")
if os.path.exists(path_imgs):
    app.mount("/imagenes", StaticFiles(directory=path_imgs), name="imagenes")

# 2. Configurar Templates (Carpeta ROUTES)
path_routes = encontrar_carpeta("routes")
templates = Jinja2Templates(directory=path_routes)

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

def extraer_fecha_pura(obj):
    if obj is None: return None
    if isinstance(obj, datetime): return obj.date()
    if isinstance(obj, date): return obj
    return None

# --- ENDPOINTS API ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        tz = pytz.timezone('America/Caracas')
        hoy_dt = datetime.now(tz)
        # Sincronizamos 7 días para que el Score no sea 0%
        datos = await descargar_rango_historico(hoy_dt - timedelta(days=7), hoy_dt)
        if datos:
            for reg in datos:
                f_val = extraer_fecha_pura(reg.get("fecha"))
                if not f_val: continue
                # Actualizar Histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l) ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # Marcar Aciertos/Fallos en Auditoría
                animal_limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', reg["animalito"]).lower()
                await db.execute(text("""
                    UPDATE auditoria_ia SET resultado_real = :a, 
                    acierto = (LOWER(animal_predicho) = :clean_a)
                    WHERE fecha = :f AND hora = :h 
                """), {"a": reg["animalito"], "clean_a": animal_limpio, "f": f_val, "h": reg["hora"]})
            await db.commit()
        return JSONResponse({"status": "success", "message": "Semana sincronizada y auditada."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/api/procesar")
async def procesar_motor(db: AsyncSession = Depends(get_db)):
    try:
        await generar_prediccion(db)
        await db.commit()
        return JSONResponse({"status": "success", "message": "Motor V4.5 PRO Recalibrado."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- DASHBOARD ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia = await generar_prediccion(db)
        
        # Consulta 12 resultados con cruce de auditoría
        query = text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto 
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

        # Eficacia 7 Días
        res_efec = await db.execute(text("""
            SELECT COUNT(*), SUM(CASE WHEN acierto = true THEN 1 ELSE 0 END)
            FROM auditoria_ia WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
            AND resultado_real != 'PENDIENTE'
        """))
        st = res_efec.fetchone()
        efectividad = round((st[1]/st[0]*100) if st and st[0]>0 else 0, 1)

        return templates.TemplateResponse("dashboard.html", {
            "request": request, 
            "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db, 
            "total_data": 28917, 
            "efectividad": efectividad
        })
    except Exception as e:
        # Esto atrapará cualquier error y te dirá qué falta en los logs
        return HTMLResponse(content=f"Error Crítico: {str(e)}", status_code=500)

@app.on_event("startup")
async def startup():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
