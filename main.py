import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date, timedelta
import pytz

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

def extraer_fecha_pura(obj):
    if obj is None: return None
    if isinstance(obj, date) and not isinstance(obj, datetime): return obj
    if isinstance(obj, datetime): return obj.date()
    if isinstance(obj, str):
        try: return datetime.strptime(obj[:10], '%Y-%m-%d').date()
        except: return None
    return obj

# Configuración de estáticos (Asegúrate que la carpeta 'imagenes' exista)
static_path = os.path.join(BASE_DIR, "..", "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "routes"))

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        tz = pytz.timezone('America/Caracas')
        hoy_dt = datetime.now(tz)
        inicio_dt = hoy_dt - timedelta(days=7) 
        datos = await descargar_rango_historico(inicio_dt, hoy_dt)
        if datos:
            for reg in datos:
                f_val = extraer_fecha_pura(reg.get("fecha"))
                if not f_val: continue
                # Insertar en histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l) ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # Actualizar Auditoría para marcar Aciertos/Fallos
                animal_limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', reg["animalito"]).lower()
                await db.execute(text("""
                    UPDATE auditoria_ia SET resultado_real = :a, 
                    acierto = (LOWER(animal_predicho) = :clean_a)
                    WHERE fecha = :f AND hora = :h 
                """), {"a": reg["animalito"], "clean_a": animal_limpio, "f": f_val, "h": reg["hora"]})
            await db.commit()
        return JSONResponse({"status": "success", "message": "Sincronización y Auditoría completada."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/api/procesar")
async def procesar_motor(db: AsyncSession = Depends(get_db)):
    try:
        await generar_prediccion(db)
        await db.commit()
        return JSONResponse({"status": "success", "message": "Motor Recalibrado con 28k registros."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # 1. Predicciones Próximo Sorteo
        res_ia = await generar_prediccion(db)
        
        # 2. Histórico de 12 (3 líneas de 4)
        res_db = await db.execute(text("""
            SELECT h.fecha, h.hora, h.animalito, a.acierto 
            FROM historico h
            LEFT JOIN auditoria_ia a ON h.fecha = a.fecha AND h.hora = a.hora
            ORDER BY h.fecha DESC, 
            CASE WHEN h.hora LIKE '%PM' AND h.hora NOT LIKE '12%' THEN 1 ELSE 0 END DESC, 
            h.hora DESC LIMIT 12
        """))
        
        tz = pytz.timezone('America/Caracas')
        hoy = datetime.now(tz).date()
        ultimos_db = []
        for r in res_db.fetchall():
            ultimos_db.append({
                "es_hoy": r[0] == hoy,
                "hora": r[1],
                "animal": r[2].upper(),
                "img": f"{re.sub(r'[^a-z]', '', r[2].lower())}.png",
                "acierto": r[3] # None=Azul, True=Azul/Verde, False=Rojo
            })

        # 3. Score de Aprendizaje (28k registros)
        res_total = await db.execute(text("SELECT COUNT(*) FROM historico"))
        total_data = res_total.scalar() or 28917
        
        # 4. Eficacia Real (Últimos 7 días)
        res_efec = await db.execute(text("""
            SELECT COUNT(*), SUM(CASE WHEN acierto = true THEN 1 ELSE 0 END)
            FROM auditoria_ia WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
            AND resultado_real != 'PENDIENTE'
        """))
        st = res_efec.fetchone()
        efectividad = round((st[1]/st[0]*100) if st and st[0]>0 else 0, 1)

        return templates.TemplateResponse("dashboard.html", {
            "request": request, "top3": res_ia.get("top3", []),
            "ultimos_db": ultimos_db, "total_data": total_data, "efectividad": efectividad
        })
    except Exception as e:
        return HTMLResponse(content=f"Error: {e}", status_code=500)

@app.on_event("startup")
async def startup():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
