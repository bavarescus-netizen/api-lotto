import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date  # Importamos ambos explícitamente

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# --- FUNCIÓN DE SEGURIDAD ABSOLUTA ---
def extraer_fecha_pura(obj):
    if obj is None: return None
    # Si ya es date y NO es datetime, devolver tal cual
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return obj
    # Si es datetime, extraer date
    if isinstance(obj, datetime):
        return obj.date()
    # Si es string, convertir
    if isinstance(obj, str):
        try:
            return datetime.strptime(obj[:10], '%Y-%m-%d').date()
        except:
            return None
    return obj

# Estáticos y Plantillas
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        # Usamos datetime para el scraper para evitar errores de timedelta
        hoy_dt = datetime.now()
        inicio_dt = datetime(2026, 2, 7)
        
        datos = await descargar_rango_historico(inicio_dt, hoy_dt)
        
        agregados = 0
        if datos:
            for reg in datos:
                # USAMOS LA FUNCIÓN DE SEGURIDAD
                f_val = extraer_fecha_pura(reg.get("fecha"))
                
                if not f_val: continue

                # A. Histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # B. Auditoría
                await db.execute(text("""
                    UPDATE auditoria_ia 
                    SET resultado_real = :a,
                        acierto = (LOWER(animal_predicho) = LOWER(REGEXP_REPLACE(:a, '[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', 'g')))
                    WHERE fecha = :f AND hora = :h 
                """), {"a": reg["animalito"], "f": f_val, "h": reg["hora"]})

            await db.commit()
            agregados = len(datos)
        
        return JSONResponse({"status": "success", "message": f"Sincronizado: {agregados} registros."})
    except Exception as e:
        await db.rollback()
        print(f"❌ ERROR REAL: {str(e)}") # Esto saldrá en el log de Render
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/api/procesar")
async def procesar_motor(db: AsyncSession = Depends(get_db)):
    try:
        await generar_prediccion(db)
        await db.commit()
        return JSONResponse({"status": "success", "message": "Motor V4.5 PRO recalibrado."})
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia = await generar_prediccion(db)
        res_db = await db.execute(text("SELECT hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 12"))
        
        ultimos_12 = []
        for r in res_db.fetchall():
            img = re.sub(r'[^a-záéíóúñ]', '', r[1].lower()).strip() + ".png"
            ultimos_12.append({"hora": r[0], "animal": r[1].upper(), "img": img})
        
        bitacora = await obtener_bitacora_avance(db)
        await db.commit()

        return templates.TemplateResponse("dashboard.html", {
            "request": request, "top3": res_ia.get("top3", []),
            "bitacora": bitacora, "ultimos_db": ultimos_12, "analisis": "LottoAI Online"
        })
    except Exception as e:
        await db.rollback()
        return HTMLResponse(content=f"Error: {e}", status_code=500)

@app.on_event("startup")
async def startup():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
