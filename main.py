import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# Estáticos e Imágenes
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# --- RUTA DE SINCRONIZACIÓN (BLINDADA) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        hoy = date.today()
        # Scraper
        datos = await descargar_rango_historico(date(2026, 2, 7), hoy)
        
        if datos:
            for reg in datos:
                # Manejo de fecha seguro para evitar el error de atributo 'date'
                f_val = reg["fecha"]
                if isinstance(f_val, str):
                    f_val = datetime.strptime(f_val, '%Y-%m-%d').date()
                
                # A. Actualizar Histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # B. Actualizar Auditoría
                await db.execute(text("""
                    UPDATE auditoria_ia 
                    SET resultado_real = :a,
                        acierto = (LOWER(animal_predicho) = LOWER(REGEXP_REPLACE(:a, '[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', 'g')))
                    WHERE fecha = :f AND hora = :h 
                """), {"a": reg["animalito"], "f": f_val, "h": reg["hora"]})

            await db.commit()
        return JSONResponse({"status": "success", "message": "Sincronización completada"})
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- HOME DASHBOARD ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia = await generar_prediccion(db)
        await db.commit()
        
        res_db = await db.execute(text("SELECT hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 12"))
        ultimos_12 = []
        for r in res_db.fetchall():
            img = re.sub(r'[^a-záéíóúñ]', '', r[1].lower()).strip() + ".png"
            ultimos_12.append({"hora": r[0], "animal": r[1].upper(), "img": img})
        
        bitacora = await obtener_bitacora_avance(db)
        return templates.TemplateResponse("dashboard.html", {
            "request": request, "top3": res_ia.get("top3", []),
            "bitacora": bitacora, "ultimos_db": ultimos_12, "analisis": "Sistema Online"
        })
    except Exception as e:
        await db.rollback()
        return HTMLResponse(content=f"Error en Dashboard: {e}", status_code=500)

@app.on_event("startup")
async def startup():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
