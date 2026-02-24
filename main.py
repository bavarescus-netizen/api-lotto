import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date

# 1. Configuración de Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos Estáticos
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# --- RUTA DE SINCRONIZACIÓN (SOLUCIÓN DEFINITIVA AL ERROR 500) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        hoy = date.today()
        # Traemos datos desde el 7 de febrero hasta hoy
        datos = await descargar_rango_historico(date(2026, 2, 7), hoy)
        
        agregados = 0
        if datos:
            for reg in datos:
                f_raw = reg["fecha"]
                
                # --- Lógica de fecha blindada ---
                if isinstance(f_raw, str):
                    f_val = datetime.strptime(f_raw, '%Y-%m-%d').date()
                elif isinstance(f_raw, datetime):
                    f_val = f_raw.date()
                elif isinstance(f_raw, date):
                    f_val = f_raw  # Si ya es date, lo usamos tal cual
                else:
                    continue # Tipo desconocido, saltar
                # ---------------------------------

                if f_val > hoy: continue

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
        
        return JSONResponse({"status": "success", "message": f"Sincronización OK: {agregados} registros."})
    except Exception as e:
        await db.rollback()
        print(f"❌ Error Crítico Sincro: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- RUTA DE PROCESAMIENTO (RECALIBRAR) ---
@app.get("/api/procesar")
async def procesar_motor(db: AsyncSession = Depends(get_db)):
    try:
        res = await generar_prediccion(db)
        await db.commit()
        return JSONResponse({"status": "success", "message": "Motor V4.5 PRO recalibrado exitosamente."})
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- DASHBOARD PRINCIPAL ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Predicción actual
        res_ia = await generar_prediccion(db)
        
        # Últimos resultados
        res_db = await db.execute(text("SELECT hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 12"))
        ultimos_12 = []
        for r in res_db.fetchall():
            img = re.sub(r'[^a-záéíóúñ]', '', r[1].lower()).strip() + ".png"
            ultimos_12.append({"hora": r[0], "animal": r[1].upper(), "img": img})
        
        bitacora = await obtener_bitacora_avance(db)
        await db.commit()

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "bitacora": bitacora,
            "ultimos_db": ultimos_12,
            "analisis": res_ia.get("analisis", "Sistema en línea")
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
