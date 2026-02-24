import sys, os, asyncio, re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date

# 1. Rutas de Sistema
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos Estáticos e Imágenes
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

# 3. Importaciones de Servicios
from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# --- RUTA DE SINCRONIZACIÓN (VERSIÓN ANTIBALAS) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    f_raw = None # Para debug en el catch
    try:
        hoy = date.today()
        # Traemos datos desde el 7 de febrero
        datos = await descargar_rango_historico(date(2026, 2, 7), hoy)
        
        agregados = 0
        if datos:
            for reg in datos:
                f_raw = reg.get("fecha")
                
                # --- LÓGICA DE FECHA DEFINITIVA ---
                # Detectamos el tipo de dato para no llamar a .date() en un objeto date
                if isinstance(f_raw, str):
                    f_val = datetime.strptime(f_raw, '%Y-%m-%d').date()
                elif isinstance(f_raw, datetime):
                    f_val = f_raw.date()
                elif isinstance(f_raw, date):
                    f_val = f_raw # Si ya es date, se usa directo
                else:
                    # Intento desesperado: convertir a string y procesar
                    try:
                        f_val = datetime.strptime(str(f_raw)[:10], '%Y-%m-%d').date()
                    except:
                        continue 

                if f_val > hoy: continue

                # A. Insertar en Histórico (Neon compatible)
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # B. Actualizar Auditoría de la IA
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
        # Este print te dirá exactamente qué tipo de dato causó el fallo en los logs de Render
        print(f"❌ Error Sincro - Tipo: {type(f_raw)} - Valor: {f_raw} - Error: {str(e)}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- RUTA PARA ENTRENAR / PROCESAR ---
@app.get("/api/procesar")
async def procesar_motor(db: AsyncSession = Depends(get_db)):
    try:
        await generar_prediccion(db)
        await db.commit()
        return JSONResponse({"status": "success", "message": "Motor V4.5 PRO recalibrado."})
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- DASHBOARD PRINCIPAL ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # Predicción actual
        res_ia = await generar_prediccion(db)
        
        # Últimos 12 resultados
        res_db = await db.execute(text("""
            SELECT hora, animalito FROM historico 
            ORDER BY fecha DESC, hora DESC LIMIT 12
        """))
        
        ultimos_12 = []
        for r in res_db.fetchall():
            img_name = re.sub(r'[^a-záéíóúñ]', '', r[1].lower()).strip()
            ultimos_12.append({
                "hora": r[0],
                "animal": r[1].upper(),
                "img": f"{img_name}.png"
            })
        
        bitacora = await obtener_bitacora_avance(db)
        await db.commit()

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []),
            "bitacora": bitacora,
            "ultimos_db": ultimos_12,
            "analisis": "LottoAI Core Activo"
        })
    except Exception as e:
        await db.rollback()
        print(f"⚠️ Error Dashboard: {e}")
        return HTMLResponse(content=f"Error en servidor: {e}", status_code=500)

# --- SCHEDULER ---
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
