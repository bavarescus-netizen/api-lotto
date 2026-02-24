import sys
import os
import asyncio
import re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date

# 1. Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Estáticos y Plantillas
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# Routers
from app.routes import prediccion, entrenar, stats, historico
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# --- RUTA DE SINCRONIZACIÓN (REHECHA PARA NEON) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        # Usamos objetos date puros
        inicio_busqueda = date(2026, 2, 7)
        hoy = date.today()
        
        datos_nuevos = await descargar_rango_historico(inicio_busqueda, hoy)
        
        agregados = 0
        if datos_nuevos:
            for reg in datos_nuevos:
                # Normalización de fecha
                f_val = reg["fecha"]
                if isinstance(f_val, str):
                    f_val = datetime.strptime(f_val, '%Y-%m-%d').date()
                
                if f_val > hoy: continue

                # A. Insertar en Histórico (Sin ID)
                result = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                if result.rowcount > 0:
                    agregados += 1
                    # B. Marcado de ACIERTOS (Usa regex para comparar solo letras)
                    await db.execute(text("""
                        UPDATE auditoria_ia 
                        SET resultado_real = :a,
                            acierto = (LOWER(animal_predicho) = LOWER(SUBSTRING(:a FROM '[a-zA-ZáéíóúñÁÉÍÓÚÑ]+')))
                        WHERE fecha = :f AND hora = :h AND (resultado_real = 'PENDIENTE' OR resultado_real IS NULL)
                    """), {"a": reg["animalito"], "f": f_val, "h": reg["hora"]})

            await db.commit() 
        
        return JSONResponse({"status": "success", "message": f"Sincronizado: {agregados} nuevos."})
    except Exception as e:
        await db.rollback() 
        return JSONResponse({"status": "error", "message": f"Fallo en Sincro: {str(e)}"}, status_code=500)

# --- RUTA HOME (CORREGIDA SIN COLUMNA ID) ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    res_ia = {"top3": [], "analisis": "Motor V4.5 PRO ONLINE"}
    bitacora = []
    ultimos_12 = []

    # 1. IA
    try:
        res_ia = await generar_prediccion(db)
        await db.commit() 
    except Exception as e:
        await db.rollback()

    # 2. Bitácora
    try:
        bitacora = await obtener_bitacora_avance(db)
        await db.commit()
    except Exception as e:
        await db.rollback()

    # 3. Histórico (Ordenado solo por fecha y hora)
    try:
        res_db = await db.execute(text("""
            SELECT hora, animalito FROM historico 
            ORDER BY fecha DESC, hora DESC LIMIT 12
        """))
        
        for r in res_db.fetchall():
            nombre_sucio = r[1].lower()
            # Limpia "05 LEON" a "leon"
            nombre_limpio = re.sub(r'[^a-záéíóúñ]', '', nombre_sucio).strip()
            
            ultimos_12.append({
                "hora": r[0],
                "animal": r[1].upper(),
                "img": f"{nombre_limpio}.png"
            })
        await db.commit()
    except Exception as e:
        print(f"Error Historial: {e}")
        await db.rollback()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "top3": res_ia.get("top3", []),
        "bitacora": bitacora,
        "ultimos_db": ultimos_12,
        "analisis": res_ia.get("analisis", "Auditoría en tiempo real")
    })

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
