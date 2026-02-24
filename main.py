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

# 1. Configuración de rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos Estáticos y Plantillas
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones de servicios
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

# --- RUTA DE SINCRONIZACIÓN (CON MARCADO DE ACIERTOS) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        inicio = datetime(2026, 2, 7).date()
        hoy = date.today()
        datos_nuevos = await descargar_rango_historico(inicio, hoy)
        
        agregados = 0
        if datos_nuevos:
            for reg in datos_nuevos:
                f_raw = reg["fecha"]
                fecha_valida = datetime.strptime(f_raw, '%Y-%m-%d').date() if isinstance(f_raw, str) else f_raw
                if fecha_valida > hoy: continue

                # A. Insertar en Histórico
                result = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": fecha_valida, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                if result.rowcount > 0:
                    agregados += 1
                    # B. Marcado de ACIERTOS automático en Auditoría
                    # Comparamos el animalito que acaba de entrar con la predicción que estaba PENDIENTE
                    await db.execute(text("""
                        UPDATE auditoria_ia 
                        SET resultado_real = :a,
                            acierto = (LOWER(animal_predicho) = LOWER(SUBSTRING(:a FROM '[a-zA-ZáéíóúñÁÉÍÓÚÑ]+')))
                        WHERE fecha = :f AND hora = :h AND (resultado_real = 'PENDIENTE' OR resultado_real IS NULL)
                    """), {"a": reg["animalito"], "f": fecha_valida, "h": reg["hora"]})

            await db.commit()
        
        return JSONResponse({"status": "success", "message": f"Sincronización Exitosa. {agregados} nuevos datos y auditoría actualizada."})
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- RUTA HOME (CON LOS ÚLTIMOS 12 DE LA DB) ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        res_ia = await generar_prediccion(db)
        bitacora = await obtener_bitacora_avance(db)

        # CONSULTA: Últimos 12 registros reales de la DB para los cuadritos
        res_db = await db.execute(text("""
            SELECT hora, animalito, fecha FROM historico 
            ORDER BY fecha DESC, hora DESC LIMIT 12
        """))
        ultimos_12 = []
        for r in res_db.fetchall():
            # Limpiamos nombre para la imagen: "Delfin (0)" -> "delfin.png"
            nombre_limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', r[1]).lower()
            ultimos_12.append({
                "hora": r[0],
                "animal": r[1].upper(),
                "img": f"{nombre_limpio}.png"
            })

        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "top3": res_ia.get("top3", []), # Enviamos top3 directamente
            "bitacora": bitacora,
            "ultimos_db": ultimos_12,
            "analisis": res_ia.get("analisis", "Carga completa")
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"Error en Home: {str(e)}"}, status_code=500)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
