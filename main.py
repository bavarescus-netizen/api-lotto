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
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# --- FUNCIÓN DE SEGURIDAD PARA FECHAS ---
def extraer_fecha_pura(obj):
    if obj is None: return None
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return obj
    if isinstance(obj, datetime):
        return obj.date()
    if isinstance(obj, str):
        try:
            return datetime.strptime(obj[:10], '%Y-%m-%d').date()
        except:
            return None
    return obj

# Configuración de Archivos Estáticos y Plantillas
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
    """Sincroniza resultados de la web a la DB (Solo ayer y hoy)"""
    try:
        # CAMBIO: Ya no usamos el 7 de febrero. Solo descargamos las últimas 48 horas.
        tz = pytz.timezone('America/Caracas')
        hoy_dt = datetime.now(tz)
        inicio_dt = hoy_dt - timedelta(days=1) 
        
        print(f"🚀 Sincronizando bloque actual desde: {inicio_dt.date()}")
        datos = await descargar_rango_historico(inicio_dt, hoy_dt)
        
        agregados = 0
        if datos:
            for reg in datos:
                f_val = extraer_fecha_pura(reg.get("fecha"))
                if not f_val: continue

                # 1. Insertar en Histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # 2. Actualizar Auditoría (Para marcar aciertos en la bitácora)
                # Limpiamos el nombre del animalito para comparar sin errores de acentos o espacios
                animal_limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', reg["animalito"]).lower()
                
                await db.execute(text("""
                    UPDATE auditoria_ia 
                    SET resultado_real = :a,
                        acierto = (LOWER(animal_predicho) = :clean_a)
                    WHERE fecha = :f AND hora = :h 
                """), {
                    "a": reg["animalito"], 
                    "clean_a": animal_limpio,
                    "f": f_val, 
                    "h": reg["hora"]
                })

            await db.commit()
            agregados = len(datos)
        
        return JSONResponse({"status": "success", "message": f"Sincronizado: {agregados} registros (Bloque actual)."})
    except Exception as e:
        await db.rollback()
        print(f"❌ ERROR EN SINCRONIZACIÓN: {str(e)}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/api/procesar")
async def procesar_motor(db: AsyncSession = Depends(get_db)):
    """Recalibra las probabilidades de la IA"""
    try:
        await generar_prediccion(db)
        await db.commit()
        return JSONResponse({"status": "success", "message": "Motor V4.5 PRO recalibrado exitosamente."})
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    """Página principal del Dashboard"""
    try:
        # Obtenemos predicción actual
        res_ia = await generar_prediccion(db)
        
        # Obtenemos últimos 12 resultados para la tabla de la derecha
        res_db = await db.execute(text("""
            SELECT hora, animalito 
            FROM historico 
            ORDER BY fecha DESC, 
            CASE WHEN hora LIKE '%PM' AND hora NOT LIKE '12%' THEN 1 ELSE 0 END DESC,
            hora DESC 
            LIMIT 12
        """))
        
        ultimos_12 = []
        for r in res_db.fetchall():
            # Generar nombre de imagen limpio
            img_name = re.sub(r'[^a-z]', '', r[1].lower()).strip()
            ultimos_12.append({
                "hora": r[0], 
                "animal": r[1].upper(), 
                "img": f"{img_name}.png"
            })
        
        # Obtenemos la bitácora de aciertos de hoy
        bitacora = await obtener_bitacora_avance(db)
        await db.commit()

        return
