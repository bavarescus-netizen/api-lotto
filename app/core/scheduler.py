import asyncio
from datetime import datetime
import logging
from sqlalchemy import text
from db import get_db

# Importaciones de tus rutas
from app.routes.cargarhist import procesar_ultimo_sorteo
from app.routes.entrenar import procesar_entrenamiento

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ULTIMO_SORTEO_PROCESADO = None

async def ciclo_infinito():
    global ULTIMO_SORTEO_PROCESADO
    logger.info("🚀 Sistema Vivo: Monitoreo con Auditoría Automática")

    while True:
        try:
            ahora = datetime.now()
            if 9 <= ahora.hour <= 19:
                minuto = ahora.minute
                
                # Ventana de acecho (cada 3 min entre el min 3 y el 25)
                if 3 <= minuto <= 25:
                    async for db in get_db():
                        exito = await procesar_ultimo_sorteo(db)
                        
                        if exito:
                            logger.info(f"✅ Nuevo resultado capturado a las {ahora.hour}:00")
                            
                            # --- BLOQUE DE AUDITORÍA AUTOMÁTICA ---
                            # Compara el resultado recién guardado con la predicción que se hizo para esa hora
                            sql_auditar = text("""
                                INSERT INTO auditoria_ia (fecha, hora, animalito_real, acierto)
                                SELECT h.fecha, h.hora, h.animalito, 
                                       CASE WHEN p.animalito = h.animalito THEN TRUE ELSE FALSE END
                                FROM historico h
                                JOIN probabilidades_hora p ON h.hora = p.hora 
                                WHERE h.fecha = CURRENT_DATE AND h.hora = :hora_sorteo
                                LIMIT 1
                                ON CONFLICT (fecha, hora) DO UPDATE SET acierto = EXCLUDED.acierto;
                            """)
                            await db.execute(sql_auditar, {"hora_sorteo": f"{ahora.hour}:00:00"})
                            
                            # 2. Re-Entrenar IA con el nuevo dato
                            await procesar_entrenamiento(db)
                            
                            # 3. Actualizar la tabla de métricas global (Efectividad %)
                            await db.execute(text("""
                                UPDATE metrics SET 
                                    total = (SELECT COUNT(*) FROM auditoria_ia),
                                    aciertos = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto = True),
                                    precision = (SELECT (COUNT(CASE WHEN acierto = True THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0)::FLOAT) * 100 FROM auditoria_ia)
                                WHERE id = 1
                            """))
                            
                            await db.commit()
                            logger.info("📊 Métricas y Auditoría actualizadas.")
                            
                            ULTIMO_SORTEO_PROCESADO = f"{ahora.hour}-{ahora.day}"
                            espera = (60 - minuto + 3) * 60
                        else:
                            logger.info(f"⏳ [{ahora.strftime('%H:%M')}] Esperando resultado... Reintento en 180s")
                            espera = 180 
                else:
                    espera = (60 - minuto + 3) * 60 if minuto > 25 else (3 - minuto) * 60
            else:
                logger.info("🌙 Fuera de horario. Modo ahorro energía.")
                espera = 1800
                
            await asyncio.sleep(espera)
                
        except Exception as e:
            logger.error(f"⚠️ Error en ciclo: {e}")
            await asyncio.sleep(120)
