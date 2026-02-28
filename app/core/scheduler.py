import asyncio
from datetime import datetime
import logging
from sqlalchemy import text
from db import get_db

# Importaciones
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
            # Horario de operación: 9 AM a 7 PM
            if 9 <= ahora.hour <= 19:
                minuto = ahora.minute
                
                # Ventana de acecho (minutos 3 al 25)
                if 3 <= minuto <= 25:
                    async for db in get_db():
                        # Intentar capturar resultado
                        exito = await procesar_ultimo_sorteo(db)
                        
                        if exito:
                            logger.info(f"✅ Nuevo resultado detectado a las {ahora.hour}:00")
                            
                            try:
                                # 1. AUDITORÍA: ¿Acertamos la predicción previa?
                                # Formateamos la hora para que coincida con la DB (ej: '10:00:00')
                                hora_str = f"{ahora.hour:02d}:00:00"
                                
                                sql_auditar = text("""
                                    INSERT INTO auditoria_ia (fecha, hora, animalito_real, acierto)
                                    SELECT h.fecha, h.hora, h.animalito, 
                                           CASE WHEN p.animalito = h.animalito THEN TRUE ELSE FALSE END
                                    FROM historico h
                                    LEFT JOIN predicciones p ON h.fecha = p.fecha AND h.hora = p.hora
                                    WHERE h.fecha = CURRENT_DATE AND h.hora = :hora_sorteo
                                    ON CONFLICT (fecha, hora) DO UPDATE SET 
                                        animalito_real = EXCLUDED.animalito_real,
                                        acierto = EXCLUDED.acierto;
                                """)
                                await db.execute(sql_auditar, {"hora_sorteo": hora_str})
                                
                                # 2. RE-ENTRENAR: Aprender del nuevo dato
                                await procesar_entrenamiento(db)
                                
                                # 3. ACTUALIZAR MÉTRICAS GLOBALES
                                await db.execute(text("""
                                    UPDATE metrics SET 
                                        total = (SELECT COUNT(*) FROM auditoria_ia),
                                        aciertos = (SELECT COUNT(*) FROM auditoria_ia WHERE acierto = True),
                                        precision = (SELECT (COUNT(CASE WHEN acierto = True THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0)::FLOAT) * 100 FROM auditoria_ia)
                                    WHERE id = 1
                                """))
                                
                                await db.commit()
                                logger.info("📊 IA Auditada y Recalibrada.")
                                
                            except Exception as audit_err:
                                logger.error(f"⚠️ Error en auditoría/entrenamiento: {audit_err}")
                                await db.rollback()
                            
                            ULTIMO_SORTEO_PROCESADO = f"{ahora.day}-{ahora.hour}"
                            # Dormir hasta la próxima hora
                            espera = (60 - minuto + 3) * 60
                        else:
                            logger.info(f"⏳ [{ahora.strftime('%H:%M')}] Esperando resultado... Reintento en 3 min")
                            espera = 180 
                else:
                    # Cálculo de espera para entrar en la ventana del minuto 3
                    if minuto < 3:
                        espera = (3 - minuto) * 60
                    else:
                        espera = (60 - minuto + 3) * 60
            else:
                logger.info("🌙 Fuera de horario. Modo ahorro energía.")
                espera = 1800 # 30 min
                
            await asyncio.sleep(espera)
                
        except Exception as e:
            logger.error(f"⚠️ Error crítico en ciclo: {e}")
            await asyncio.sleep(60)
