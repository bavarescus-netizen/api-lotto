import asyncio
from datetime import datetime
import logging
from sqlalchemy import text
from db import get_db

# IMPORTACIONES REALES DE TU PROYECTO
from app.routes.cargarhist import procesar_ultimo_sorteo # Tu función de scraping
from app.routes.entrenar import procesar_entrenamiento # Tu motor de IA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ULTIMO_SORTEO_PROCESADO = None

async def ciclo_infinito():
    global ULTIMO_SORTEO_PROCESADO
    logger.info("🚀 Sistema Vivo: Monitoreo Inteligente LottoAI")

    while True:
        try:
            ahora = datetime.now()
            # 1. HORARIO OPERATIVO (9 AM a 7:30 PM)
            if 9 <= ahora.hour <= 19:
                minuto = ahora.minute
                
                # 2. VENTANA DE ACECHO (Minutos 03 al 25 para capturar el PIN)
                if 3 <= minuto <= 25:
                    async for db in get_db():
                        # Intentamos capturar el resultado real
                        # exito debe devolver True si guardó un nuevo animalito
                        exito = await procesar_ultimo_sorteo(db)
                        
                        if exito:
                            logger.info(f"✅ ¡NUEVO RESULTADO DETECTADO! Hora: {ahora.hour}:00")
                            
                            # 3. LANZAR ENTRENAMIENTO AUTOMÁTICO
                            # Esto recalibra el motor con el nuevo dato recién guardado
                            await procesar_entrenamiento(db)
                            logger.info("🧠 Motor IA Recalibrado exitosamente.")
                            
                            # 4. EVITAR DUPLICADOS: Dormir hasta la próxima hora + 3 min
                            ULTIMO_SORTEO_PROCESADO = f"{ahora.hour}-{ahora.day}"
                            espera = (60 - minuto + 3) * 60
                        else:
                            # No ha salido el resultado, reintento en 3 min (el PIN que pediste)
                            logger.info(f"⏳ [{ahora.strftime('%H:%M')}] Resultado no disponible. Reintentando en 180s...")
                            espera = 180 
                else:
                    # Fuera de ventana de sorteo, calcular espera al próximo minuto 03
                    if minuto < 3:
                        espera = (3 - minuto) * 60
                    else:
                        espera = (60 - minuto + 3) * 60
            else:
                logger.info("🌙 Fuera de horario de sorteos. Durmiendo 30 min...")
                espera = 1800
                
            await asyncio.sleep(espera)
                
        except Exception as e:
            logger.error(f"⚠️ Error en ciclo: {e}")
            await asyncio.sleep(120)
