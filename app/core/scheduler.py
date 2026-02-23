import asyncio
from datetime import datetime
import logging

# Configuración de logs para que los veas en el panel de Render
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variable para evitar procesar el mismo sorteo dos veces
ULTIMO_SORTEO_PROCESADO = None

# Importa aquí tus funciones necesarias (ajusta las rutas si es necesario)
# from app.services.scraper import obtener_ultimo_resultado
# from app.services.guardar_sorteo import guardar_sorteo
# from app.services.evaluar_prediccion import evaluar

async def ciclo_infinito():
    global ULTIMO_SORTEO_PROCESADO
    logger.info("🚀 Sistema Vivo: Monitoreo Inteligente (9 AM - 7:30 PM)")

    while True:
        try:
            ahora = datetime.now()
            # Solo actuar entre las 9:00 y las 19:30
            if 9 <= ahora.hour <= 19:
                minuto = ahora.minute
                
                # REVISIÓN SOLO EN LA VENTANA DEL SORTEO (Minutos 05 al 25)
                if 5 <= minuto <= 25:
                    # NOTA: Asegúrate de que estas funciones estén importadas o definidas
                    # data = await obtener_ultimo_resultado()
                    data = None # Placeholder para evitar error si no están las funciones
                    
                    if data and data != ULTIMO_SORTEO_PROCESADO:
                        # guardado = await guardar_sorteo(data)
                        guardado = True 
                        
                        if guardado:
                            # await evaluar(data)
                            ULTIMO_SORTEO_PROCESADO = data
                            logger.info(f"✅ Sorteo registrado: {data.get('hora')}. Durmiendo hasta la próxima hora.")
                            # Dormir hasta el minuto 05 de la siguiente hora
                            espera = (60 - minuto + 5) * 60
                        else:
                            espera = 300 # Reintento en 5 min si falló DB
                    else:
                        espera = 300 # No ha salido el dato aún, reintento corto
                else:
                    # Si estamos fuera de la ventana 05-25, calculamos espera al próximo sorteo
                    if minuto < 5:
                        espera = (5 - minuto) * 60
                    else:
                        espera = (60 - minuto + 5) * 60
            else:
                logger.info("🌙 Fuera de horario. Durmiendo 30 min...")
                espera = 1800
                
            await asyncio.sleep(espera)
                
        except Exception as e:
            logger.error(f"⚠️ Error en ciclo: {e}")
            await asyncio.sleep(120)
