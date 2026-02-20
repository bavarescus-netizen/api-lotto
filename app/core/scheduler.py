import asyncio
from datetime import datetime, timedelta
from app.services.scraper import obtener_ultimo_resultado
from app.services.guardar_sorteo import guardar_sorteo
from app.services.evaluar_prediccion import evaluar

async def ciclo_infinito():
    print("🚀 Monitor Inteligente Activado (9:05 AM - 7:30 PM)")
    
    # Diccionario para no repetir el mismo sorteo
    ultimo_registro_local = {"hora": None, "fecha": None}

    while True:
        ahora = datetime.now()
        # Solo actuar entre 9:00 y 19:40 (7:40 PM para cubrir el último sorteo)
        if 9 <= ahora.hour <= 19:
            minuto = ahora.minute
            
            # Lógica: Revisar solo en la ventana crítica (entre el minuto 05 y 25 de cada hora)
            if 5 <= minuto <= 25:
                try:
                    print(f"🔍 [{ahora.strftime('%H:%M')}] Buscando resultado en la web...")
                    data = await obtener_ultimo_resultado() 
                    
                    if data:
                        # Evitar duplicados: Comparar hora y fecha del sorteo
                        if data['hora'] != ultimo_registro_local['hora'] or data['fecha'] != ultimo_registro_local['fecha']:
                            
                            guardado = await guardar_sorteo(data)
                            if guardado:
                                await evaluar(data)
                                ultimo_registro_local['hora'] = data['hora']
                                ultimo_registro_local['fecha'] = data['fecha']
                                print(f"✅ Sorteo de las {data['hora']} guardado con éxito.")
                                
                                # Si ya guardamos el de esta hora, dormimos hasta la siguiente hora minuto 05
                                print("😴 Sorteo capturado. Esperando a la siguiente hora...")
                                sleep_time = (60 - ahora.minute + 5) * 60 
                            else:
                                sleep_time = 300 # Error al guardar, reintentar en 5 min
                        else:
                            print("⏳ El sorteo en la web sigue siendo el anterior. Reintentando en 5 min...")
                            sleep_time = 300
                    else:
                        sleep_time = 300 # No hay data, reintentar en 5 min
                except Exception as e:
                    print(f"⚠️ Error: {e}")
                    sleep_time = 300
            else:
                # Si estamos fuera del rango 05-25, calculamos cuánto falta para el siguiente minuto 05
                if minuto < 5:
                    sleep_time = (5 - minuto) * 60
                else:
                    sleep_time = (60 - minuto + 5) * 60
                print(f"💤 Fuera de ventana de sorteo. Durmiendo {sleep_time // 60} minutos...")
        else:
            print("🌙 Fuera de horario operativo. Durmiendo 30 min...")
            sleep_time = 1800
            
        await asyncio.sleep(sleep_time)
