async def ciclo_infinito():
    global ULTIMO_SORTEO_PROCESADO
    print("🚀 Sistema Vivo: Monitoreo Inteligente (9 AM - 7:30 PM)")

    while True:
        try:
            ahora = datetime.now()
            # Solo actuar entre las 9:00 y las 19:30
            if 9 <= ahora.hour <= 19:
                minuto = ahora.minute
                
                # REVISIÓN SOLO EN LA VENTANA DEL SORTEO (Minutos 05 al 25)
                if 5 <= minuto <= 25:
                    data = await obtener_ultimo_resultado()
                    if data and data != ULTIMO_SORTEO_PROCESADO:
                        guardado = await guardar_sorteo(data)
                        if guardado:
                            await evaluar(data)
                            ULTIMO_SORTEO_PROCESADO = data
                            print(f"✅ Sorteo registrado: {data['hora']}. Durmiendo hasta la próxima hora.")
                            # Dormir hasta el minuto 05 de la siguiente hora
                            espera = (60 - minuto + 5) * 60
                        else:
                            espera = 300 # Reintento en 5 min si falló Neon
                    else:
                        espera = 300 # No ha salido el dato aún, reintento corto
                else:
                    # Si estamos fuera de la ventana 05-25, calculamos espera al próximo sorteo
                    if minuto < 5:
                        espera = (5 - minuto) * 60
                    else:
                        espera = (60 - minuto + 5) * 60
            else:
                print("🌙 Fuera de horario. Durmiendo 30 min...")
                espera = 1800
                
            await asyncio.sleep(espera)
                
        except Exception as e:
            print(f"⚠️ Error en ciclo: {e}")
            await asyncio.sleep(120)
