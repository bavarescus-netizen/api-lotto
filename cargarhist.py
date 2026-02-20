import requests
from bs4 import BeautifulSoup

# TU RUTA EXACTA
URL_API = "https://api-lotto-t6p5.onrender.com/api/historico/agregar"

# ... (aquí va tu html_content que ya tienes) ...

def cargar():
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    total_exitos = 0
    
    for table in tables:
        headers = [th.text.strip() for th in table.find('thead').find_all('th')]
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all(['th', 'td'])
            hora = cols[0].text.strip()
            
            for i in range(1, len(cols)):
                animal = cols[i].text.strip().lower()
                fecha = headers[i]
                
                if animal:
                    # CAMBIO CLAVE: Diccionario para Formulario
                    payload = {"animalito": animal, "hora": hora, "fecha": fecha}
                    try:
                        # CAMBIO CLAVE: usamos data= en lugar de json=
                        r = requests.post(URL_API, data=payload, timeout=10)
                        if r.status_code in [200, 201]:
                            print(f"✅ Cargado: {fecha} {hora} -> {animal}")
                            total_exitos += 1
                        else:
                            print(f"⚠️ Error {r.status_code}: {r.text}")
                    except Exception as e:
                        print(f"❌ Error: {e}")

    print(f"\n🚀 ¡LISTO! Se cargaron {total_exitos} resultados.")


