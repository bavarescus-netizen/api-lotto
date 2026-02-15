import random

# Diccionario mapeado a tus nombres de archivos
MAPA_ANIMALES = {
    "0": "delfin", "00": "ballena", "1": "carnero", "2": "toro", "3": "ciempies",
    "4": "alacran", "5": "leon", "6": "rana", "7": "perico", "8": "raton",
    "9": "aguila", "10": "tigre", "11": "gato", "12": "caballo", "13": "mono",
    "14": "paloma", "15": "zorro", "16": "oso", "17": "pavo", "18": "burro",
    "19": "chivo", "20": "cochino", "21": "gallo", "22": "camello", "23": "cebra",
    "24": "iguana", "25": "gallina", "26": "vaca", "27": "perro", "28": "zamuro",
    "29": "elefante", "30": "caiman", "31": "lapa", "32": "ardilla", "33": "pescado",
    "34": "venado", "35": "jirafa", "36": "culebra"
}

async def generar_prediccion():
    # Seleccionamos 3 al azar
    seleccion = random.sample(list(MAPA_ANIMALES.items()), 3)
    top3 = []
    for num, nombre in seleccion:
        top3.append({
            "numero": num,
            "animal": nombre.upper(), # Para el texto (ej: AGUILA)
            "imagen": f"{nombre}.png", # Nombre exacto del archivo
            "porcentaje": f"{random.randint(75, 98)}%"
        })
    return {"decision": "ALTA PROBABILIDAD", "top3": top3}
