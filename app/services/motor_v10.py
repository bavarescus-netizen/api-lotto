import math
from collections import Counter, defaultdict

# --------------------------------
# IA (Random Forest)
# --------------------------------
try:
    from app.services.modelo_rf import ModeloRF
    modelo_rf = ModeloRF()
except:
    modelo_rf = None

# --------------------------------
# APRENDIZAJE AUTOMÁTICO
# --------------------------------
try:
    from app.services.aprendizaje import obtener_pesos, actualizar_pesos
except:
    def obtener_pesos():
        return {
            "deuda": 0.25,
            "frecuencia": 0.25,
            "patron": 0.15,
            "anti": 0.10,
            "markov": 0.10,
            "rf": 0.15
        }

    def actualizar_pesos(bitacora):
        return obtener_pesos()

# --------------------------------
# CONFIG
# --------------------------------

ANIMALES = list(range(1, 39))

# --------------------------------
# BITÁCORA
# --------------------------------

bitacora_global = []

def obtener_bitacora():
    return bitacora_global

def guardar_en_bitacora(prediccion, resultado_real=None):
    registro = {
        "prediccion": prediccion,
        "resultado_real": resultado_real
    }
    bitacora_global.append(registro)

    if len(bitacora_global) > 1000:
        bitacora_global.pop(0)

# --------------------------------
# SEÑALES
# --------------------------------

def calcular_deuda(historial):
    gap = {}
    ciclos = {a: [] for a in ANIMALES}
    ultima = {}

    for i, animal in enumerate(historial):
        if animal in ultima:
            ciclos[animal].append(i - ultima[animal])
        ultima[animal] = i

    for animal in ANIMALES:
        dias = 0
        for i in range(len(historial)-1, -1, -1):
            if historial[i] == animal:
                break
            dias += 1

        ciclo_prom = sum(ciclos[animal])/len(ciclos[animal]) if ciclos[animal] else 10
        ratio = dias / ciclo_prom if ciclo_prom > 0 else 0

        gap[animal] = math.log1p(ratio)

    return gap


def calcular_frecuencia_reciente(historial):
    total = len(historial)
    freq = Counter(historial)

    scores = {}

    for a in ANIMALES:
        prob_real = freq[a] / total if total > 0 else 0
        prob_azar = 1 / 38
        scores[a] = math.log1p(prob_real / prob_azar)

    return scores


def calcular_anti_racha(historial):
    dias = {}

    for a in ANIMALES:
        dias[a] = 0

        for i in range(len(historial)-1, -1, -1):
            if historial[i] == a:
                break
            dias[a] += 1

        dias[a] = math.exp(-dias[a] * 0.3)

    return dias


def calcular_markov_hora(historial):
    transiciones = defaultdict(Counter)

    for i in range(1, len(historial)):
        transiciones[historial[i-1]][historial[i]] += 1

    if len(historial) < 2:
        return {}

    ultimo = historial[-1]

    if ultimo not in transiciones:
        return {}

    total = sum(transiciones[ultimo].values())

    return {a: c / total for a, c in transiciones[ultimo].items()}


def calcular_patron_dia(historial):
    freq = Counter(historial)
    total = len(historial)

    return {a: freq[a] / total if total > 0 else 0 for a in ANIMALES}

# --------------------------------
# COMBINADOR (CON APRENDIZAJE)
# --------------------------------

def combinar_señales(
    deuda,
    frecuencia,
    patron,
    anti,
    markov,
    historial_predicciones=None,
    rf_probs=None
):

    pesos = obtener_pesos()
    scores = {}

    for a in ANIMALES:

        s_deuda = deuda.get(a, 0)
        s_freq = frecuencia.get(a, 0)
        s_patron = patron.get(a, 0)
        s_anti = anti.get(a, 0)
        s_markov = markov.get(a, 0)
        s_rf = rf_probs.get(a, 0) if rf_probs else 0

        score = (
            pesos["deuda"] * s_deuda +
            pesos["frecuencia"] * s_freq +
            pesos["patron"] * s_patron +
            pesos["anti"] * s_anti +
            pesos["markov"] * s_markov +
            pesos["rf"] * s_rf
        )

        # Penalizar repetición
        if historial_predicciones:
            veces = historial_predicciones.count(a)
            score *= math.exp(-0.15 * veces)

        scores[a] = score

    return scores

# --------------------------------
# PREDICCIÓN
# --------------------------------

def generar_prediccion(historial, historial_predicciones=None):

    rf_probs = {}

    if modelo_rf:
        try:
            modelo_rf.entrenar(historial)
            rf_probs = modelo_rf.predecir(historial)
        except:
            rf_probs = {}

    deuda = calcular_deuda(historial)
    frecuencia = calcular_frecuencia_reciente(historial)
    patron = calcular_patron_dia(historial)
    anti = calcular_anti_racha(historial)
    markov = calcular_markov_hora(historial)

    scores = combinar_señales(
        deuda,
        frecuencia,
        patron,
        anti,
        markov,
        historial_predicciones,
        rf_probs
    )

    ordenados = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top5 = [a for a, _ in ordenados[:5]]

    # 🔥 guardar en bitácora
    guardar_en_bitacora(top5)

    # 🔥 aprendizaje automático
    try:
        actualizar_pesos(bitacora_global)
    except:
        pass

    return {
        "top1": top5[0],
        "top3": top5[:3],
        "top5": top5,
        "scores": scores
    }

# --------------------------------
# ESTADÍSTICAS
# --------------------------------

def obtener_estadisticas(historial):
    total = len(historial)
    freq = Counter(historial)

    top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "total_sorteos": total,
        "top_animales": top
    }
