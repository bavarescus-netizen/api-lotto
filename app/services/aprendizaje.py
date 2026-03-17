import math

# Pesos iniciales (igual que motor)
pesos = {
    "deuda": 0.25,
    "frecuencia": 0.25,
    "patron": 0.15,
    "anti": 0.10,
    "markov": 0.10,
    "rf": 0.15
}

# Historial de rendimiento
historial_resultados = []

# --------------------------------
# EVALUAR RESULTADO
# --------------------------------

def evaluar_prediccion(prediccion, resultado_real):
    """
    Retorna:
    3 = acierto en top1
    2 = acierto en top3
    1 = acierto en top5
    0 = fallo
    """

    if resultado_real == prediccion["top1"]:
        return 3

    if resultado_real in prediccion["top3"]:
        return 2

    if resultado_real in prediccion["top5"]:
        return 1

    return 0

# --------------------------------
# ACTUALIZAR MODELO
# --------------------------------

def actualizar_pesos(bitacora):

    if len(bitacora) < 20:
        return pesos

    score_total = 0

    for registro in bitacora:
        if registro["resultado_real"] is None:
            continue

        resultado = evaluar_prediccion(
            {
                "top1": registro["prediccion"][0],
                "top3": registro["prediccion"][:3],
                "top5": registro["prediccion"]
            },
            registro["resultado_real"]
        )

        score_total += resultado

    promedio = score_total / len(bitacora)

    # 🔥 AJUSTE INTELIGENTE
    if promedio < 1:
        # sistema malo → más exploración
        pesos["deuda"] *= 1.05
        pesos["anti"] *= 1.05

    elif promedio < 1.5:
        # medio → balance
        pesos["frecuencia"] *= 1.02
        pesos["patron"] *= 1.02

    else:
        # bueno → explotar IA
        pesos["rf"] *= 1.05
        pesos["markov"] *= 1.03

    # 🔥 NORMALIZAR
    total = sum(pesos.values())
    for k in pesos:
        pesos[k] /= total

    return pesos

# --------------------------------
# OBTENER PESOS
# --------------------------------

def obtener_pesos():
    return pesos
