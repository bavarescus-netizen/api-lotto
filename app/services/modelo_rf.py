import numpy as np
from sklearn.ensemble import RandomForestClassifier


class ModeloRF:

    def __init__(self):
        self.model = RandomForestClassifier(
            n_estimators=120,
            max_depth=10,
            random_state=42
        )
        self.entrenado = False

    # --------------------------------
    # CREAR FEATURES
    # --------------------------------
    def crear_features(self, historial):

        X = []
        y = []

        for i in range(5, len(historial)):

            fila = [
                historial[i-1],
                historial[i-2],
                historial[i-3],
                historial[i-4],
                historial[i-5],
            ]

            X.append(fila)
            y.append(historial[i])

        return np.array(X), np.array(y)

    # --------------------------------
    # ENTRENAR
    # --------------------------------
    def entrenar(self, historial):

        if len(historial) < 50:
            return

        X, y = self.crear_features(historial)

        self.model.fit(X, y)
        self.entrenado = True

    # --------------------------------
    # PREDECIR PROBABILIDADES
    # --------------------------------
    def predecir(self, historial):

        if not self.entrenado or len(historial) < 5:
            return {}

        entrada = np.array([[
            historial[-1],
            historial[-2],
            historial[-3],
            historial[-4],
            historial[-5],
        ]])

        probs = self.model.predict_proba(entrada)[0]

        resultado = {}

        for i, clase in enumerate(self.model.classes_):
            resultado[int(clase)] = float(probs[i])

        return resultado
