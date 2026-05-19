# LOTTOAI PRO V11 — Instrucciones de Deployment
## Cambios incluidos en este paquete

### Archivos modificados:
- `main.py` — 6 bugs corregidos + endpoint /resultado + /health + /recalibrar-pesos
- `db.py` — pool_recycle optimizado para Neon (240s en vez de 300s)

### Archivos nuevos:
- `app/services/motor_aprendizaje.py` — Motor de aprendizaje automático post-sorteo
- `app/core/scheduler.py` — Scheduler con ciclo automático 12x/día

---

## Orden de deployment (importante seguirlo)

### PASO 1 — Subir motor_aprendizaje.py (nuevo archivo)
Crear en GitHub: `app/services/motor_aprendizaje.py`
→ Copiar contenido del archivo del mismo nombre en este paquete

### PASO 2 — Reemplazar scheduler.py
Reemplazar: `app/core/scheduler.py`
→ Copiar contenido del archivo del mismo nombre en este paquete

### PASO 3 — Reemplazar db.py
Reemplazar: `db.py`
→ Sólo cambia pool_recycle de 300 a 240

### PASO 4 — Reemplazar main.py
Reemplazar: `main.py`
→ Este es el cambio más grande. Contiene todos los fixes.

### PASO 5 — Después del deploy, ejecutar en el navegador:
1. https://api-lotto-goj5.onrender.com/fix-markov-directo
   → Limpia las probabilidades corruptas (>100%)

2. https://api-lotto-goj5.onrender.com/health
   → Verificar que todos los checks estén en ✅

3. https://api-lotto-goj5.onrender.com/recalibrar-pesos  (POST)
   → Dispara recalibración de pesos con historia ponderada

---

## Qué hace el nuevo sistema de aprendizaje

### Cada sorteo (12x al día):
1. T-5 min: genera predicción tentativa y la guarda
2. T+0: sorteo ocurre
3. T+3 min: scheduler captura el resultado real del historico
4. T+3 min: **aprender_tras_sorteo()** se ejecuta automáticamente:
   - Actualiza auditoria_ia con acierto=True/False
   - Ajusta motor_pesos_hora para esa hora específica
   - Actualiza markov_transiciones con la nueva transición
   - Recalcula λ adaptativo para esa hora
   - Guarda registro en aprendizaje_sorteo

### Cada sábado 8PM (VET):
- **recalcular_todos_los_pesos()** con historia completa ponderada:
  - Datos 2018-2022: peso 1x
  - Datos 2023-2024: peso 2x
  - Últimos 90 días: peso 3x
  - Últimos 14 días: peso 6x
  - Últimos 3 días:  peso 10x ← máximo impacto

### Endpoints nuevos:
- POST /resultado {"fecha":"2026-05-18","hora":"11:00 AM","animal":"Perro"}
  → Registra resultado manual y dispara aprendizaje

- GET /health
  → Estado de todos los subsistemas en un vistazo

- POST /recalibrar-pesos
  → Fuerza recalibración inmediata de pesos

---

## Bugs corregidos

| # | Bug | Impacto |
|---|-----|---------|
| FIX-1 | Import app.core.motor_v10 → app.services.motor_v10 | CRÍTICO — fallback de predicción nunca funcionaba |
| FIX-2 | Upsert Markov ON CONFLICT DO NOTHING → DO UPDATE | CRÍTICO — frecuencias nunca se actualizaban |
| FIX-3 | Pesos en /estado hardcodeados → leen motor_pesos_hora real | ALTO — dashboard mostraba 0.25 siempre |
| FIX-4 | Sin trigger de aprendizaje → aprendizaje automático post-sorteo | CRÍTICO — el modelo nunca aprendía en tiempo real |
| FIX-5 | Constraint UNIQUE fallaba por duplicados → limpiar antes | MEDIO — constraint nunca se aplicaba |
| FIX-6 | λ hardcodeado 0.008 → viene de BD | BAJO — λ adaptativo ahora visible en /estado |
