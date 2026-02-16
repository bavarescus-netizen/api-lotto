import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ ERROR: DATABASE_URL no configurada en las variables de entorno.")

# ✅ Convertir a driver async si es necesario
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ✅ Limpieza de parámetros para compatibilidad con Neon
DATABASE_URL = DATABASE_URL.split("?")[0] + "?ssl=require"

# ✅ Configuración del Motor (Optimizado para carga de datos masiva)
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,      # Verifica la conexión antes de usarla
    pool_size=10,            # Máximo de conexiones abiertas
    max_overflow=20,         # Conexiones extra en picos de tráfico
    pool_recycle=3600        # Reinicia conexiones cada hora para evitar bloqueos
)

SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Inyección de dependencia para FastAPI
async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
