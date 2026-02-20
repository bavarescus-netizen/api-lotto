import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ ERROR: DATABASE_URL no configurada en las variables de entorno.")

# ✅ Convertir a driver async si es necesario
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ✅ Limpieza de parámetros para compatibilidad con Neon
# Usamos sslmode=require para que asyncpg no tenga problemas
if "?" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.split("?")[0] + "?ssl=require"
else:
    DATABASE_URL += "?ssl=require"

# ✅ Configuración del Motor (Optimizado para evitar cierres inesperados)
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,      # Fundamental: reconecta si la DB se "durmió"
    pool_size=5,             # Reducido: Neon (free) tiene límites de conexiones
    max_overflow=10,         # Controlado: para no exceder los límites de Neon
    pool_recycle=300,        # Ajustado: 5 min. Neon cierra conexiones inactivas rápido
    connect_args={
        "command_timeout": 60  # Evita que una consulta pesada mate la App
    }
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
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
