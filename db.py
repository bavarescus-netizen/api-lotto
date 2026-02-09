import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL no configurada")

DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
DATABASE_URL = DATABASE_URL.replace("?sslmode=require", "")

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True
)

SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db():
    async with SessionLocal() as session:
        yield session
