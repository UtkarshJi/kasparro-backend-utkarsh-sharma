"""Database connection and session management."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from core.config import get_settings
from core.logging import get_logger

settings = get_settings()
logger = get_logger(__name__)

# Create async engine with connection pooling
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

# Session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base."""

    pass


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for getting database sessions."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_session_context() -> AsyncGenerator[AsyncSession, None]:
    """Context manager for database sessions."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def check_db_connection() -> bool:
    """Check if database connection is healthy."""
    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
            return True
    except Exception:
        return False


async def _check_schema_needs_migration() -> bool:
    """Check if unified_data table needs migration (missing canonical_id column)."""
    try:
        async with async_session_factory() as session:
            # Check if canonical_id column exists
            result = await session.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'unified_data' AND column_name = 'canonical_id'"
            ))
            row = result.fetchone()
            if row is None:
                # Table exists but missing canonical_id column - needs migration
                return True
            return False
    except Exception:
        # Table doesn't exist yet, will be created fresh
        return False


async def init_db() -> None:
    """Initialize database tables with schema migration support.
    
    Handles the case where unified_data table exists but is missing
    the canonical_id column (added for identity unification).
    """
    # Check if we need to migrate the schema
    needs_migration = await _check_schema_needs_migration()
    
    if needs_migration:
        logger.info("schema_migration_needed", reason="canonical_id column missing")
        async with engine.begin() as conn:
            # Drop and recreate unified_data table to add new columns
            # This is safe because unified_data can be regenerated from raw data
            await conn.execute(text("DROP TABLE IF EXISTS unified_data CASCADE"))
            logger.info("unified_data_table_dropped")
    
    # Create all tables (or recreate unified_data with new schema)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    if needs_migration:
        logger.info("schema_migration_completed", reason="canonical_id column added")


async def close_db() -> None:
    """Close database connections."""
    await engine.dispose()

