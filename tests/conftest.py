"""pytest configuration and fixtures."""

from typing import AsyncGenerator
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.database import Base


# Use in-memory SQLite for tests (simulating PostgreSQL behavior)
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="function")
async def test_engine():
    """Create a test database engine."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def test_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session."""
    session_factory = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def client(test_engine) -> AsyncGenerator[AsyncClient, None]:
    """Create a test HTTP client with isolated database."""
    # Import app here to avoid circular imports and ensure fresh app
    from api.main import app
    from api.dependencies import get_db_session

    session_factory = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_session

    # Use ASGITransport for httpx 0.25+
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def sample_api_records():
    """Sample API response records."""
    return [
        {
            "id": 1,
            "userId": 1,
            "title": "Test Post 1",
            "body": "This is the body of test post 1",
        },
        {
            "id": 2,
            "userId": 1,
            "title": "Test Post 2",
            "body": "This is the body of test post 2",
        },
        {
            "id": 3,
            "userId": 2,
            "title": "Test Post 3",
            "body": "This is the body of test post 3",
        },
    ]


@pytest.fixture
def sample_csv_records():
    """Sample CSV records."""
    return [
        {
            "product_id": "PROD001",
            "name": "Test Product 1",
            "category": "Electronics",
            "price": "29.99",
            "description": "A test product",
            "stock_quantity": "100",
            "_row_number": 1,
            "_file_name": "test.csv",
        },
        {
            "product_id": "PROD002",
            "name": "Test Product 2",
            "category": "Books",
            "price": "14.99",
            "description": "Another test product",
            "stock_quantity": "50",
            "_row_number": 2,
            "_file_name": "test.csv",
        },
    ]


@pytest.fixture
def sample_rss_entries():
    """Sample RSS feed entries."""
    return [
        {
            "id": "https://example.com/entry1",
            "title": "Test RSS Entry 1",
            "link": "https://example.com/entry1",
            "summary": "Summary of entry 1",
            "content": "Full content of entry 1",
            "author": "Author 1",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
            "tags": ["tech", "news"],
            "_feed_url": "https://example.com/feed",
            "_feed_title": "Test Feed",
        },
        {
            "id": "https://example.com/entry2",
            "title": "Test RSS Entry 2",
            "link": "https://example.com/entry2",
            "summary": "Summary of entry 2",
            "content": "",
            "author": "",
            "published": "",
            "tags": [],
            "_feed_url": "https://example.com/feed",
            "_feed_title": "Test Feed",
        },
    ]


@pytest.fixture
def mock_httpx_client():
    """Mock httpx client for API tests."""
    return MagicMock()
