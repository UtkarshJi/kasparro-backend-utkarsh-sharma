"""Tests for failure scenarios."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient

from ingestion.sources.api_source import ApiSource
from ingestion.sources.base import SourceConfig
from services.rate_limiter import RateLimiter, TokenBucket


class TestDatabaseFailures:
    """Tests for database connection failures."""

    @pytest.mark.asyncio
    async def test_health_reports_db_disconnected(self, client: AsyncClient):
        """Test health endpoint reports when DB is down."""
        # This test would normally mock the DB connection
        # For now, just verify the endpoint structure
        response = await client.get("/health")
        assert response.status_code == 200
        assert "db_connected" in response.json()


class TestAPISourceFailures:
    """Tests for API source failure scenarios."""

    @pytest.fixture
    def api_source(self):
        config = SourceConfig(name="api", enabled=True, batch_size=10)
        return ApiSource(config)

    @pytest.mark.asyncio
    async def test_fetch_handles_http_error(self, api_source):
        """Test fetch handles HTTP errors gracefully."""
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.side_effect = Exception("Connection refused")

            with pytest.raises(Exception) as exc_info:
                await api_source.fetch()

            assert "Connection refused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_handles_invalid_json(self, api_source):
        """Test fetch handles invalid JSON response."""
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )

            # Should raise or handle the error
            with pytest.raises(Exception):
                await api_source.fetch()


class TestRateLimiterFailures:
    """Tests for rate limiter behavior."""

    def test_token_bucket_blocks_when_empty(self):
        """Test token bucket blocks requests when empty."""
        bucket = TokenBucket(rate=1.0, capacity=2)

        # Drain the bucket
        import asyncio

        async def drain():
            assert await bucket.acquire()  # Token 1
            assert await bucket.acquire()  # Token 2
            assert not await bucket.acquire()  # Should fail

        asyncio.get_event_loop().run_until_complete(drain())

    def test_rate_limiter_tracks_errors(self):
        """Test rate limiter tracks error counts."""
        limiter = RateLimiter()

        limiter.record_error("api")
        limiter.record_error("api")

        stats = limiter.get_stats()
        assert stats["error_counts"]["api"] == 2

    def test_rate_limiter_backoff_increases(self):
        """Test exponential backoff increases with errors."""
        limiter = RateLimiter()

        # No errors - no backoff
        assert limiter.get_backoff_seconds("api") == 0

        # Record errors
        limiter.record_error("api")
        backoff1 = limiter.get_backoff_seconds("api")

        limiter.record_error("api")
        backoff2 = limiter.get_backoff_seconds("api")

        assert backoff2 > backoff1

    def test_rate_limiter_resets_errors(self):
        """Test error count resets after success."""
        limiter = RateLimiter()

        limiter.record_error("api")
        limiter.record_error("api")
        assert limiter.get_stats()["error_counts"]["api"] == 2

        limiter.reset_errors("api")
        assert limiter.get_stats()["error_counts"]["api"] == 0


class TestSchemaMismatch:
    """Tests for schema mismatch scenarios."""

    @pytest.fixture
    def api_source(self):
        config = SourceConfig(name="api", enabled=True)
        return ApiSource(config)

    def test_validate_missing_required_field(self, api_source):
        """Test validation fails for missing required fields."""
        record = {
            "title": "Test",
            "body": "Body",
            # Missing id and userId
        }

        with pytest.raises(Exception):
            api_source.validate(record)

    def test_validate_wrong_type(self, api_source):
        """Test validation handles wrong types."""
        record = {
            "id": "not_an_int",  # Should be int
            "userId": 1,
            "title": "Test",
            "body": "Body",
        }

        # Pydantic should handle type coercion or raise error
        with pytest.raises(Exception):
            api_source.validate(record)

    @pytest.mark.asyncio
    async def test_batch_processing_continues_after_failure(self, api_source):
        """Test batch processing continues after individual failures."""
        records = [
            {"id": 1, "userId": 1, "title": "Valid", "body": "Body"},
            {"invalid": "record"},  # Will fail
            {"id": 2, "userId": 1, "title": "Also Valid", "body": "Body"},
        ]

        successful, failed = await api_source.process_batch(records)

        # Should still process valid records
        assert len(successful) == 2
        assert len(failed) == 1


class TestETLRecovery:
    """Tests for ETL recovery scenarios."""

    @pytest.mark.asyncio
    async def test_checkpoint_enables_resume(self, tmp_path):
        """Test that checkpoint allows resuming from failure."""
        from ingestion.sources.csv_source import CsvSource
        from ingestion.sources.base import SourceConfig

        # Create test CSV
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "product_id,name,category,price\n"
            "PROD001,Product 1,Cat1,10.00\n"
            "PROD002,Product 2,Cat2,20.00\n"
            "PROD003,Product 3,Cat3,30.00\n"
        )

        source = CsvSource(
            file_path=str(csv_file),
            config=SourceConfig(name="csv", batch_size=1),
        )

        # First fetch
        result1 = await source.fetch(batch_size=1)
        assert result1.total_fetched == 1
        assert result1.has_more

        # "Fail" here - but we have checkpoint
        checkpoint = result1.checkpoint_value

        # Resume from checkpoint
        result2 = await source.fetch(checkpoint=checkpoint, batch_size=1)
        assert result2.total_fetched == 1
        assert result2.records[0]["product_id"] == "PROD002"
