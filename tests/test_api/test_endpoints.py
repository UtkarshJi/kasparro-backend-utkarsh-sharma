"""Tests for API endpoints."""

import pytest
from httpx import AsyncClient


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""

    @pytest.mark.asyncio
    async def test_health_returns_ok(self, client: AsyncClient):
        """Test health endpoint returns 200."""
        response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "db_connected" in data
        assert "etl" in data
        assert "metadata" in data

    @pytest.mark.asyncio
    async def test_health_includes_metadata(self, client: AsyncClient):
        """Test health endpoint includes request metadata."""
        response = await client.get("/health")

        data = response.json()
        metadata = data["metadata"]
        assert "request_id" in metadata
        assert "api_latency_ms" in metadata
        assert "timestamp" in metadata


class TestDataEndpoint:
    """Tests for GET /data endpoint."""

    @pytest.mark.asyncio
    async def test_data_returns_ok(self, client: AsyncClient):
        """Test data endpoint returns 200."""
        response = await client.get("/data")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "total" in data
        assert "limit" in data
        assert "offset" in data
        assert "has_more" in data
        assert "metadata" in data

    @pytest.mark.asyncio
    async def test_data_pagination(self, client: AsyncClient):
        """Test data endpoint pagination."""
        response = await client.get("/data?limit=5&offset=0")

        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 5
        assert data["offset"] == 0

    @pytest.mark.asyncio
    async def test_data_pagination_limits(self, client: AsyncClient):
        """Test pagination limit validation."""
        # Limit too large
        response = await client.get("/data?limit=200")
        assert response.status_code == 422  # Validation error

        # Negative offset
        response = await client.get("/data?offset=-1")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_data_filtering_by_source(self, client: AsyncClient):
        """Test filtering by source."""
        response = await client.get("/data?source=api")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @pytest.mark.asyncio
    async def test_data_includes_metadata(self, client: AsyncClient):
        """Test data endpoint includes metadata."""
        response = await client.get("/data")

        data = response.json()
        metadata = data["metadata"]
        assert "request_id" in metadata
        assert "api_latency_ms" in metadata
        assert metadata["api_latency_ms"] >= 0


class TestStatsEndpoint:
    """Tests for GET /stats endpoint."""

    @pytest.mark.asyncio
    async def test_stats_returns_ok(self, client: AsyncClient):
        """Test stats endpoint returns 200."""
        response = await client.get("/stats")

        assert response.status_code == 200
        data = response.json()
        assert "summary" in data
        assert "by_source" in data
        assert "metadata" in data

    @pytest.mark.asyncio
    async def test_stats_summary_structure(self, client: AsyncClient):
        """Test stats summary has correct structure."""
        response = await client.get("/stats")

        data = response.json()
        summary = data["summary"]
        assert "total_runs" in summary
        assert "successful_runs" in summary
        assert "failed_runs" in summary
        assert "total_records_processed" in summary


class TestRunsEndpoint:
    """Tests for GET /runs endpoint."""

    @pytest.mark.asyncio
    async def test_runs_returns_ok(self, client: AsyncClient):
        """Test runs endpoint returns 200."""
        response = await client.get("/runs")

        assert response.status_code == 200
        data = response.json()
        assert "runs" in data
        assert "total" in data
        assert "metadata" in data

    @pytest.mark.asyncio
    async def test_runs_limit(self, client: AsyncClient):
        """Test runs endpoint respects limit."""
        response = await client.get("/runs?limit=5")

        assert response.status_code == 200
        data = response.json()
        assert len(data["runs"]) <= 5


class TestCompareRunsEndpoint:
    """Tests for GET /compare-runs endpoint."""

    @pytest.mark.asyncio
    async def test_compare_runs_missing_params(self, client: AsyncClient):
        """Test compare-runs endpoint requires parameters."""
        response = await client.get("/compare-runs")

        assert response.status_code == 422  # Missing required params

    @pytest.mark.asyncio
    async def test_compare_runs_not_found(self, client: AsyncClient):
        """Test compare-runs with non-existent run IDs."""
        response = await client.get(
            "/compare-runs?run_id_1=nonexistent1&run_id_2=nonexistent2"
        )

        assert response.status_code == 404


class TestMetricsEndpoint:
    """Tests for GET /metrics endpoint."""

    @pytest.mark.asyncio
    async def test_metrics_returns_prometheus_format(self, client: AsyncClient):
        """Test metrics endpoint returns Prometheus format."""
        response = await client.get("/metrics")

        assert response.status_code == 200
        # Prometheus metrics should be text format
        assert "text/plain" in response.headers.get("content-type", "") or \
               "text" in response.headers.get("content-type", "")


class TestRootEndpoint:
    """Tests for GET / endpoint."""

    @pytest.mark.asyncio
    async def test_root_returns_ok(self, client: AsyncClient):
        """Test root endpoint returns 200."""
        response = await client.get("/")

        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "docs" in data
