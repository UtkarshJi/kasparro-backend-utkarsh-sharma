"""Tests for data source connectors."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.sources.api_source import ApiSource
from ingestion.sources.csv_source import CsvSource
from ingestion.sources.rss_source import RssSource
from ingestion.sources.base import SourceConfig


class TestApiSource:
    """Tests for API source connector."""

    @pytest.fixture
    def api_source(self):
        """Create API source instance."""
        config = SourceConfig(name="api", enabled=True, batch_size=10)
        return ApiSource(config)

    def test_source_type(self, api_source):
        """Test source type is correct."""
        assert api_source.source_type == "api"

    def test_validate(self, api_source, sample_api_records):
        """Test record validation."""
        record = sample_api_records[0]
        validated = api_source.validate(record)

        assert validated.id == 1
        assert validated.user_id == 1
        assert validated.title == "Test Post 1"

    def test_transform(self, api_source, sample_api_records):
        """Test record transformation to unified schema."""
        record = sample_api_records[0]
        validated = api_source.validate(record)
        transformed = api_source.transform(validated)

        assert transformed.source == "api"
        assert transformed.source_id == "1"
        assert transformed.title == "Test Post 1"
        assert transformed.content == "This is the body of test post 1"
        assert transformed.author == "user_1"
        assert transformed.category == "posts"
        assert transformed.checksum is not None

    def test_compute_checksum(self, api_source):
        """Test checksum computation is consistent."""
        data = {"id": 1, "title": "Test"}
        checksum1 = api_source.compute_checksum(data)
        checksum2 = api_source.compute_checksum(data)

        assert checksum1 == checksum2
        assert len(checksum1) == 64  # SHA-256 hex length

    def test_compute_checksum_different_data(self, api_source):
        """Test different data produces different checksums."""
        data1 = {"id": 1, "title": "Test"}
        data2 = {"id": 2, "title": "Test"}

        assert api_source.compute_checksum(data1) != api_source.compute_checksum(data2)


class TestCsvSource:
    """Tests for CSV source connector."""

    @pytest.fixture
    def csv_source(self, tmp_path):
        """Create CSV source with temp file."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "product_id,name,category,price,description,stock_quantity\n"
            "PROD001,Test Product,Electronics,29.99,A test product,100\n"
            "PROD002,Another Product,Books,14.99,Another product,50\n"
        )
        config = SourceConfig(name="csv", enabled=True, batch_size=10)
        return CsvSource(file_path=str(csv_file), config=config)

    def test_source_type(self, csv_source):
        """Test source type is correct."""
        assert csv_source.source_type == "csv"

    @pytest.mark.asyncio
    async def test_fetch(self, csv_source):
        """Test fetching records from CSV."""
        result = await csv_source.fetch()

        assert result.total_fetched == 2
        assert len(result.records) == 2
        assert result.has_more is False

    @pytest.mark.asyncio
    async def test_fetch_with_batch_size(self, csv_source):
        """Test fetching with batch size limit."""
        result = await csv_source.fetch(batch_size=1)

        assert result.total_fetched == 1
        assert result.has_more is True

    @pytest.mark.asyncio
    async def test_fetch_incremental(self, csv_source):
        """Test incremental fetching with checkpoint."""
        # First batch
        result1 = await csv_source.fetch(batch_size=1)
        assert result1.total_fetched == 1

        # Second batch using checkpoint
        result2 = await csv_source.fetch(
            checkpoint=result1.checkpoint_value,
            batch_size=1,
        )
        assert result2.total_fetched == 1
        assert result2.records[0]["product_id"] == "PROD002"

    def test_validate(self, csv_source, sample_csv_records):
        """Test record validation."""
        record = sample_csv_records[0]
        validated = csv_source.validate(record)

        assert validated.product_id == "PROD001"
        assert validated.price == 29.99
        assert validated.stock_quantity == 100

    def test_transform(self, csv_source, sample_csv_records):
        """Test record transformation."""
        record = sample_csv_records[0]
        validated = csv_source.validate(record)
        transformed = csv_source.transform(validated)

        assert transformed.source == "csv"
        assert transformed.source_id == "PROD001"
        assert transformed.title == "Test Product 1"
        assert transformed.category == "Electronics"
        assert transformed.extra_data["price"] == 29.99


class TestRssSource:
    """Tests for RSS source connector."""

    @pytest.fixture
    def rss_source(self):
        """Create RSS source instance."""
        config = SourceConfig(name="rss", enabled=True, batch_size=10)
        return RssSource(feed_urls=["https://example.com/feed"], config=config)

    def test_source_type(self, rss_source):
        """Test source type is correct."""
        assert rss_source.source_type == "rss"

    def test_validate(self, rss_source, sample_rss_entries):
        """Test entry validation."""
        entry = sample_rss_entries[0]
        validated = rss_source.validate(entry)

        assert validated.entry_id == "https://example.com/entry1"
        assert validated.title == "Test RSS Entry 1"
        assert len(validated.tags) == 2

    def test_transform(self, rss_source, sample_rss_entries):
        """Test entry transformation."""
        entry = sample_rss_entries[0]
        validated = rss_source.validate(entry)
        transformed = rss_source.transform(validated)

        assert transformed.source == "rss"
        assert transformed.source_id == "https://example.com/entry1"
        assert transformed.title == "Test RSS Entry 1"
        assert transformed.content == "Full content of entry 1"
        assert transformed.author == "Author 1"
        assert transformed.category == "news"


class TestSourceProcessBatch:
    """Tests for batch processing functionality."""

    @pytest.fixture
    def api_source(self):
        config = SourceConfig(name="api", enabled=True)
        return ApiSource(config)

    @pytest.mark.asyncio
    async def test_process_batch_success(self, api_source, sample_api_records):
        """Test successful batch processing."""
        successful, failed = await api_source.process_batch(sample_api_records)

        assert len(successful) == 3
        assert len(failed) == 0

    @pytest.mark.asyncio
    async def test_process_batch_with_invalid_records(self, api_source):
        """Test batch processing with some invalid records."""
        records = [
            {"id": 1, "userId": 1, "title": "Valid", "body": "Body"},
            {"invalid": "record"},  # Missing required fields
            {"id": 2, "userId": 1, "title": "Also Valid", "body": "Body"},
        ]

        successful, failed = await api_source.process_batch(records)

        assert len(successful) == 2
        assert len(failed) == 1
