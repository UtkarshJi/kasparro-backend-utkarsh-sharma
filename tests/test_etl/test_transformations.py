"""Tests for ETL transformation logic."""

import pytest
from datetime import datetime

from schemas.etl import (
    ApiPostSchema,
    CsvProductSchema,
    RssEntrySchema,
    UnifiedDataInput,
)


class TestApiPostSchema:
    """Tests for API post schema validation."""

    def test_valid_post(self):
        """Test valid post validation."""
        data = {
            "id": 1,
            "userId": 1,
            "title": "Test Title",
            "body": "Test body content",
        }
        post = ApiPostSchema.model_validate(data)

        assert post.id == 1
        assert post.user_id == 1
        assert post.title == "Test Title"
        assert post.body == "Test body content"

    def test_title_strip_whitespace(self):
        """Test that titles are stripped of whitespace."""
        data = {
            "id": 1,
            "userId": 1,
            "title": "  Test Title  ",
            "body": "Body",
        }
        post = ApiPostSchema.model_validate(data)
        assert post.title == "Test Title"

    def test_invalid_missing_id(self):
        """Test validation fails without id."""
        data = {
            "userId": 1,
            "title": "Test",
            "body": "Body",
        }
        with pytest.raises(Exception):
            ApiPostSchema.model_validate(data)


class TestCsvProductSchema:
    """Tests for CSV product schema validation."""

    def test_valid_product(self):
        """Test valid product validation."""
        data = {
            "product_id": "PROD001",
            "name": "Test Product",
            "category": "Electronics",
            "price": "29.99",
            "description": "A test product",
            "stock_quantity": "100",
        }
        product = CsvProductSchema.model_validate(data)

        assert product.product_id == "PROD001"
        assert product.name == "Test Product"
        assert product.price == 29.99
        assert product.stock_quantity == 100

    def test_price_parsing(self):
        """Test price parsing removes currency symbols."""
        data = {
            "product_id": "PROD001",
            "name": "Test",
            "category": "Test",
            "price": "$29.99",
        }
        product = CsvProductSchema.model_validate(data)
        assert product.price == 29.99

    def test_price_with_comma(self):
        """Test price parsing handles commas."""
        data = {
            "product_id": "PROD001",
            "name": "Test",
            "category": "Test",
            "price": "1,299.99",
        }
        product = CsvProductSchema.model_validate(data)
        assert product.price == 1299.99

    def test_empty_stock_quantity(self):
        """Test empty stock quantity defaults to 0."""
        data = {
            "product_id": "PROD001",
            "name": "Test",
            "category": "Test",
            "price": "10.00",
            "stock_quantity": "",
        }
        product = CsvProductSchema.model_validate(data)
        assert product.stock_quantity == 0


class TestRssEntrySchema:
    """Tests for RSS entry schema validation."""

    def test_valid_entry(self):
        """Test valid RSS entry validation."""
        data = {
            "id": "https://example.com/entry1",
            "title": "Test Entry",
            "link": "https://example.com/entry1",
            "summary": "Summary",
            "content": "Full content",
            "author": "Author",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
            "tags": ["tech"],
        }
        entry = RssEntrySchema.model_validate(data)

        assert entry.entry_id == "https://example.com/entry1"
        assert entry.title == "Test Entry"
        assert entry.published is not None

    def test_empty_published_date(self):
        """Test empty published date is None."""
        data = {
            "id": "test",
            "title": "Test",
            "link": "https://example.com",
            "published": "",
        }
        entry = RssEntrySchema.model_validate(data)
        assert entry.published is None

    def test_iso_date_format(self):
        """Test ISO date format parsing."""
        data = {
            "id": "test",
            "title": "Test",
            "link": "https://example.com",
            "published": "2024-01-15T10:30:00Z",
        }
        entry = RssEntrySchema.model_validate(data)
        assert entry.published is not None


class TestUnifiedDataInput:
    """Tests for unified data input schema."""

    def test_valid_unified_data(self):
        """Test valid unified data creation."""
        data = UnifiedDataInput(
            source="api",
            source_id="123",
            title="Test Title",
            content="Test content",
            author="Author",
            category="Category",
            url="https://example.com",
            external_created_at=datetime.now(),
            extra_data={"key": "value"},
            checksum="abc123",
        )

        assert data.source == "api"
        assert data.source_id == "123"
        assert data.extra_data["key"] == "value"

    def test_optional_fields_none(self):
        """Test optional fields can be None."""
        data = UnifiedDataInput(
            source="csv",
            source_id="456",
            checksum="def456",
        )

        assert data.title is None
        assert data.content is None
        assert data.author is None
