"""Tests for services module."""

import pytest
from services.schema_drift import SchemaDriftDetector


class TestSchemaDriftDetector:
    """Tests for schema drift detection."""

    @pytest.fixture
    def detector(self):
        return SchemaDriftDetector(fuzzy_threshold=80.0)

    def test_no_drift(self, detector):
        """Test detection when schemas match."""
        expected = {"id": "int", "name": "str", "value": "float"}
        record = {"id": 1, "name": "test", "value": 10.5}

        result = detector.detect_drift(expected, record)

        assert not result.has_drift
        assert result.confidence_score == 1.0
        assert len(result.new_fields) == 0
        assert len(result.removed_fields) == 0

    def test_new_fields(self, detector):
        """Test detection of new fields."""
        expected = {"id": "int", "name": "str"}
        record = {"id": 1, "name": "test", "new_field": "value"}

        result = detector.detect_drift(expected, record)

        assert result.has_drift
        assert "new_field" in result.new_fields
        assert "New fields detected" in result.warnings[0]

    def test_removed_fields(self, detector):
        """Test detection of removed fields."""
        expected = {"id": "int", "name": "str", "old_field": "str"}
        record = {"id": 1, "name": "test"}

        result = detector.detect_drift(expected, record)

        assert result.has_drift
        assert "old_field" in result.removed_fields
        assert "Missing fields" in result.warnings[0]

    def test_type_change(self, detector):
        """Test detection of type changes."""
        expected = {"id": "int", "name": "str"}
        record = {"id": 1, "name": ["list", "instead"]}  # str -> list

        result = detector.detect_drift(expected, record)

        assert result.has_drift
        assert len(result.type_changes) == 1
        assert result.type_changes[0]["field"] == "name"

    def test_fuzzy_match_renamed_field(self, detector):
        """Test fuzzy matching for renamed fields."""
        expected = {"user_name": "str", "id": "int"}
        record = {"username": "test", "id": 1}  # user_name -> username

        result = detector.detect_drift(expected, record)

        # Should detect as possible rename
        assert any("rename" in w.lower() for w in result.warnings)

    def test_infer_schema(self, detector):
        """Test schema inference from record."""
        record = {
            "id": 1,
            "name": "test",
            "value": 10.5,
            "active": True,
            "tags": ["a", "b"],
            "meta": {"key": "value"},
        }

        schema = detector.infer_schema(record)

        assert schema["id"] == "int"
        assert schema["name"] == "str"
        assert schema["value"] == "float"
        assert schema["active"] == "bool"
        assert schema["tags"] == "list"
        assert schema["meta"] == "dict"

    def test_confidence_score_decreases_with_drift(self, detector):
        """Test confidence score decreases with more drift."""
        expected = {"a": "int", "b": "str", "c": "float", "d": "bool"}

        # No drift
        record1 = {"a": 1, "b": "test", "c": 1.0, "d": True}
        result1 = detector.detect_drift(expected, record1)

        # Some drift
        record2 = {"a": 1, "b": "test", "new": "field"}
        result2 = detector.detect_drift(expected, record2)

        assert result1.confidence_score > result2.confidence_score
