"""Schema drift detection service."""

from typing import Any

from rapidfuzz import fuzz
from pydantic import BaseModel

from core.logging import get_logger

logger = get_logger(__name__)


class SchemaDriftResult(BaseModel):
    """Result of schema drift detection."""

    has_drift: bool
    confidence_score: float
    new_fields: list[str]
    removed_fields: list[str]
    type_changes: list[dict[str, Any]]
    warnings: list[str]


class SchemaDriftDetector:
    """Detects schema drift between data batches."""

    def __init__(self, fuzzy_threshold: float = 80.0):
        self.fuzzy_threshold = fuzzy_threshold
        self.logger = get_logger("schema_drift")

    def detect_drift(
        self,
        expected_schema: dict[str, str],
        actual_record: dict[str, Any],
    ) -> SchemaDriftResult:
        """
        Detect schema drift between expected schema and actual record.

        Args:
            expected_schema: Expected field names and types
            actual_record: Actual data record

        Returns:
            SchemaDriftResult with drift details
        """
        expected_fields = set(expected_schema.keys())
        actual_fields = set(actual_record.keys())

        # Find new and removed fields
        new_fields = list(actual_fields - expected_fields)
        removed_fields = list(expected_fields - actual_fields)

        # Check for fuzzy matches (renamed fields)
        fuzzy_matches = []
        for new_field in new_fields[:]:
            for removed_field in removed_fields[:]:
                similarity = fuzz.ratio(new_field, removed_field)
                if similarity >= self.fuzzy_threshold:
                    fuzzy_matches.append({
                        "old_field": removed_field,
                        "new_field": new_field,
                        "similarity": similarity,
                    })
                    new_fields.remove(new_field)
                    removed_fields.remove(removed_field)
                    break

        # Check for type changes in common fields
        type_changes = []
        common_fields = expected_fields & actual_fields
        for field in common_fields:
            expected_type = expected_schema[field]
            actual_value = actual_record[field]
            actual_type = type(actual_value).__name__

            # Type mapping for comparison
            type_mapping = {
                "str": ["str", "NoneType"],
                "int": ["int", "float", "NoneType"],
                "float": ["float", "int", "NoneType"],
                "bool": ["bool", "NoneType"],
                "list": ["list", "NoneType"],
                "dict": ["dict", "NoneType"],
            }

            compatible_types = type_mapping.get(expected_type, [expected_type, "NoneType"])
            if actual_type not in compatible_types:
                type_changes.append({
                    "field": field,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                })

        # Generate warnings
        warnings = []
        if new_fields:
            warnings.append(f"New fields detected: {', '.join(new_fields)}")
        if removed_fields:
            warnings.append(f"Missing fields: {', '.join(removed_fields)}")
        if fuzzy_matches:
            for match in fuzzy_matches:
                warnings.append(
                    f"Possible field rename: {match['old_field']} -> "
                    f"{match['new_field']} ({match['similarity']:.0f}% similar)"
                )
        if type_changes:
            for change in type_changes:
                warnings.append(
                    f"Type change in '{change['field']}': "
                    f"{change['expected_type']} -> {change['actual_type']}"
                )

        # Calculate confidence score
        total_expected = len(expected_fields)
        if total_expected == 0:
            confidence_score = 1.0
        else:
            matches = len(common_fields) - len(type_changes) + len(fuzzy_matches)
            confidence_score = max(0.0, min(1.0, matches / total_expected))

        has_drift = bool(new_fields or removed_fields or type_changes)

        # Log warnings
        if has_drift:
            for warning in warnings:
                self.logger.warning("schema_drift_detected", message=warning)

        return SchemaDriftResult(
            has_drift=has_drift,
            confidence_score=round(confidence_score, 2),
            new_fields=new_fields,
            removed_fields=removed_fields,
            type_changes=type_changes,
            warnings=warnings,
        )

    def infer_schema(self, record: dict[str, Any]) -> dict[str, str]:
        """Infer schema from a record."""
        schema = {}
        for field, value in record.items():
            if value is None:
                schema[field] = "NoneType"
            else:
                schema[field] = type(value).__name__
        return schema
