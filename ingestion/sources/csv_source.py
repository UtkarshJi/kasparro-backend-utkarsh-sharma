"""CSV data source connector."""

import csv
import hashlib
from pathlib import Path
from typing import Any, Optional

from core.config import get_settings
from core.logging import get_logger
from ingestion.sources.base import BaseSource, FetchResult, SourceConfig
from schemas.etl import CsvProductSchema, UnifiedDataInput

logger = get_logger(__name__)
settings = get_settings()


class CsvSource(BaseSource[CsvProductSchema]):
    """Data source for CSV file ingestion."""

    def __init__(
        self,
        file_path: Optional[str] = None,
        config: Optional[SourceConfig] = None,
    ):
        if config is None:
            config = SourceConfig(
                name="csv",
                enabled=True,
                batch_size=settings.etl_batch_size,
            )
        super().__init__(config)
        self.file_path = file_path or str(Path(__file__).parent.parent.parent / "data" / "products.csv")

    @property
    def source_type(self) -> str:
        return "csv"

    def _get_file_checksum(self) -> str:
        """Get file checksum for change detection."""
        try:
            with open(self.file_path, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        except FileNotFoundError:
            return ""

    def _read_csv(self) -> list[dict[str, Any]]:
        """Read CSV file and return list of records."""
        records = []
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=1):
                    row["_row_number"] = row_num
                    row["_file_name"] = Path(self.file_path).name
                    records.append(row)
        except FileNotFoundError:
            self.logger.warning("csv_file_not_found", file_path=self.file_path)
        except Exception as e:
            self.logger.error("csv_read_error", error=str(e))
            raise
        return records

    async def fetch(
        self, checkpoint: Optional[str] = None, batch_size: Optional[int] = None
    ) -> FetchResult:
        """Fetch records from CSV file with incremental support."""
        batch_size = batch_size or self.config.batch_size

        # Parse checkpoint to get last processed row
        start_row = 0
        last_file_checksum = None
        if checkpoint:
            try:
                parts = checkpoint.split("|")
                start_row = int(parts[0])
                if len(parts) > 1:
                    last_file_checksum = parts[1]
            except (ValueError, IndexError):
                start_row = 0

        # Check if file has changed
        current_checksum = self._get_file_checksum()
        if last_file_checksum and last_file_checksum != current_checksum:
            # File changed, reprocess from beginning
            self.logger.info("csv_file_changed", old=last_file_checksum, new=current_checksum)
            start_row = 0

        # Read all records and slice
        all_records = self._read_csv()
        end_row = start_row + batch_size
        records = all_records[start_row:end_row]

        has_more = end_row < len(all_records)
        checkpoint_value = f"{end_row}|{current_checksum}" if records else None

        return FetchResult(
            records=records,
            total_fetched=len(records),
            has_more=has_more,
            checkpoint_value=checkpoint_value,
            metadata={
                "file_path": self.file_path,
                "file_checksum": current_checksum,
                "total_rows": len(all_records),
                "start_row": start_row,
                "end_row": end_row,
            },
        )

    def validate(self, record: dict[str, Any]) -> CsvProductSchema:
        """Validate CSV record against schema."""
        # Remove internal fields before validation
        clean_record = {k: v for k, v in record.items() if not k.startswith("_")}
        return CsvProductSchema.model_validate(clean_record)

    def transform(self, validated_record: CsvProductSchema) -> UnifiedDataInput:
        """Transform CSV record to unified schema with identity resolution."""
        # For CSV products, use product_id as canonical ID
        canonical_id = f"product_{validated_record.product_id}"
        
        return UnifiedDataInput(
            source=self.source_type,
            source_id=validated_record.product_id,
            canonical_id=canonical_id,
            symbol=None,  # Not applicable for products
            title=validated_record.name,
            content=validated_record.description,
            author=None,
            category=validated_record.category,
            url=None,
            external_created_at=validated_record.created_at,
            extra_data={
                "price": validated_record.price,
                "stock_quantity": validated_record.stock_quantity,
                "product_id": validated_record.product_id,
            },
            checksum=self.compute_checksum(validated_record.model_dump()),
        )

    def get_checkpoint_value(self, record: dict[str, Any]) -> str:
        """Extract row number as checkpoint value."""
        return str(record.get("_row_number", ""))
