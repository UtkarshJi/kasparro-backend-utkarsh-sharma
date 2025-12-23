"""Ingestion sources module exports."""

from ingestion.sources.base import BaseSource, FetchResult, SourceConfig
from ingestion.sources.coinpaprika_source import CoinPaprikaSource
from ingestion.sources.coingecko_source import CoinGeckoSource
from ingestion.sources.csv_source import CsvSource
from ingestion.sources.rss_source import RssSource

# Legacy - kept for backward compatibility
from ingestion.sources.api_source import ApiSource

__all__ = [
    "BaseSource",
    "SourceConfig",
    "FetchResult",
    "CoinPaprikaSource",
    "CoinGeckoSource",
    "CsvSource",
    "RssSource",
    "ApiSource",  # Legacy
]
