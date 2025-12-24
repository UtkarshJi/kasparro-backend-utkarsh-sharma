"""Identity resolution service for unifying cryptocurrency data across sources."""

import re
from typing import Optional

from core.logging import get_logger

logger = get_logger(__name__)


class IdentityResolver:
    """Resolves cryptocurrency identities across different data sources.
    
    This service creates canonical identifiers for cryptocurrencies, allowing
    data from multiple sources (CoinPaprika, CoinGecko, etc.) to be unified
    under a single identity.
    """

    # Known symbol mappings for edge cases where sources use different symbols
    SYMBOL_ALIASES = {
        # Some stablecoins have variations
        "usdt": "usdt",
        "tether": "usdt",
        "usdc": "usdc",
        "usd-coin": "usdc",
        # Common variations
        "wbtc": "wbtc",
        "wrapped-bitcoin": "wbtc",
        "weth": "weth", 
        "wrapped-ether": "weth",
    }

    # Known name to symbol mappings for popular coins
    NAME_TO_SYMBOL = {
        "bitcoin": "btc",
        "ethereum": "eth",
        "tether": "usdt",
        "usd coin": "usdc",
        "binance coin": "bnb",
        "ripple": "xrp",
        "cardano": "ada",
        "solana": "sol",
        "dogecoin": "doge",
        "polkadot": "dot",
        "polygon": "matic",
        "litecoin": "ltc",
        "chainlink": "link",
        "avalanche": "avax",
        "uniswap": "uni",
    }

    def __init__(self):
        self.logger = get_logger("identity_resolver")

    def normalize_symbol(self, symbol: str) -> str:
        """Normalize a cryptocurrency symbol to a standard format.
        
        Args:
            symbol: The symbol from any source (e.g., "BTC", "btc", "Bitcoin")
            
        Returns:
            Normalized lowercase symbol
        """
        if not symbol:
            return ""
        
        # Convert to lowercase and strip whitespace
        normalized = symbol.lower().strip()
        
        # Remove common suffixes/prefixes
        normalized = re.sub(r'^wrapped[-_\s]?', '', normalized)
        normalized = re.sub(r'[-_\s]?token$', '', normalized)
        
        # Check for known aliases
        if normalized in self.SYMBOL_ALIASES:
            return self.SYMBOL_ALIASES[normalized]
        
        return normalized

    def get_canonical_id(
        self,
        source: str,
        source_id: str,
        symbol: str,
        name: Optional[str] = None,
    ) -> str:
        """Generate a canonical identifier for a cryptocurrency.
        
        The canonical ID is based on the normalized symbol, which is the most
        reliable way to match cryptocurrencies across sources. For example,
        Bitcoin from both CoinPaprika and CoinGecko will have canonical_id "btc".
        
        Args:
            source: The data source (coinpaprika, coingecko, etc.)
            source_id: The source-specific ID
            symbol: The cryptocurrency symbol (BTC, ETH, etc.)
            name: Optional name for additional matching
            
        Returns:
            Canonical identifier string
        """
        # Primary: use normalized symbol
        normalized_symbol = self.normalize_symbol(symbol)
        
        if normalized_symbol:
            self.logger.debug(
                "canonical_id_resolved",
                source=source,
                source_id=source_id,
                symbol=symbol,
                canonical_id=normalized_symbol,
            )
            return normalized_symbol
        
        # Fallback: try to extract from name
        if name:
            name_lower = name.lower().strip()
            if name_lower in self.NAME_TO_SYMBOL:
                return self.NAME_TO_SYMBOL[name_lower]
            # Use first word of name as last resort
            first_word = name_lower.split()[0] if name_lower else ""
            if first_word:
                return first_word[:10]  # Limit length
        
        # Last resort: use source-prefixed ID
        self.logger.warning(
            "canonical_id_fallback",
            source=source,
            source_id=source_id,
            reason="no_symbol_or_name",
        )
        return f"{source}_{source_id}"

    def merge_extra_data(
        self,
        existing_data: Optional[dict],
        new_source: str,
        new_data: dict,
    ) -> dict:
        """Merge extra_data from multiple sources for the same coin.
        
        This preserves data from all sources under source-specific keys,
        allowing comparison and cross-validation.
        
        Args:
            existing_data: Current extra_data dict (may be None)
            new_source: The source of the new data
            new_data: The new data to merge
            
        Returns:
            Merged extra_data dictionary
        """
        if existing_data is None:
            existing_data = {}
        
        # Store source-specific data under source key
        merged = dict(existing_data)
        merged[f"_{new_source}"] = new_data
        
        # Also keep top-level fields for easy access (use latest)
        for key, value in new_data.items():
            if not key.startswith("_"):
                merged[key] = value
        
        return merged


# Global instance
_resolver: Optional[IdentityResolver] = None


def get_identity_resolver() -> IdentityResolver:
    """Get or create the global identity resolver instance."""
    global _resolver
    if _resolver is None:
        _resolver = IdentityResolver()
    return _resolver
