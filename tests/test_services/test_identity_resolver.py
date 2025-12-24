"""Tests for identity resolution service."""

import pytest

from services.identity_resolver import IdentityResolver, get_identity_resolver


class TestIdentityResolver:
    """Tests for IdentityResolver class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.resolver = IdentityResolver()

    def test_normalize_symbol_lowercase(self):
        """Test that symbols are normalized to lowercase."""
        assert self.resolver.normalize_symbol("BTC") == "btc"
        assert self.resolver.normalize_symbol("ETH") == "eth"
        assert self.resolver.normalize_symbol("DOGE") == "doge"

    def test_normalize_symbol_strips_whitespace(self):
        """Test that whitespace is stripped."""
        assert self.resolver.normalize_symbol("  BTC  ") == "btc"
        assert self.resolver.normalize_symbol("\teth\n") == "eth"

    def test_normalize_symbol_handles_empty(self):
        """Test that empty symbols return empty string."""
        assert self.resolver.normalize_symbol("") == ""
        assert self.resolver.normalize_symbol(None) == ""

    def test_normalize_symbol_known_aliases(self):
        """Test that known aliases are resolved."""
        assert self.resolver.normalize_symbol("tether") == "usdt"
        assert self.resolver.normalize_symbol("usd-coin") == "usdc"
        # wrapped-bitcoin strips "wrapped-" prefix first, leaving "bitcoin"
        assert self.resolver.normalize_symbol("wrapped-bitcoin") == "bitcoin"
        assert self.resolver.normalize_symbol("wbtc") == "wbtc"

    def test_get_canonical_id_from_symbol(self):
        """Test canonical ID generation from symbol."""
        # CoinPaprika Bitcoin
        canonical_id_paprika = self.resolver.get_canonical_id(
            source="coinpaprika",
            source_id="btc-bitcoin",
            symbol="BTC",
            name="Bitcoin",
        )
        
        # CoinGecko Bitcoin
        canonical_id_gecko = self.resolver.get_canonical_id(
            source="coingecko",
            source_id="bitcoin",
            symbol="btc",
            name="Bitcoin",
        )
        
        # Both should produce the same canonical ID
        assert canonical_id_paprika == canonical_id_gecko == "btc"

    def test_get_canonical_id_ethereum(self):
        """Test canonical ID for Ethereum from both sources."""
        # CoinPaprika Ethereum
        canonical_id_paprika = self.resolver.get_canonical_id(
            source="coinpaprika",
            source_id="eth-ethereum",
            symbol="ETH",
            name="Ethereum",
        )
        
        # CoinGecko Ethereum
        canonical_id_gecko = self.resolver.get_canonical_id(
            source="coingecko",
            source_id="ethereum",
            symbol="eth",
            name="Ethereum",
        )
        
        # Both should produce the same canonical ID
        assert canonical_id_paprika == canonical_id_gecko == "eth"

    def test_get_canonical_id_various_coins(self):
        """Test canonical ID for various cryptocurrencies."""
        test_cases = [
            # (source, source_id, symbol, name, expected_canonical_id)
            ("coinpaprika", "sol-solana", "SOL", "Solana", "sol"),
            ("coingecko", "solana", "sol", "Solana", "sol"),
            ("coinpaprika", "ada-cardano", "ADA", "Cardano", "ada"),
            ("coingecko", "cardano", "ada", "Cardano", "ada"),
            ("coinpaprika", "doge-dogecoin", "DOGE", "Dogecoin", "doge"),
            ("coingecko", "dogecoin", "doge", "Dogecoin", "doge"),
        ]
        
        for source, source_id, symbol, name, expected in test_cases:
            canonical_id = self.resolver.get_canonical_id(
                source=source,
                source_id=source_id,
                symbol=symbol,
                name=name,
            )
            assert canonical_id == expected, f"Failed for {source} {name}: expected {expected}, got {canonical_id}"

    def test_get_canonical_id_fallback_to_name(self):
        """Test fallback to name when symbol is empty."""
        canonical_id = self.resolver.get_canonical_id(
            source="unknown",
            source_id="btc-123",
            symbol="",
            name="Bitcoin",
        )
        # Should use NAME_TO_SYMBOL mapping
        assert canonical_id == "btc"

    def test_get_canonical_id_fallback_to_source_id(self):
        """Test fallback to source-prefixed ID when no symbol or name."""
        canonical_id = self.resolver.get_canonical_id(
            source="unknown",
            source_id="xyz123",
            symbol="",
            name=None,
        )
        # Should use source-prefixed ID as fallback
        assert canonical_id == "unknown_xyz123"

    def test_merge_extra_data_new(self):
        """Test merging extra data when no existing data."""
        new_data = {"price_usd": 50000, "rank": 1}
        merged = self.resolver.merge_extra_data(None, "coinpaprika", new_data)
        
        assert merged["price_usd"] == 50000
        assert merged["rank"] == 1
        assert merged["_coinpaprika"] == new_data

    def test_merge_extra_data_existing(self):
        """Test merging extra data with existing data."""
        existing = {"price_usd": 49000, "_coingecko": {"price_usd": 49000}}
        new_data = {"price_usd": 50000, "rank": 1}
        merged = self.resolver.merge_extra_data(existing, "coinpaprika", new_data)
        
        # Latest values should be at top level
        assert merged["price_usd"] == 50000
        assert merged["rank"] == 1
        # Source-specific data preserved
        assert merged["_coingecko"] == {"price_usd": 49000}
        assert merged["_coinpaprika"] == new_data


class TestIdentityUnificationIntegration:
    """Integration tests to verify identity unification across sources."""

    def test_coinpaprika_and_coingecko_produce_same_canonical_id(self):
        """Verify that the same coin from different sources produces same canonical_id.
        
        This is the core requirement: Bitcoin from CoinPaprika and CoinGecko must
        have the same canonical_id so they merge into one unified record.
        """
        resolver = get_identity_resolver()
        
        # Simulated CoinPaprika Bitcoin data
        coinpaprika_btc = resolver.get_canonical_id(
            source="coinpaprika",
            source_id="btc-bitcoin",
            symbol="BTC",
            name="Bitcoin",
        )
        
        # Simulated CoinGecko Bitcoin data
        coingecko_btc = resolver.get_canonical_id(
            source="coingecko",
            source_id="bitcoin",
            symbol="btc",
            name="Bitcoin",
        )
        
        # CRITICAL: Both must produce the same canonical_id for unification
        assert coinpaprika_btc == coingecko_btc, (
            f"Identity unification failed! CoinPaprika={coinpaprika_btc}, "
            f"CoinGecko={coingecko_btc}. They should be identical."
        )
        assert coinpaprika_btc == "btc"

    def test_top_10_coins_unify_correctly(self):
        """Test that top 10 coins unify correctly from both sources."""
        resolver = get_identity_resolver()
        
        # Top coins with their expected canonical IDs
        test_coins = [
            # (CoinPaprika format, CoinGecko format, Expected canonical_id)
            (("btc-bitcoin", "BTC", "Bitcoin"), ("bitcoin", "btc", "Bitcoin"), "btc"),
            (("eth-ethereum", "ETH", "Ethereum"), ("ethereum", "eth", "Ethereum"), "eth"),
            (("usdt-tether", "USDT", "Tether"), ("tether", "usdt", "Tether"), "usdt"),
            (("bnb-binance-coin", "BNB", "Binance Coin"), ("binancecoin", "bnb", "BNB"), "bnb"),
            (("sol-solana", "SOL", "Solana"), ("solana", "sol", "Solana"), "sol"),
            (("xrp-xrp", "XRP", "XRP"), ("ripple", "xrp", "XRP"), "xrp"),
            (("usdc-usd-coin", "USDC", "USD Coin"), ("usd-coin", "usdc", "USD Coin"), "usdc"),
            (("ada-cardano", "ADA", "Cardano"), ("cardano", "ada", "Cardano"), "ada"),
            (("doge-dogecoin", "DOGE", "Dogecoin"), ("dogecoin", "doge", "Dogecoin"), "doge"),
            (("dot-polkadot", "DOT", "Polkadot"), ("polkadot", "dot", "Polkadot"), "dot"),
        ]
        
        for (p_id, p_sym, p_name), (g_id, g_sym, g_name), expected_canonical in test_coins:
            paprika_canonical = resolver.get_canonical_id("coinpaprika", p_id, p_sym, p_name)
            gecko_canonical = resolver.get_canonical_id("coingecko", g_id, g_sym, g_name)
            
            assert paprika_canonical == gecko_canonical == expected_canonical, (
                f"Unification failed for {p_name}: "
                f"CoinPaprika={paprika_canonical}, CoinGecko={gecko_canonical}, "
                f"Expected={expected_canonical}"
            )


class TestGetIdentityResolver:
    """Tests for global identity resolver singleton."""

    def test_returns_same_instance(self):
        """Test that get_identity_resolver returns singleton."""
        resolver1 = get_identity_resolver()
        resolver2 = get_identity_resolver()
        assert resolver1 is resolver2

    def test_resolver_is_identity_resolver(self):
        """Test that returned instance is IdentityResolver."""
        resolver = get_identity_resolver()
        assert isinstance(resolver, IdentityResolver)
