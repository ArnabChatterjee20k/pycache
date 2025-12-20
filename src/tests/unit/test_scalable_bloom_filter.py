import pytest
from src.pycache.collections.bloomfilters.ScalableBloomFilter import ScalableBloomFilter
from src.pycache.collections.bloomfilters.BloomFilter import BloomFilter


class TestScalableBloomFilter:
    """Memory-safe unit tests for ScalableBloomFilter"""

    # ----------------------------
    # Initialization
    # ----------------------------

    def test_init_defaults(self):
        sbf = ScalableBloomFilter(100, 0.01)
        assert sbf.chains == 1
        assert isinstance(sbf.active, BloomFilter)
        assert sbf.tightening == 0.5
        assert sbf.growth == 2
        assert len(sbf) == 0

    def test_init_custom_params(self):
        sbf = ScalableBloomFilter(50, 0.05, tightening=0.3, growth=3)
        assert sbf.tightening == 0.3
        assert sbf.growth == 3

    # ----------------------------
    # Basic add / exists
    # ----------------------------

    def test_add_and_exists(self):
        sbf = ScalableBloomFilter(10, 0.1)
        assert sbf.add("a") is True
        assert sbf.exists("a") is True
        assert len(sbf) == 1

    def test_duplicate_add(self):
        sbf = ScalableBloomFilter(10, 0.1)
        assert sbf.add("dup") is True
        assert sbf.add("dup") is False
        assert len(sbf) == 1

    def test_exists_before_add(self):
        sbf = ScalableBloomFilter(10, 0.1)
        assert isinstance(sbf.exists("x"), bool)

    # ----------------------------
    # Key variants (bounded)
    # ----------------------------

    @pytest.mark.parametrize(
        "key",
        ["", "🚀", "a" * 256, "!@#$%", "12345"],
    )
    def test_various_keys(self, key):
        sbf = ScalableBloomFilter(10, 0.1)
        sbf.add(key)
        assert sbf.exists(key) is True

    # ----------------------------
    # Chain behavior (controlled)
    # ----------------------------

    def test_active_property(self):
        sbf = ScalableBloomFilter(10, 0.1)
        assert sbf.active is sbf._filters[-1]

    def test_manual_chain_growth(self):
        sbf = ScalableBloomFilter(10, 0.1)
        sbf._add_new_bf(10, 0.1)

        assert sbf.chains == 2
        assert sbf.active.number_of_elements == 20
        assert sbf.active.false_positive_rate == 0.05

    def test_exists_across_chains(self):
        sbf = ScalableBloomFilter(10, 0.1)
        sbf.add("early")

        sbf._add_new_bf(10, 0.1)

        assert sbf.exists("early") is True

    # ----------------------------
    # Size checks (non-explosive)
    # ----------------------------

    def test_check_size_returns_bool(self):
        sbf = ScalableBloomFilter(10, 0.1)
        assert isinstance(sbf.check_size(sbf.active), bool)

    # ----------------------------
    # Length semantics
    # ----------------------------

    def test_len_counts_unique_only(self):
        sbf = ScalableBloomFilter(10, 0.1)
        sbf.add("a")
        sbf.add("b")
        sbf.add("a")
        assert len(sbf) == 2

    # ----------------------------
    # Value property (not implemented)
    # ----------------------------

    def test_value_getter_raises(self):
        sbf = ScalableBloomFilter(10, 0.1)
        with pytest.raises(Exception):
            _ = sbf.value

    def test_value_setter_raises(self):
        sbf = ScalableBloomFilter(10, 0.1)
        with pytest.raises(Exception):
            sbf.value = bytearray()
