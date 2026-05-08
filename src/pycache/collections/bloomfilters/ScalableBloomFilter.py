from . import BloomFilter


class ScalableBloomFilter(BloomFilter):
    """
    A dynamic chained array of BloomFilters with geometric growth and reduction in false positive rate as it grows
    Not used RationalBloomFilter as the error tightening might be too aggressive now as rationalbloomfilter alread
    targets the false positive rate
    """

    # determines reduce in error rate in the next filters
    TIGHTENING = 0.5
    # determines the number of elements in next filters
    GROWTH = 2

    def __init__(
        self,
        expected_number_of_elements: int,
        false_positive_rate: float,
        tightening=TIGHTENING,
        growth=GROWTH,
    ):
        self._filters: list[BloomFilter] = [
            BloomFilter(expected_number_of_elements, false_positive_rate)
        ]
        self._unique_elements_inserted = 0
        self.tightening = tightening
        self.growth = growth

    def __len__(self):
        return self._unique_elements_inserted

    def add(self, key) -> bool:
        # check if the key exists in any of the chain
        if self.exists(key):
            return False

        if not self.check_size(self.active):
            self._add_new_bf(
                self.active.number_of_elements, self.active.false_positive_rate
            )

        if not self.active.add(key):
            return False

        self._unique_elements_inserted += 1
        return True

    def exists(self, key):
        for filters in self._filters:
            if filters.exists(key):
                return True
        return False

    @property
    def active(self) -> BloomFilter:
        return self._filters[-1]

    @property
    def value(self):
        raise Exception("not implemented")

    @value.setter
    def value(self):
        raise Exception("not implemented")

    @property
    def chains(self) -> int:
        return len(self._filters)

    def _add_new_bf(self, expected_number_of_elements, false_positive_rate):
        expected_number_of_elements = self.active.number_of_elements * self.growth
        false_positive_rate = self.active.false_positive_rate * self.tightening
        self._filters.append(
            BloomFilter(expected_number_of_elements, false_positive_rate)
        )

    def check_size(self, filter: BloomFilter) -> bool:
        return filter.size < filter.number_of_elements
