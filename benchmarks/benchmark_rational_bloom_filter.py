"""
Comprehensive Benchmark Suite for RationalBloomFilter
Generates metrics in a simple array format for graphing
"""

import json
import time
import uuid
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Add src to path to import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pycache.collections.bloomfilters.RationalBloomFilter import RationalBloomFilter


class Benchmark:
    """Benchmark suite for RationalBloomFilter focusing on false positive rate vs filter size"""

    def __init__(self):
        self.metrics: List[Dict[str, Any]] = []

    def run_all(self) -> None:
        """Run all benchmark tests"""
        print("Starting comprehensive benchmarks...\n")

        self.benchmark_false_positive_rate_vs_filter_size()

        self.save_results()
        print("\nBenchmarks completed! Results saved to benchmarks/graph.json")

    def benchmark_false_positive_rate_vs_filter_size(self) -> None:
        """Benchmark: False Positive Rate vs Filter Size
        Tests how false positive rate changes as filter size increases
        """
        print("Running: False Positive Rate vs Filter Size...")
        n = 500
        keys_train = []
        keys_test = []

        # Generate test data once and reuse for all tests
        for i in range(1, n + 1):
            uuid_str = str(uuid.uuid4())
            if i > (n / 2):
                keys_train.append(uuid_str)
            else:
                keys_test.append(uuid_str)

        # Test different false positive rates to get different filter sizes
        # Using a wider range with more granular steps for better curve
        false_positive_rates = [
            0.9,
            0.8,
            0.7,
            0.6,
            0.5,
            0.4,
            0.3,
            0.25,
            0.2,
            0.15,
            0.1,
            0.08,
            0.06,
            0.05,
            0.04,
            0.03,
            0.02,
            0.015,
            0.01,
            0.008,
            0.006,
            0.005,
            0.004,
            0.003,
            0.002,
            0.0015,
            0.001,
            0.0008,
            0.0006,
            0.0005,
            0.0004,
            0.0003,
            0.0002,
            0.0001,
        ]

        base_timestamp = datetime.now(timezone.utc)

        for idx, target_rate in enumerate(false_positive_rates):
            # Create timestamp for this metric
            timestamp = (
                base_timestamp.replace(microsecond=0) + timedelta(seconds=idx)
            ).isoformat() + "Z"

            filter_obj = RationalBloomFilter(n, target_rate)
            filter_size = filter_obj.size

            # Add training keys
            for train_key in keys_train:
                filter_obj.add(train_key)

            # Check for false positives
            false_positive_count = 0
            for test_key in keys_test:
                if filter_obj.exists(test_key):
                    if test_key not in keys_train:
                        false_positive_count += 1

            actual_false_positive_rate = (
                false_positive_count / len(keys_test) if keys_test else 0
            )

            # Create metric entry
            metric = {
                "timestamp": timestamp,
                "false_positive_rate": {
                    "target": target_rate,
                    "actual": actual_false_positive_rate,
                    "count": false_positive_count,
                    "test_keys": len(keys_test),
                },
                "filter_size": {
                    "bits": filter_size,
                    "bytes": filter_size / 8,
                    "kb": (filter_size / 8) / 1024,
                },
                "configuration": {
                    "number_of_elements": n,
                    "hash_functions": filter_obj._number_of_hash_functions,
                    "floor_k": filter_obj.floor_k,
                    "activation_probability": filter_obj._activation_proability,
                },
            }

            self.metrics.append(metric)
            print(
                f"  Target FPR: {target_rate:.6f}, Actual FPR: {actual_false_positive_rate:.6f}, "
                f"Filter Size: {filter_size} bits, False Positives: {false_positive_count}"
            )

    def save_results(self) -> None:
        """Save benchmark results to JSON file"""
        output_dir = os.path.dirname(__file__)
        output_path = os.path.join(output_dir, "graph.json")

        # Sort metrics by filter_size.bits to ensure proper ordering for graphing
        sorted_metrics = sorted(self.metrics, key=lambda x: x["filter_size"]["bits"])

        result = {"metrics": sorted_metrics}

        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)

        print(f"\nSaved {len(sorted_metrics)} metric entries to {output_path}")
        print(
            f"Filter size range: {sorted_metrics[0]['filter_size']['bits']} - "
            f"{sorted_metrics[-1]['filter_size']['bits']} bits"
        )


if __name__ == "__main__":
    benchmark = Benchmark()
    benchmark.run_all()
