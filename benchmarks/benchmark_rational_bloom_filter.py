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
from pycache.collections.bloomfilters.BloomFilter import BloomFilter


class Benchmark:
    """Benchmark suite for RationalBloomFilter focusing on false positive rate vs filter size"""

    def __init__(self):
        self.metrics: List[Dict[str, Any]] = []
        self.bloom_filter_metrics: List[Dict[str, Any]] = []
        self.capacity_violation_rbf_metrics: List[Dict[str, Any]] = []
        self.capacity_violation_bf_metrics: List[Dict[str, Any]] = []

    def run_all(self) -> None:
        """Run all benchmark tests"""
        print("Starting comprehensive benchmarks...\n")

        # Generate test data once and reuse for all tests
        n = 500
        keys_train = []
        keys_test = []

        for i in range(1, n + 1):
            uuid_str = str(uuid.uuid4())
            if i > (n / 2):
                keys_train.append(uuid_str)
            else:
                keys_test.append(uuid_str)

        self.benchmark_false_positive_rate_vs_filter_size(keys_train, keys_test, n)
        self.benchmark_bloom_filter_false_positive_rate_vs_filter_size(
            keys_train, keys_test, n
        )

        # Generate larger dataset for capacity violation test
        # Need at least 10,000 elements for capacity violation
        max_capacity_violation = 10_000
        capacity_keys = []
        for i in range(max_capacity_violation):
            capacity_keys.append(str(uuid.uuid4()))

        # Use a separate set of test keys that are guaranteed not to be in capacity_keys
        capacity_test_keys = []
        for i in range(1000):  # Generate 1000 test keys
            capacity_test_keys.append(str(uuid.uuid4()))

        self.benchmark_capacity_violation(capacity_keys, capacity_test_keys)

        self.save_results()
        print("\nBenchmarks completed!")
        print("  - RationalBloomFilter results saved to benchmarks/graph.json")
        print("  - BloomFilter results saved to benchmarks/graph_bloom_filter.json")
        print(
            "  - RationalBloomFilter capacity violation saved to benchmarks/graph_capacity_violation_rational.json"
        )
        print(
            "  - BloomFilter capacity violation saved to benchmarks/graph_capacity_violation_bloom.json"
        )

    def benchmark_false_positive_rate_vs_filter_size(
        self, keys_train: List[str], keys_test: List[str], n: int
    ) -> None:
        """Benchmark: False Positive Rate vs Filter Size
        Tests how false positive rate changes as filter size increases
        """
        print("Running: False Positive Rate vs Filter Size (RationalBloomFilter)...")

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
                    "activation_probability": filter_obj._activation_proabability,
                },
            }

            self.metrics.append(metric)
            print(
                f"  Target FPR: {target_rate:.6f}, Actual FPR: {actual_false_positive_rate:.6f}, "
                f"Filter Size: {filter_size} bits, False Positives: {false_positive_count}"
            )

    def benchmark_bloom_filter_false_positive_rate_vs_filter_size(
        self, keys_train: List[str], keys_test: List[str], n: int
    ) -> None:
        """Benchmark: False Positive Rate vs Filter Size for standard BloomFilter
        Tests how false positive rate changes as filter size increases
        """
        print("\nRunning: False Positive Rate vs Filter Size (BloomFilter)...")

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

            filter_obj = BloomFilter(n, target_rate)
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
                },
            }

            self.bloom_filter_metrics.append(metric)
            print(
                f"  Target FPR: {target_rate:.6f}, Actual FPR: {actual_false_positive_rate:.6f}, "
                f"Filter Size: {filter_size} bits, False Positives: {false_positive_count}"
            )

    def benchmark_capacity_violation(
        self, capacity_keys: List[str], test_keys: List[str]
    ) -> None:
        """Benchmark: Capacity Violation
        Tests how false positive rate changes when inserting more elements than expected.
        This is where RationalBloomFilter should show its advantage over standard BloomFilter.

        Fixed configuration: expected_n=500, target_p=0.01
        Progressive insertion: [500, 1_000, 2_000, 5_000, 10_000] elements
        """
        print("\nRunning: Capacity Violation Benchmark...")

        expected_n = 500
        target_p = 0.01
        insertion_levels = [500, 1_000, 2_000, 5_000, 10_000]

        base_timestamp = datetime.now(timezone.utc)

        for idx, num_insertions in enumerate(insertion_levels):
            if num_insertions > len(capacity_keys):
                print(f"  Skipping {num_insertions} insertions (insufficient keys)")
                continue

            # Create timestamp for this metric
            timestamp = (
                base_timestamp.replace(microsecond=0) + timedelta(seconds=idx)
            ).isoformat() + "Z"

            # Get the keys to insert for this level
            keys_to_insert = capacity_keys[:num_insertions]

            # Test RationalBloomFilter
            rbf_filter = RationalBloomFilter(expected_n, target_p)
            for key in keys_to_insert:
                rbf_filter.add(key)

            # Check false positives for RationalBloomFilter
            rbf_false_positive_count = 0
            for test_key in test_keys:
                if rbf_filter.exists(test_key):
                    if test_key not in keys_to_insert:
                        rbf_false_positive_count += 1

            rbf_actual_fpr = (
                rbf_false_positive_count / len(test_keys) if test_keys else 0
            )

            # Test standard BloomFilter
            bf_filter = BloomFilter(expected_n, target_p)
            for key in keys_to_insert:
                bf_filter.add(key)

            # Check false positives for BloomFilter
            bf_false_positive_count = 0
            for test_key in test_keys:
                if bf_filter.exists(test_key):
                    if test_key not in keys_to_insert:
                        bf_false_positive_count += 1

            bf_actual_fpr = bf_false_positive_count / len(test_keys) if test_keys else 0

            # Create metric entry for RationalBloomFilter
            rbf_metric = {
                "timestamp": timestamp,
                "inserted_elements": num_insertions,
                "expected_elements": expected_n,
                "capacity_ratio": num_insertions / expected_n,
                "target_false_positive_rate": target_p,
                "fpr_ratio_to_target": (
                    rbf_actual_fpr / target_p if target_p > 0 else float("inf")
                ),
                "false_positive_rate": rbf_actual_fpr,
                "false_positive_count": rbf_false_positive_count,
                "test_keys": len(test_keys),
                "filter_size_bits": rbf_filter.size,
                "hash_functions": rbf_filter._number_of_hash_functions,
                "floor_k": rbf_filter.floor_k,
                "activation_probability": rbf_filter._activation_proabability,
            }

            # Create metric entry for BloomFilter
            bf_metric = {
                "timestamp": timestamp,
                "inserted_elements": num_insertions,
                "expected_elements": expected_n,
                "capacity_ratio": num_insertions / expected_n,
                "target_false_positive_rate": target_p,
                "fpr_ratio_to_target": (
                    bf_actual_fpr / target_p if target_p > 0 else float("inf")
                ),
                "false_positive_rate": bf_actual_fpr,
                "false_positive_count": bf_false_positive_count,
                "test_keys": len(test_keys),
                "filter_size_bits": bf_filter.size,
                "hash_functions": bf_filter._number_of_hash_functions,
            }

            self.capacity_violation_rbf_metrics.append(rbf_metric)
            self.capacity_violation_bf_metrics.append(bf_metric)
            print(
                f"  Inserted: {num_insertions} ({num_insertions/expected_n:.1f}x capacity) | "
                f"RationalBloomFilter FPR: {rbf_actual_fpr:.6f} | "
                f"BloomFilter FPR: {bf_actual_fpr:.6f}"
            )

    def save_results(self) -> None:
        """Save benchmark results to JSON files"""
        output_dir = os.path.dirname(__file__)

        # Save RationalBloomFilter results
        output_path = os.path.join(output_dir, "graph.json")
        sorted_metrics = sorted(self.metrics, key=lambda x: x["filter_size"]["bits"])
        result = {"metrics": sorted_metrics}

        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)

        print(
            f"\nSaved {len(sorted_metrics)} RationalBloomFilter metric entries to {output_path}"
        )
        if sorted_metrics:
            print(
                f"Filter size range: {sorted_metrics[0]['filter_size']['bits']} - "
                f"{sorted_metrics[-1]['filter_size']['bits']} bits"
            )

        # Save BloomFilter results
        output_path_bf = os.path.join(output_dir, "graph_bloom_filter.json")
        sorted_bf_metrics = sorted(
            self.bloom_filter_metrics, key=lambda x: x["filter_size"]["bits"]
        )
        result_bf = {"metrics": sorted_bf_metrics}

        with open(output_path_bf, "w") as f:
            json.dump(result_bf, f, indent=2)

        print(
            f"\nSaved {len(sorted_bf_metrics)} BloomFilter metric entries to {output_path_bf}"
        )
        if sorted_bf_metrics:
            print(
                f"Filter size range: {sorted_bf_metrics[0]['filter_size']['bits']} - "
                f"{sorted_bf_metrics[-1]['filter_size']['bits']} bits"
            )

        # Save capacity violation results for RationalBloomFilter
        output_path_cv_rbf = os.path.join(
            output_dir, "graph_capacity_violation_rational.json"
        )
        sorted_cv_rbf_metrics = sorted(
            self.capacity_violation_rbf_metrics, key=lambda x: x["inserted_elements"]
        )
        result_cv_rbf = {"metrics": sorted_cv_rbf_metrics}

        with open(output_path_cv_rbf, "w") as f:
            json.dump(result_cv_rbf, f, indent=2)

        print(
            f"\nSaved {len(sorted_cv_rbf_metrics)} RationalBloomFilter capacity violation metric entries to {output_path_cv_rbf}"
        )
        if sorted_cv_rbf_metrics:
            print(
                f"Insertion range: {sorted_cv_rbf_metrics[0]['inserted_elements']} - "
                f"{sorted_cv_rbf_metrics[-1]['inserted_elements']} elements "
                f"({sorted_cv_rbf_metrics[0]['capacity_ratio']:.1f}x - {sorted_cv_rbf_metrics[-1]['capacity_ratio']:.1f}x capacity)"
            )

        # Save capacity violation results for BloomFilter
        output_path_cv_bf = os.path.join(
            output_dir, "graph_capacity_violation_bloom.json"
        )
        sorted_cv_bf_metrics = sorted(
            self.capacity_violation_bf_metrics, key=lambda x: x["inserted_elements"]
        )
        result_cv_bf = {"metrics": sorted_cv_bf_metrics}

        with open(output_path_cv_bf, "w") as f:
            json.dump(result_cv_bf, f, indent=2)

        print(
            f"\nSaved {len(sorted_cv_bf_metrics)} BloomFilter capacity violation metric entries to {output_path_cv_bf}"
        )
        if sorted_cv_bf_metrics:
            print(
                f"Insertion range: {sorted_cv_bf_metrics[0]['inserted_elements']} - "
                f"{sorted_cv_bf_metrics[-1]['inserted_elements']} elements "
                f"({sorted_cv_bf_metrics[0]['capacity_ratio']:.1f}x - {sorted_cv_bf_metrics[-1]['capacity_ratio']:.1f}x capacity)"
            )


if __name__ == "__main__":
    benchmark = Benchmark()
    benchmark.run_all()
