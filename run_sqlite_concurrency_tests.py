#!/usr/bin/env python3
"""
SQLite Concurrency Test Runner

This script runs SQLite-specific concurrency tests that properly handle
SQLite's connection limitations by using separate cache instances for each
concurrent operation.
"""

import asyncio
import sys
import time
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tests.e2e.test_sqlite_concurrency import TestSQLiteConcurrency


async def run_sqlite_concurrency_tests():
    """Run all SQLite concurrency tests and report results."""
    print("🚀 Starting SQLite Concurrency Tests")
    print("=" * 60)
    print(
        "Note: Each test uses separate cache instances to avoid SQLite connection issues"
    )
    print("=" * 60)

    test_class = TestSQLiteConcurrency()
    test_methods = [
        method
        for method in dir(test_class)
        if method.startswith("test_") and callable(getattr(test_class, method))
    ]

    passed = 0
    failed = 0
    start_time = time.time()

    for method_name in test_methods:
        print(f"\n📋 Running: {method_name}")
        print("-" * 40)

        try:
            # Run test
            method = getattr(test_class, method_name)
            await method()

            print(f"✅ PASSED: {method_name}")
            passed += 1

        except Exception as e:
            print(f"❌ FAILED: {method_name}")
            print(f"   Error: {type(e).__name__}: {str(e)}")
            failed += 1

    end_time = time.time()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("📊 SQLite Concurrency Test Results Summary")
    print("=" * 60)
    print(f"Total Tests: {passed + failed}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {(passed / (passed + failed) * 100):.1f}%")
    print(f"Duration: {duration:.2f} seconds")

    if failed == 0:
        print("\n🎉 All SQLite concurrency tests passed!")
        print("✅ SQLite cache is working correctly with separate instances")
        return True
    else:
        print(f"\n⚠️  {failed} test(s) failed. Please review the errors above.")
        return False


def main():
    """Main entry point."""
    try:
        success = asyncio.run(run_sqlite_concurrency_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
