#!/usr/bin/env python3
"""
MDMT Model Test Suite Runner

This script runs all MDMT model variant tests in sequence and provides
a comprehensive summary of results.

Usage:
    python run_all_tests.py              # Run all tests with real data
    python run_all_tests.py --demo_only  # Run architecture tests only
    python run_all_tests.py --verbose    # Detailed output
    python run_all_tests.py --variant dem_optical  # Run specific variant
"""

import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Tuple

# Test script mapping
TEST_SCRIPTS = {
    'all_modalities_alphaearth': {
        'script': 'test_all_modalities_alphaearth.py',
        'description': 'All Modalities + AlphaEarth (73 channels)',
        'channels': 73,
        'modalities': ['DEM', 'Optical', 'Thermal', 'SAR', 'AlphaEarth']
    },
    'dem_alphaearth': {
        'script': 'test_dem_alphaearth.py', 
        'description': 'DEM + AlphaEarth (65 channels)',
        'channels': 65,
        'modalities': ['DEM', 'AlphaEarth']
    },
    'alphaearth_only': {
        'script': 'test_alphaearth_only.py',
        'description': 'AlphaEarth Only (64 channels)', 
        'channels': 64,
        'modalities': ['AlphaEarth']
    },
    'dem_optical': {
        'script': 'test_dem_optical.py',
        'description': 'DEM + Optical (7 channels)',
        'channels': 7, 
        'modalities': ['DEM', 'Optical']
    },
    'dem_thermal': {
        'script': 'test_dem_thermal.py',
        'description': 'DEM + Thermal (2 channels)',
        'channels': 2,
        'modalities': ['DEM', 'Thermal'] 
    },
    'dem_sar': {
        'script': 'test_dem_sar.py',
        'description': 'DEM + SAR (2 channels)',
        'channels': 2,
        'modalities': ['DEM', 'SAR']
    }
}

def run_test(script_name: str, demo_only: bool = False, verbose: bool = False) -> Tuple[bool, str, float]:
    """Run a single test script and return (success, output, duration)."""
    cmd = ['python', script_name]
    if demo_only:
        cmd.append('--demo_only')
    
    if verbose:
        print(f"  Running: {' '.join(cmd)}")
    
    start_time = time.time()
    try:
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=300  # 5 minute timeout
        )
        duration = time.time() - start_time
        
        success = result.returncode == 0
        output = result.stdout if success else result.stderr
        
        return success, output, duration
    
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        return False, "Test timed out after 5 minutes", duration
    except Exception as e:
        duration = time.time() - start_time
        return False, f"Test failed with exception: {e}", duration

def print_header():
    """Print the test suite header."""
    print("=" * 80)
    print("🚀 MDMT MODEL TEST SUITE")
    print("=" * 80)
    print(f"Running comprehensive tests for all MDMT model variants")
    print(f"Test directory: {os.getcwd()}")
    print("")

def print_summary(results: Dict[str, Tuple[bool, str, float]], demo_only: bool):
    """Print test results summary."""
    print("\n" + "=" * 80)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for success, _, _ in results.values() if success)
    total = len(results)
    
    print(f"Tests completed: {total}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {total - passed} ❌")
    print(f"Success rate: {passed/total*100:.1f}%")
    print("")
    
    # Detailed results
    print("Detailed Results:")
    print("-" * 80)
    
    for variant, (success, output, duration) in results.items():
        config = TEST_SCRIPTS[variant]
        status = "✅ PASS" if success else "❌ FAIL"
        
        print(f"{status} | {config['description']:<35} | {duration:6.2f}s")
        
        if not success:
            # Show error details for failed tests
            lines = output.split('\n')
            error_lines = [line for line in lines if '❌' in line or 'ERROR' in line.upper() or 'FAILED' in line.upper()]
            if error_lines:
                for error_line in error_lines[:3]:  # Show first 3 error lines
                    print(f"      {error_line.strip()}")
    
    print("-" * 80)
    
    # Architecture summary
    if demo_only and passed > 0:
        print("\n🏗️  Architecture Summary:")
        print("-" * 40)
        for variant, (success, _, _) in results.items():
            if success:
                config = TEST_SCRIPTS[variant]
                modalities_str = " + ".join(config['modalities'])
                print(f"  {config['channels']:2d} channels | {modalities_str}")
    
    print("")
    
    if passed == total:
        print("🎉 All tests PASSED! The MDMT model suite is ready for training!")
    else:
        print("⚠️  Some tests failed. Check the details above.")
        print("   Use --verbose flag for more detailed output.")

def main():
    parser = argparse.ArgumentParser(description="Run MDMT model test suite")
    parser.add_argument("--demo_only", action="store_true",
                       help="Only run architecture tests (no real data required)")
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed output for each test")
    parser.add_argument("--variant", type=str, choices=list(TEST_SCRIPTS.keys()),
                       help="Run tests for specific variant only")
    parser.add_argument("--timeout", type=int, default=300,
                       help="Timeout per test in seconds (default: 300)")
    
    args = parser.parse_args()
    
    # Change to test directory
    test_dir = Path(__file__).parent
    os.chdir(test_dir)
    
    print_header()
    
    # Determine which tests to run
    if args.variant:
        tests_to_run = {args.variant: TEST_SCRIPTS[args.variant]}
        print(f"Running single variant: {TEST_SCRIPTS[args.variant]['description']}")
    else:
        tests_to_run = TEST_SCRIPTS
        print(f"Running all {len(TEST_SCRIPTS)} MDMT model variants")
    
    if args.demo_only:
        print("Mode: Architecture tests only (demo mode)")
    else:
        print("Mode: Full tests (architecture + data loading + end-to-end)")
    
    print("")
    
    # Run tests
    results = {}
    total_start_time = time.time()
    
    for i, (variant, config) in enumerate(tests_to_run.items(), 1):
        print(f"[{i}/{len(tests_to_run)}] Testing {config['description']}...")
        
        if not os.path.exists(config['script']):
            print(f"  ❌ Test script not found: {config['script']}")
            results[variant] = (False, f"Test script not found: {config['script']}", 0.0)
            continue
        
        success, output, duration = run_test(config['script'], args.demo_only, args.verbose)
        results[variant] = (success, output, duration)
        
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {status} in {duration:.2f}s")
        
        if args.verbose and output:
            # Show last few lines of output
            lines = output.split('\n')
            relevant_lines = [line for line in lines[-10:] if line.strip()]
            for line in relevant_lines[-3:]:
                print(f"    {line}")
        
        print("")
    
    total_duration = time.time() - total_start_time
    
    # Print summary
    print_summary(results, args.demo_only)
    print(f"Total execution time: {total_duration:.2f}s")
    
    # Exit with appropriate code
    failed_count = sum(1 for success, _, _ in results.values() if not success)
    sys.exit(failed_count)

if __name__ == "__main__":
    main()