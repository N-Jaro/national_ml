#!/usr/bin/env python3
"""
Test script to verify the run config functionality works correctly.
"""

import sys
import os
from pathlib import Path

# Add paths
sys.path.append(str(Path(__file__).parent))

def test_config_loading():
    """Test loading run configurations."""
    
    from batch_foundation_evaluator import load_run_config
    
    config_dir = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config"
    
    print("Testing Prithvi config loading...")
    prithvi_runs = load_run_config("prithvi", config_dir)
    print(f"Loaded {len(prithvi_runs)} Prithvi runs:")
    for run in prithvi_runs:
        exists = "✓" if os.path.exists(run['checkpoint_path']) else "✗"
        print(f"  {exists} {run['run_name']}: {run['checkpoint_path']}")
    
    print("\nTesting Clay config loading...")
    clay_runs = load_run_config("clay", config_dir)
    print(f"Loaded {len(clay_runs)} Clay runs:")
    for run in clay_runs:
        exists = "✓" if os.path.exists(run['checkpoint_path']) else "✗"
        print(f"  {exists} {run['run_name']}: {run['checkpoint_path']}")
    
    return len(prithvi_runs) > 0 and len(clay_runs) > 0

def test_checkpoint_verification():
    """Test checkpoint verification."""
    
    from batch_foundation_evaluator import load_run_config, verify_checkpoint_exists
    
    config_dir = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config"
    
    # Test with first Prithvi run
    prithvi_runs = load_run_config("prithvi", config_dir)
    if prithvi_runs:
        test_run = prithvi_runs[0]
        print(f"\nTesting checkpoint verification for {test_run['run_name']}...")
        exists = verify_checkpoint_exists(test_run)
        print(f"Checkpoint exists: {exists}")
        return exists
    
    return False

def main():
    """Run config tests."""
    
    print("Testing Foundation Model Run Configuration System")
    print("=" * 60)
    
    # Test config loading
    config_test = test_config_loading()
    
    # Test checkpoint verification
    checkpoint_test = test_checkpoint_verification()
    
    print("\n" + "=" * 60)
    print("Test Results:")
    print(f"  Config Loading: {'PASS' if config_test else 'FAIL'}")
    print(f"  Checkpoint Verification: {'PASS' if checkpoint_test else 'FAIL'}")
    
    if config_test and checkpoint_test:
        print("\n🎉 All tests passed! Run config system is working correctly.")
        print("\nYou can now run:")
        print("  sbatch submit_foundation_batch_eval.sh")
        return 0
    else:
        print("\n❌ Some tests failed. Please check the issues above.")
        return 1

if __name__ == "__main__":
    exit(main())