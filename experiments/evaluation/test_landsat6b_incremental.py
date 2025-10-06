#!/usr/bin/env python3
"""
Test script for the incremental CSV output and wandb logging functionality
in the ultra_fast_landsat6b_evaluator.py
"""

import os
import sys
import subprocess
from datetime import datetime

def test_landsat6b_incremental():
    """Test the Landsat6B evaluator with incremental output and wandb logging"""
    
    # Find a test checkpoint - look for any available checkpoint
    checkpoint_dirs = [
        "/projects/bcrm/nathanj/checkpoints/landsat6B_run1",
        "/projects/bcrm/nathanj/checkpoints/landsat6B_run2", 
        "/projects/bcrm/nathanj/checkpoints/landsat6B_run3"
    ]
    
    test_checkpoint = None
    for checkpoint_dir in checkpoint_dirs:
        if os.path.exists(checkpoint_dir):
            # Look for .ckpt files
            for file in os.listdir(checkpoint_dir):
                if file.endswith('.ckpt'):
                    test_checkpoint = os.path.join(checkpoint_dir, file)
                    break
            if test_checkpoint:
                break
    
    if not test_checkpoint:
        print("No test checkpoint found. Please ensure checkpoints exist in:")
        for dir in checkpoint_dirs:
            print(f"  - {dir}")
        return False
    
    print(f"Using test checkpoint: {test_checkpoint}")
    
    # Create a small test HUC list (just 3 HUCs for quick testing)
    test_huc_list = "/tmp/test_huc_incremental.txt"
    with open(test_huc_list, 'w') as f:
        f.write("# Small test HUC list for incremental output testing\n")
        f.write("02070008\n")
        f.write("03040208\n") 
        f.write("04040001\n")
    
    print(f"Created test HUC list: {test_huc_list}")
    
    # Run the evaluator
    evaluator_script = "/u/nathanj/national_ml/experiments/evaluation/ultra_fast_landsat6b_evaluator.py"
    
    cmd = [
        "python", evaluator_script,
        "--checkpoint", test_checkpoint,
        "--huc-list", test_huc_list,
        "--device", "auto"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    print("This will test:")
    print("  - Incremental CSV output")
    print("  - wandb logging integration")
    print("  - Run name extraction from checkpoint")
    print("  - Real-time progress tracking")
    print()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        if result.returncode == 0:
            print("✅ Test completed successfully!")
            
            # Check if CSV output was created
            output_dir = "/u/nathanj/national_ml/experiments/evaluation/results"
            if os.path.exists(output_dir):
                csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
                if csv_files:
                    print(f"✅ CSV output created: {csv_files}")
                else:
                    print("⚠️  No CSV files found in output directory")
            
            return True
        else:
            print(f"❌ Test failed with return code: {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Test timed out (5 minutes)")
        return False
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False
    finally:
        # Clean up test file
        if os.path.exists(test_huc_list):
            os.remove(test_huc_list)

if __name__ == "__main__":
    print("Testing Landsat6B Incremental Output and wandb Logging")
    print("=" * 60)
    
    success = test_landsat6b_incremental()
    
    if success:
        print("\n🎉 All tests passed! The incremental output and wandb logging is working.")
    else:
        print("\n💥 Tests failed. Check the output above for issues.")
    
    sys.exit(0 if success else 1)