#!/usr/bin/env python3
"""
Quick Test for Multitask Benefit Analysis
Validates the analysis with minimal data to ensure everything works
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

def quick_multitask_test():
    """Run a very quick test of the multitask benefit analysis"""
    
    # Import and run with limited batches for speed
    sys.path.append(str(Path(__file__).parent.parent))
    from multitask_benefit_analysis import main
    
    # Checkpoint path
    multitask_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt"
    
    # Check if file exists
    if not Path(multitask_checkpoint).exists():
        print(f"❌ Multitask checkpoint not found: {multitask_checkpoint}")
        print("Please update the checkpoint path in this script")
        return
    
    # Create mock arguments for very quick test
    sys.argv = [
        'quick_multitask_test.py',
        '--multitask-checkpoint', multitask_checkpoint,
        '--huc-list', '../dem_sanity/test_huc_minimal.txt',
        '--test-data-path', '/projects/bcrm/nathanj/data/processed/test/patch_dataset',
        '--output-dir', './results_test',
        '--limit-batches', '2',  # VERY limited for quick test
        '--device', 'auto'
    ]
    
    print("🔬 Multitask Benefit Analysis - Quick Test")
    print("=" * 60)
    print("⚡ Running with VERY limited data (2 batches)")
    print("🎯 Purpose: Validate connectivity and hydrological metrics")
    print("⏱️ Expected runtime: 3-5 minutes")
    print("-" * 60)
    
    try:
        result = main()
        if result == 0:
            print("\n✅ Multitask benefit test completed successfully!")
            print("📊 Check the test results to see connectivity metrics")
            print("📈 If results look good, run full analysis with:")
            print("   sbatch run_multitask_benefit.sh      # Full analysis")
            print("   # OR")
            print("   sbatch run_multitask_benefit_dev.sh  # Quick version")
        else:
            print("\n❌ Test failed with errors")
    except Exception as e:
        print(f"\n💥 Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_multitask_test()