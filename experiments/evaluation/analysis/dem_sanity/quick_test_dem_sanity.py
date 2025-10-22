#!/usr/bin/env python3
"""
Interactive DEM Sanity Analysis - Quick Test Version

This script provides a simplified interface for testing the DEM sanity analysis
with minimal setup and fast execution for development and validation.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

def quick_test():
    """Run a quick test of the DEM sanity analysis with minimal data"""
    
    # Actual trained model checkpoint paths
    dem_ae_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt"
    ae_only_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251003_162616_run10/checkpoints/mdmt-alphaearth-only-epoch=29-val_loss=0.8005.ckpt"
    
    # Check if files exist
    if not Path(dem_ae_checkpoint).exists():
        print(f"❌ DEM+AlphaEarth checkpoint not found: {dem_ae_checkpoint}")
        print("Please update the checkpoint path in this script")
        return
        
    if not Path(ae_only_checkpoint).exists():
        print(f"❌ AlphaEarth-only checkpoint not found: {ae_only_checkpoint}")
        print("Please update the checkpoint path in this script")
        return
    
    # Import and run the analysis
    import argparse
    from dem_sanity_analysis import main
    
    # Create mock arguments for testing
    sys.argv = [
        'quick_test_dem_sanity.py',
        '--dem-alphaearth-checkpoint', dem_ae_checkpoint,
        '--alphaearth-only-checkpoint', ae_only_checkpoint,
        '--huc-list', './test_huc_minimal.txt',
        '--test-data-path', '/projects/bcrm/nathanj/data/processed/test/patch_dataset',
        '--output-dir', './dem_sanity_results',
        '--gaussian-sigma', '3.0',
        '--limit-batches', '5',  # Limit for quick testing
        '--device', 'auto'
    ]
    
    print("🔬 Running DEM Sanity Analysis (Quick Test Mode)...")
    print("📊 This will test all variants with limited data for fast validation")
    print("-" * 60)
    
    try:
        result = main()
        if result == 0:
            print("\n✅ DEM Sanity Analysis completed successfully!")
            print("📈 Check the output directory for results and plots")
        else:
            print("\n❌ Analysis failed with errors")
    except Exception as e:
        print(f"\n💥 Error during analysis: {e}")
        import traceback
        traceback.print_exc()

def show_usage():
    """Show usage instructions"""
    
    print("🔬 DEM Sanity Analysis - Quick Test")
    print("=" * 50)
    print()
    print("This script tests whether DEM provides meaningful topographic signal")
    print("by comparing performance across different DEM modifications:")
    print()
    print("1. DEM+AlphaEarth (original)")
    print("2. SmoothedDEM+AlphaEarth (reduced topographic detail)")  
    print("3. ConstantDEM+AlphaEarth (no topographic detail)")
    print("4. AlphaEarth-only (baseline comparison)")
    print()
    print("Expected result: Performance should decrease in that order,")
    print("proving DEM contributes meaningful topographic information.")
    print()
    print("Before running:")
    print("1. Update checkpoint paths in this script")
    print("2. Ensure test data is available")
    print("3. Run: python quick_test_dem_sanity.py")
    print()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        show_usage()
    else:
        quick_test()