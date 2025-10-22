#!/usr/bin/env python3
"""
Quick Performance Preview for DEM Sanity Analysis
Tests a small subset to preview expected performance patterns
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

def quick_performance_preview():
    """Run a very quick preview with limited data"""
    
    # Import and run with severely limited batches for speed
    from dem_sanity_analysis import main
    
    # Checkpoint paths
    dem_ae_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt"
    ae_only_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251003_162616_run10/checkpoints/mdmt-alphaearth-only-epoch=29-val_loss=0.8005.ckpt"
    
    # Check if files exist
    if not Path(dem_ae_checkpoint).exists():
        print(f"❌ DEM+AlphaEarth checkpoint not found: {dem_ae_checkpoint}")
        return
        
    if not Path(ae_only_checkpoint).exists():
        print(f"❌ AlphaEarth-only checkpoint not found: {ae_only_checkpoint}")
        return
    
    # Create mock arguments for very quick preview
    sys.argv = [
        'quick_performance_preview.py',
        '--dem-alphaearth-checkpoint', dem_ae_checkpoint,
        '--alphaearth-only-checkpoint', ae_only_checkpoint,
        '--huc-list', './test_huc_minimal.txt',
        '--test-data-path', '/projects/bcrm/nathanj/data/processed/test/patch_dataset',
        '--output-dir', './dem_sanity_results_preview',
        '--gaussian-sigma', '3.0',
        '--limit-batches', '2',  # VERY limited for quick preview
        '--device', 'auto'
    ]
    
    print("🔬 DEM Sanity Analysis - Quick Performance Preview")
    print("=" * 60)
    print("⚡ Running with VERY limited data (2 batches per variant)")
    print("🎯 Purpose: Preview expected performance patterns")
    print("⏱️ Expected runtime: 2-3 minutes")
    print("-" * 60)
    
    try:
        result = main()
        if result == 0:
            print("\n✅ Performance preview completed successfully!")
            print("📊 Check the preview results to see expected performance patterns")
            print("📈 If patterns look good, run full analysis with:")
            print("   sbatch run_dem_sanity_quick.sh  # 30-45 min")
            print("   # OR")
            print("   sbatch run_dem_sanity_dev.sh    # 10-15 min")
        else:
            print("\n❌ Preview failed with errors")
    except Exception as e:
        print(f"\n💥 Error during preview: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_performance_preview()