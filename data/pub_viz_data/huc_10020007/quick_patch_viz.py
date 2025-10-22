#!/usr/bin/env python3
"""
Simple Patch Visualization Script - Quick Setup

This is a simplified version for quick patch visualization with customizable parameters.
Perfect for exploring different HUCs and creating publication figures.
"""

import sys
sys.path.append('/u/nathanj/national_ml/data/pub_viz_data/huc_10020007')

from create_patch_visualization import visualize_patches, copy_random_patches
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='Create patch visualizations')
    parser.add_argument('--huc', type=str, default='03030005', 
                       help='HUC ID to visualize (default: 03030005)')
    parser.add_argument('--num_patches', type=int, default=4,
                       help='Number of patches to visualize (default: 4)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducibility (default: 42)')
    parser.add_argument('--copy_patches', action='store_true',
                       help='Also copy sample patches to directory')
    parser.add_argument('--copy_num', type=int, default=20,
                       help='Number of patches to copy (default: 20)')
    
    args = parser.parse_args()
    
    # Check if HUC exists
    base_path = Path("/u/nathanj/national_ml/data/processed/patch_dataset")
    available_hucs = [d.name for d in base_path.glob("*") if d.is_dir()]
    
    if args.huc not in available_hucs:
        print(f"❌ HUC {args.huc} not found!")
        print("Available HUCs:")
        for huc in sorted(available_hucs):
            print(f"   • {huc}")
        return
    
    print(f"🎯 Processing HUC {args.huc}")
    
    # Create visualization
    try:
        png_path, pdf_path = visualize_patches(
            huc_id=args.huc, 
            num_patches=args.num_patches,
            random_seed=args.seed
        )
        
        print(f"✅ Visualization created: {png_path}")
        
        # Optionally copy patches
        if args.copy_patches:
            sample_dir = copy_random_patches(
                source_huc=args.huc,
                num_patches=args.copy_num,
                train_test_split=True,
                random_seed=args.seed
            )
            print(f"✅ Sample patches copied to: {sample_dir}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()