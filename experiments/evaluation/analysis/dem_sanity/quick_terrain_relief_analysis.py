#!/usr/bin/env python3
"""
Quick Relief Calculation for Transfer/Robustness Analysis Planning
Calculate terrain relief across the 10 representative HUCs to determine percentile thresholds.
"""

import os
import numpy as np
import sys
from pathlib import Path
from typing import List, Tuple, Dict
import json

def load_huc_list(file_path: str) -> List[str]:
    """Load HUC codes from file, skipping comments."""
    huc_list = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                huc_list.append(line)
    return huc_list

def calculate_patch_relief(dem_patch: np.ndarray) -> float:
    """Calculate terrain relief (elevation range) within a DEM patch."""
    if dem_patch.size == 0:
        return 0.0
    
    # Relief = max elevation - min elevation within patch
    relief = np.max(dem_patch) - np.min(dem_patch)
    return float(relief)

def analyze_huc_terrain_relief(data_path: str, huc_code: str) -> Dict:
    """Analyze terrain relief for all patches in a HUC."""
    huc_dir = os.path.join(data_path, huc_code)
    
    if not os.path.exists(huc_dir):
        print(f"⚠️  HUC directory not found: {huc_dir}")
        return {"huc": huc_code, "relief_values": [], "stats": {}}
    
    # Find all patch files
    patch_files = list(Path(huc_dir).glob("patch_*.npz"))
    
    if not patch_files:
        print(f"⚠️  No patch files found in {huc_dir}")
        return {"huc": huc_code, "relief_values": [], "stats": {}}
    
    relief_values = []
    
    print(f"📊 Analyzing {len(patch_files)} patches in HUC {huc_code}...")
    
    for i, patch_file in enumerate(patch_files[:50]):  # Limit to first 50 for speed
        try:
            # Load patch data
            with np.load(patch_file) as data:
                if 'dem' in data:
                    dem_patch = data['dem']
                    
                    # Handle different DEM shapes (might be (1,224,224) or (224,224))
                    if dem_patch.ndim == 3:
                        dem_patch = dem_patch[0]  # Take first channel
                    
                    relief = calculate_patch_relief(dem_patch)
                    relief_values.append(relief)
                    
                    if i == 0:  # Show first patch info
                        print(f"  Sample patch: {dem_patch.shape}, relief={relief:.2f}m")
                        
        except Exception as e:
            print(f"  ⚠️  Error loading {patch_file}: {e}")
            continue
    
    if not relief_values:
        print(f"  ❌ No valid relief values found for HUC {huc_code}")
        return {"huc": huc_code, "relief_values": [], "stats": {}}
    
    # Calculate statistics
    relief_array = np.array(relief_values)
    stats = {
        "n_patches": len(relief_values),
        "mean": float(np.mean(relief_array)),
        "std": float(np.std(relief_array)),
        "min": float(np.min(relief_array)),
        "max": float(np.max(relief_array)),
        "p25": float(np.percentile(relief_array, 25)),
        "p50": float(np.percentile(relief_array, 50)),
        "p75": float(np.percentile(relief_array, 75)),
        "p33": float(np.percentile(relief_array, 33.3)),
        "p67": float(np.percentile(relief_array, 66.7))
    }
    
    print(f"  ✅ Relief stats: mean={stats['mean']:.1f}m, std={stats['std']:.1f}m, range={stats['min']:.1f}-{stats['max']:.1f}m")
    
    return {
        "huc": huc_code,
        "relief_values": relief_values,
        "stats": stats
    }

def main():
    # Paths
    representative_huc_file = "/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/test_huc_representative.txt"
    data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    
    print("🏔️  TERRAIN RELIEF ANALYSIS")
    print("=" * 50)
    print(f"Data path: {data_path}")
    print(f"HUC list: {representative_huc_file}")
    
    # Load HUCs
    huc_list = load_huc_list(representative_huc_file)
    print(f"Analyzing {len(huc_list)} representative HUCs...\n")
    
    # Analyze each HUC
    all_results = []
    all_relief_values = []
    
    for huc_code in huc_list:
        result = analyze_huc_terrain_relief(data_path, huc_code)
        if result["relief_values"]:
            all_results.append(result)
            all_relief_values.extend(result["relief_values"])
    
    if not all_relief_values:
        print("❌ No relief data found across any HUCs!")
        return
    
    # Calculate overall statistics
    print(f"\n📈 OVERALL TERRAIN RELIEF STATISTICS")
    print("=" * 50)
    
    all_relief = np.array(all_relief_values)
    overall_stats = {
        "total_patches": len(all_relief_values),
        "total_hucs": len(all_results),
        "mean": float(np.mean(all_relief)),
        "std": float(np.std(all_relief)),
        "min": float(np.min(all_relief)),
        "max": float(np.max(all_relief)),
        "p25": float(np.percentile(all_relief, 25)),
        "p33": float(np.percentile(all_relief, 33.3)),
        "p50": float(np.percentile(all_relief, 50)),
        "p67": float(np.percentile(all_relief, 66.7)),
        "p75": float(np.percentile(all_relief, 75))
    }
    
    print(f"Total patches analyzed: {overall_stats['total_patches']}")
    print(f"Total HUCs: {overall_stats['total_hucs']}")
    print(f"Relief range: {overall_stats['min']:.1f} - {overall_stats['max']:.1f} meters")
    print(f"Mean ± std: {overall_stats['mean']:.1f} ± {overall_stats['std']:.1f} meters")
    print(f"\nPercentile thresholds for binning:")
    print(f"  33rd percentile (low-relief threshold): {overall_stats['p33']:.1f}m")
    print(f"  67th percentile (high-relief threshold): {overall_stats['p67']:.1f}m")
    print(f"  Median: {overall_stats['p50']:.1f}m")
    
    # Per-HUC summary
    print(f"\n🗺️  PER-HUC TERRAIN CLASSIFICATION")
    print("=" * 50)
    
    low_threshold = overall_stats['p33']
    high_threshold = overall_stats['p67']
    
    low_relief_hucs = []
    medium_relief_hucs = []
    high_relief_hucs = []
    
    for result in all_results:
        huc = result["huc"]
        mean_relief = result["stats"]["mean"]
        
        if mean_relief < low_threshold:
            category = "LOW"
            low_relief_hucs.append(huc)
        elif mean_relief > high_threshold:
            category = "HIGH"
            high_relief_hucs.append(huc)
        else:
            category = "MEDIUM"
            medium_relief_hucs.append(huc)
        
        print(f"  {huc}: {mean_relief:6.1f}m ({category} relief)")
    
    print(f"\n📊 TERRAIN DISTRIBUTION")
    print(f"Low relief HUCs  (<{low_threshold:.1f}m): {len(low_relief_hucs)} - {low_relief_hucs}")
    print(f"Medium relief HUCs: {len(medium_relief_hucs)} - {medium_relief_hucs}")
    print(f"High relief HUCs (>{high_threshold:.1f}m): {len(high_relief_hucs)} - {high_relief_hucs}")
    
    # Save results
    output_file = "/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/terrain_relief_analysis.json"
    
    output_data = {
        "analysis_date": "2025-10-11",
        "data_path": data_path,
        "huc_count": len(all_results),
        "total_patches": len(all_relief_values),
        "overall_stats": overall_stats,
        "huc_results": all_results,
        "terrain_classification": {
            "low_threshold": low_threshold,
            "high_threshold": high_threshold,
            "low_relief_hucs": low_relief_hucs,
            "medium_relief_hucs": medium_relief_hucs,
            "high_relief_hucs": high_relief_hucs
        }
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    print(f"\n✅ Quick terrain analysis complete!")
    print(f"   Recommended thresholds: Low <{low_threshold:.0f}m, High >{high_threshold:.0f}m")

if __name__ == "__main__":
    main()