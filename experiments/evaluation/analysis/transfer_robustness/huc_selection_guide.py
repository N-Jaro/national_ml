#!/usr/bin/env python3
"""
HUC Selection Guide for Transfer/Robustness Analysis

This script helps you understand and choose HUCs strategically based on:
1. Geographic diversity
2. Terrain characteristics  
3. Data availability
4. Analysis objectives
"""

import os
import sys
import pandas as pd
from pathlib import Path
from collections import defaultdict
import json

# Add project paths
sys.path.append('/u/nathanj/national_ml/experiments')

def analyze_available_hucs():
    """Analyze all available HUCs in the processed dataset."""
    
    patch_dataset_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
    
    if not os.path.exists(patch_dataset_path):
        print(f"❌ Patch dataset not found: {patch_dataset_path}")
        return {}
    
    available_hucs = []
    huc_info = {}
    
    # Get all HUC directories
    for huc_dir in Path(patch_dataset_path).iterdir():
        if huc_dir.is_dir() and len(huc_dir.name) == 8:
            huc_code = huc_dir.name
            available_hucs.append(huc_code)
            
            # Count patches
            patch_files = list(huc_dir.glob("*.npz"))
            patch_count = len(patch_files)
            
            # Check for modalities (if first patch exists)
            modalities = []
            if patch_files:
                try:
                    import numpy as np
                    sample_patch = np.load(patch_files[0])
                    modalities = list(sample_patch.keys())
                except:
                    modalities = ["unknown"]
            
            huc_info[huc_code] = {
                'patch_count': patch_count,
                'modalities': modalities,
                'region': classify_huc_region(huc_code)
            }
    
    return huc_info

def classify_huc_region(huc_code):
    """Classify HUC by major hydrologic region (first 2 digits)."""
    region_map = {
        '01': 'New England',
        '02': 'Mid-Atlantic', 
        '03': 'South Atlantic-Gulf',
        '04': 'Great Lakes',
        '05': 'Ohio',
        '06': 'Tennessee',
        '07': 'Upper Mississippi',
        '08': 'Lower Mississippi',
        '09': 'Souris-Red-Rainy',
        '10': 'Missouri',
        '11': 'Arkansas-White-Red',
        '12': 'Texas-Gulf',
        '13': 'Rio Grande',
        '14': 'Upper Colorado',
        '15': 'Lower Colorado',
        '16': 'Great Basin',
        '17': 'Pacific Northwest',
        '18': 'California',
        '19': 'Alaska',
        '20': 'Hawaii',
        '21': 'Caribbean'
    }
    
    region_code = huc_code[:2]
    return region_map.get(region_code, f'Unknown ({region_code})')

def get_geographic_diversity_selection(huc_info, count=5):
    """Select HUCs for maximum geographic diversity."""
    
    # Group by region
    regions = defaultdict(list)
    for huc, info in huc_info.items():
        regions[info['region']].append((huc, info))
    
    # Select one HUC from each region, prioritizing patch count
    selected = []
    region_names = []
    
    for region, hucs in regions.items():
        if len(selected) >= count:
            break
        
        # Sort by patch count (descending) to get best data availability
        hucs.sort(key=lambda x: x[1]['patch_count'], reverse=True)
        selected.append(hucs[0][0])
        region_names.append(region)
    
    return selected, region_names

def get_high_data_availability_selection(huc_info, count=5):
    """Select HUCs with highest patch counts for statistical power."""
    
    # Sort by patch count
    sorted_hucs = sorted(huc_info.items(), key=lambda x: x[1]['patch_count'], reverse=True)
    
    selected = [huc for huc, info in sorted_hucs[:count]]
    patch_counts = [info['patch_count'] for huc, info in sorted_hucs[:count]]
    
    return selected, patch_counts

def get_terrain_diversity_selection(huc_info, count=5):
    """Select HUCs representing different terrain types based on geographic regions."""
    
    # Terrain diversity strategy: select from different physiographic regions
    priority_regions = [
        'Great Lakes',           # Glacial terrain, low relief
        'Pacific Northwest',     # Mountainous, high relief  
        'Great Basin',          # Arid, variable relief
        'South Atlantic-Gulf',  # Coastal plains, low relief
        'Upper Colorado',       # High altitude, extreme relief
        'Missouri',             # Great Plains, moderate relief
        'California',           # Mediterranean, mixed terrain
        'Texas-Gulf',           # Subtropical, low-moderate relief
    ]
    
    selected = []
    selected_regions = []
    
    # Group by region
    regions = defaultdict(list)
    for huc, info in huc_info.items():
        regions[info['region']].append((huc, info))
    
    # Select from priority regions first
    for region in priority_regions:
        if len(selected) >= count:
            break
        if region in regions:
            # Get HUC with most patches from this region
            best_huc = max(regions[region], key=lambda x: x[1]['patch_count'])
            selected.append(best_huc[0])
            selected_regions.append(region)
    
    # Fill remaining slots with highest patch count HUCs
    remaining_hucs = [huc for huc in huc_info.keys() if huc not in selected]
    remaining_sorted = sorted(remaining_hucs, key=lambda x: huc_info[x]['patch_count'], reverse=True)
    
    while len(selected) < count and remaining_sorted:
        selected.append(remaining_sorted.pop(0))
        selected_regions.append(huc_info[selected[-1]]['region'])
    
    return selected, selected_regions

def analyze_current_selection():
    """Analyze the currently used HUC selection."""
    current_hucs = ["03030005", "04060102", "07040006", "08020301", "10270104"]
    
    print("🔍 CURRENT HUC SELECTION ANALYSIS")
    print("=" * 50)
    
    huc_info = analyze_available_hucs()
    
    for huc in current_hucs:
        if huc in huc_info:
            info = huc_info[huc]
            print(f"📍 {huc}: {info['region']}")
            print(f"   Patches: {info['patch_count']}")
            print(f"   Modalities: {', '.join(info['modalities'])}")
        else:
            print(f"❌ {huc}: Not found in processed data")
        print()

def print_selection_recommendations():
    """Print different HUC selection strategies."""
    
    print("🎯 HUC SELECTION STRATEGIES")
    print("=" * 60)
    
    huc_info = analyze_available_hucs()
    
    if not huc_info:
        print("❌ No HUC data available")
        return
    
    print(f"📊 Total available HUCs: {len(huc_info)}")
    total_patches = sum(info['patch_count'] for info in huc_info.values())
    print(f"📦 Total patches: {total_patches:,}")
    print()
    
    # Strategy 1: Geographic Diversity
    geo_hucs, geo_regions = get_geographic_diversity_selection(huc_info, 5)
    print("🌍 STRATEGY 1: GEOGRAPHIC DIVERSITY")
    print("   Goal: Maximum regional coverage")
    print("   Recommended HUCs:")
    for huc, region in zip(geo_hucs, geo_regions):
        patches = huc_info[huc]['patch_count']
        print(f"   • {huc}: {region} ({patches} patches)")
    print()
    
    # Strategy 2: High Data Availability  
    data_hucs, data_counts = get_high_data_availability_selection(huc_info, 5)
    print("📈 STRATEGY 2: HIGH DATA AVAILABILITY")
    print("   Goal: Maximum statistical power")
    print("   Recommended HUCs:")
    for huc, count in zip(data_hucs, data_counts):
        region = huc_info[huc]['region']
        print(f"   • {huc}: {region} ({count} patches)")
    print()
    
    # Strategy 3: Terrain Diversity
    terrain_hucs, terrain_regions = get_terrain_diversity_selection(huc_info, 5)
    print("🏔️  STRATEGY 3: TERRAIN DIVERSITY")
    print("   Goal: Different physiographic regions")
    print("   Recommended HUCs:")
    for huc, region in zip(terrain_hucs, terrain_regions):
        patches = huc_info[huc]['patch_count']
        print(f"   • {huc}: {region} ({patches} patches)")
    print()
    
    # Current selection analysis
    analyze_current_selection()
    
    # Runtime estimates
    print("⏱️  RUNTIME ESTIMATES (50 patches/HUC)")
    print("=" * 40)
    print("Transfer Analysis (4a): ~5-7 min/HUC")
    print("Robustness Analysis (4b): ~3-5 min/HUC") 
    print("Total per HUC: ~8-12 minutes")
    print("5 HUCs total: ~40-60 minutes")
    print()
    
    # Quick mode estimates
    print("⚡ QUICK MODE ESTIMATES (20 patches/HUC)")
    print("=" * 40)
    print("Total per HUC: ~3-5 minutes")
    print("5 HUCs total: ~15-25 minutes")

def generate_huc_commands():
    """Generate example commands for different strategies."""
    
    huc_info = analyze_available_hucs()
    
    if not huc_info:
        return
        
    geo_hucs, _ = get_geographic_diversity_selection(huc_info, 5)
    data_hucs, _ = get_high_data_availability_selection(huc_info, 5)
    terrain_hucs, _ = get_terrain_diversity_selection(huc_info, 5)
    
    print("🚀 COMMAND EXAMPLES")
    print("=" * 40)
    print()
    
    print("Geographic Diversity:")
    print(f"python run_transfer_robustness.py --hucs \"{','.join(geo_hucs)}\"")
    print()
    
    print("High Data Availability:")  
    print(f"python run_transfer_robustness.py --hucs \"{','.join(data_hucs)}\"")
    print()
    
    print("Terrain Diversity:")
    print(f"python run_transfer_robustness.py --hucs \"{','.join(terrain_hucs)}\"")
    print()
    
    print("Quick Tests:")
    print(f"python run_transfer_robustness.py --quick --hucs \"{','.join(geo_hucs[:3])}\"")

def main():
    """Main function."""
    print("🔍 HUC SELECTION GUIDE FOR TRANSFER/ROBUSTNESS ANALYSIS")
    print("=" * 70)
    print()
    
    print_selection_recommendations()
    print()
    generate_huc_commands()

if __name__ == "__main__":
    main()