#!/usr/bin/env python3
"""
Count patches per HUC2 (Water Resource Region) for test HUC analysis.
This helps understand geographic distribution and diversity of test HUCs.
"""

import os
import glob
import pandas as pd
from collections import defaultdict
from pathlib import Path
import argparse

# US Water Resource Regions (HUC2 codes)
HUC2_REGIONS = {
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
    '21': 'Caribbean',
    '22': 'Alaska (continued)'
}

def load_test_hucs(test_huc_file):
    """Load test HUC codes from file."""
    test_hucs = []
    with open(test_huc_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                test_hucs.append(line)
    return test_hucs

def count_patches_by_huc2(patch_base_dir, test_hucs):
    """Count patches organized by HUC2 (Water Resource Region)."""
    huc2_stats = defaultdict(lambda: {
        'huc_count': 0,
        'total_patches': 0,
        'huc_list': [],
        'patch_counts': []
    })
    
    print(f"Analyzing patch distribution by HUC2 (Water Resource Region)...")
    print(f"Scanning: {patch_base_dir}")
    
    for huc in test_hucs:
        huc2 = huc[:2]  # First 2 digits = Water Resource Region
        huc_dir = os.path.join(patch_base_dir, huc)
        
        patch_count = 0
        if os.path.exists(huc_dir):
            # Count different file types that might be patches
            patterns = ['*.npz', '*.tif', '*.tiff', '*.npy', '*.h5', '*.hdf5']
            for pattern in patterns:
                files = glob.glob(os.path.join(huc_dir, pattern))
                patch_count += len(files)
                
                # Also check subdirectories
                subdirs = glob.glob(os.path.join(huc_dir, '*', pattern))
                patch_count += len(subdirs)
        
        # Update HUC2 statistics
        huc2_stats[huc2]['huc_count'] += 1
        huc2_stats[huc2]['total_patches'] += patch_count
        huc2_stats[huc2]['huc_list'].append(huc)
        huc2_stats[huc2]['patch_counts'].append(patch_count)
        
        print(f"  {huc} (HUC2: {huc2}): {patch_count:,} patches")
    
    return dict(huc2_stats)

def analyze_huc2_distribution(huc2_stats):
    """Analyze and display HUC2 distribution statistics."""
    
    print(f"\n{'='*80}")
    print("HUC2 (WATER RESOURCE REGION) ANALYSIS")
    print(f"{'='*80}")
    
    # Calculate totals
    total_hucs = sum(stats['huc_count'] for stats in huc2_stats.values())
    total_patches = sum(stats['total_patches'] for stats in huc2_stats.values())
    
    print(f"Total HUCs: {total_hucs}")
    print(f"Total patches: {total_patches:,}")
    print(f"HUC2 regions represented: {len(huc2_stats)}")
    
    # Create summary table
    summary_data = []
    for huc2, stats in sorted(huc2_stats.items()):
        region_name = HUC2_REGIONS.get(huc2, 'Unknown')
        avg_patches = stats['total_patches'] / stats['huc_count'] if stats['huc_count'] > 0 else 0
        
        summary_data.append({
            'HUC2': huc2,
            'Region_Name': region_name,
            'HUC_Count': stats['huc_count'],
            'Total_Patches': stats['total_patches'],
            'Avg_Patches_per_HUC': avg_patches,
            'Patch_Percentage': (stats['total_patches'] / total_patches * 100) if total_patches > 0 else 0,
            'Min_Patches': min(stats['patch_counts']) if stats['patch_counts'] else 0,
            'Max_Patches': max(stats['patch_counts']) if stats['patch_counts'] else 0
        })
    
    # Sort by total patches (descending)
    summary_data.sort(key=lambda x: x['Total_Patches'], reverse=True)
    
    print(f"\n{'HUC2':<4} {'Region Name':<25} {'HUCs':<5} {'Total Patches':<15} {'Avg/HUC':<10} {'% Total':<8} {'Min':<8} {'Max':<8}")
    print("-" * 90)
    
    for data in summary_data:
        print(f"{data['HUC2']:<4} {data['Region_Name']:<25} {data['HUC_Count']:<5} "
              f"{data['Total_Patches']:<15,} {data['Avg_Patches_per_HUC']:<10.0f} "
              f"{data['Patch_Percentage']:<8.1f} {data['Min_Patches']:<8,} {data['Max_Patches']:<8,}")
    
    return summary_data

def analyze_geographic_coverage(huc2_stats):
    """Analyze geographic coverage and diversity."""
    
    print(f"\n{'='*60}")
    print("GEOGRAPHIC COVERAGE ANALYSIS")
    print(f"{'='*60}")
    
    # Define major climate/geographic zones
    climate_zones = {
        'Humid Continental': ['01', '02', '04', '05', '07', '09'],
        'Humid Subtropical': ['03', '06', '08', '11'],
        'Arid/Semi-arid': ['12', '13', '14', '15', '16'],
        'Marine West Coast': ['17'],
        'Mediterranean/Arid': ['18'],
        'Arctic/Subarctic': ['19', '22'],
        'Tropical': ['20', '21']
    }
    
    climate_coverage = defaultdict(lambda: {'huc2_count': 0, 'huc_count': 0, 'patches': 0})
    
    for huc2, stats in huc2_stats.items():
        for climate, regions in climate_zones.items():
            if huc2 in regions:
                climate_coverage[climate]['huc2_count'] += 1
                climate_coverage[climate]['huc_count'] += stats['huc_count']
                climate_coverage[climate]['patches'] += stats['total_patches']
                break
    
    print(f"{'Climate Zone':<20} {'HUC2s':<7} {'HUCs':<6} {'Patches':<12} {'% Patches':<10}")
    print("-" * 60)
    
    total_patches = sum(stats['total_patches'] for stats in huc2_stats.values())
    
    for climate, stats in sorted(climate_coverage.items()):
        patch_pct = (stats['patches'] / total_patches * 100) if total_patches > 0 else 0
        print(f"{climate:<20} {stats['huc2_count']:<7} {stats['huc_count']:<6} "
              f"{stats['patches']:<12,} {patch_pct:<10.1f}")

def main():
    parser = argparse.ArgumentParser(description='Analyze HUC2 patch distribution for test HUCs')
    parser.add_argument('--patch_dir', 
                       default='/projects/bcrm/nathanj/data/processed/test/patch_dataset',
                       help='Base directory containing patch data')
    parser.add_argument('--test_huc_file', 
                       default='test_huc_list.txt',
                       help='File containing test HUC codes')
    
    args = parser.parse_args()
    
    # Load test HUCs
    test_huc_file = args.test_huc_file
    if not os.path.isabs(test_huc_file):
        test_huc_file = os.path.join(os.path.dirname(__file__), test_huc_file)
    
    test_hucs = load_test_hucs(test_huc_file)
    print(f"Loaded {len(test_hucs)} test HUCs from {test_huc_file}")
    
    # Count patches by HUC2
    huc2_stats = count_patches_by_huc2(args.patch_dir, test_hucs)
    
    # Analyze distribution
    summary_data = analyze_huc2_distribution(huc2_stats)
    
    # Analyze geographic coverage
    analyze_geographic_coverage(huc2_stats)
    
    # Check for missing regions
    print(f"\n{'='*60}")
    print("COVERAGE GAPS ANALYSIS")
    print(f"{'='*60}")
    
    represented_huc2s = set(huc2_stats.keys())
    all_huc2s = set(HUC2_REGIONS.keys())
    missing_huc2s = all_huc2s - represented_huc2s
    
    if missing_huc2s:
        print("Missing HUC2 regions (no test HUCs):")
        for huc2 in sorted(missing_huc2s):
            region_name = HUC2_REGIONS.get(huc2, 'Unknown')
            print(f"  {huc2}: {region_name}")
    else:
        print("✅ All HUC2 regions are represented in test HUCs!")
    
    # Save detailed results
    results_file = 'huc2_patch_analysis.csv'
    results_df = pd.DataFrame(summary_data)
    results_df.to_csv(results_file, index=False)
    print(f"\n📊 Detailed results saved to: {results_file}")
    
    # Save HUC-level details
    detailed_file = 'huc2_detailed_breakdown.csv'
    detailed_data = []
    
    for huc2, stats in sorted(huc2_stats.items()):
        region_name = HUC2_REGIONS.get(huc2, 'Unknown')
        for i, huc in enumerate(stats['huc_list']):
            detailed_data.append({
                'huc2': huc2,
                'region_name': region_name,
                'huc8': huc,
                'patch_count': stats['patch_counts'][i]
            })
    
    detailed_df = pd.DataFrame(detailed_data)
    detailed_df.to_csv(detailed_file, index=False)
    print(f"📊 HUC-level breakdown saved to: {detailed_file}")
    
    return huc2_stats

if __name__ == "__main__":
    main()