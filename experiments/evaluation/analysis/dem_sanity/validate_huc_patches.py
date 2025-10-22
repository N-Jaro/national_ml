#!/usr/bin/env python3
"""
Validate Patch Data for DEM Sanity Analysis
Checks data availability, quality, and generates detailed statistics for each HUC
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import glob

def validate_huc_patches(huc_code: str, data_path: str) -> Dict[str, Any]:
    """Validate patch data for a single HUC"""
    
    huc_dir = os.path.join(data_path, huc_code)
    
    if not os.path.isdir(huc_dir):
        return {
            'huc_code': huc_code,
            'status': 'MISSING_DIRECTORY',
            'patch_count': 0,
            'has_normalization_stats': False,
            'error': f'Directory not found: {huc_dir}'
        }
    
    # Check for normalization stats
    stats_file = os.path.join(huc_dir, 'normalization_stats.json')
    has_stats = os.path.exists(stats_file)
    
    # Get patch files
    patch_files = glob.glob(os.path.join(huc_dir, 'patch_*.npz'))
    patch_count = len(patch_files)
    
    if patch_count == 0:
        return {
            'huc_code': huc_code,
            'status': 'NO_PATCHES',
            'patch_count': 0,
            'has_normalization_stats': has_stats,
            'error': 'No patch files found'
        }
    
    # Validate a sample of patches
    sample_patches = patch_files[:min(5, len(patch_files))]  # Check first 5 patches
    valid_patches = 0
    invalid_patches = []
    
    required_keys = ['dem', 'alphaearth', 'hydro_mask', 'flow_dir']
    
    dem_stats = {'min': [], 'max': [], 'mean': [], 'std': []}
    alphaearth_stats = {'channels': [], 'min': [], 'max': [], 'mean': []}
    water_coverage = []
    
    for patch_file in sample_patches:
        try:
            with np.load(patch_file) as data:
                # Check required keys
                missing_keys = [key for key in required_keys if key not in data]
                if missing_keys:
                    invalid_patches.append({
                        'file': os.path.basename(patch_file),
                        'error': f'Missing keys: {missing_keys}'
                    })
                    continue
                
                # Check data shapes and types
                dem = data['dem']
                alphaearth = data['alphaearth']
                hydro_mask = data['hydro_mask']
                flow_dir = data['flow_dir']
                
                # Validate shapes
                if not (dem.ndim == 2 and hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                    invalid_patches.append({
                        'file': os.path.basename(patch_file),
                        'error': f'Invalid dimensions: dem={dem.shape}, hydro={hydro_mask.shape}, flow={flow_dir.shape}'
                    })
                    continue
                
                if not (dem.shape == hydro_mask.shape == flow_dir.shape):
                    invalid_patches.append({
                        'file': os.path.basename(patch_file),
                        'error': f'Shape mismatch: dem={dem.shape}, hydro={hydro_mask.shape}, flow={flow_dir.shape}'
                    })
                    continue
                
                # AlphaEarth validation
                if alphaearth.ndim == 2:
                    if alphaearth.shape != dem.shape:
                        invalid_patches.append({
                            'file': os.path.basename(patch_file),
                            'error': f'AlphaEarth shape mismatch: {alphaearth.shape} vs dem {dem.shape}'
                        })
                        continue
                    ae_channels = 1
                elif alphaearth.ndim == 3:
                    if alphaearth.shape[:2] != dem.shape:
                        invalid_patches.append({
                            'file': os.path.basename(patch_file),
                            'error': f'AlphaEarth spatial shape mismatch: {alphaearth.shape[:2]} vs dem {dem.shape}'
                        })
                        continue
                    ae_channels = alphaearth.shape[2]
                else:
                    invalid_patches.append({
                        'file': os.path.basename(patch_file),
                        'error': f'Invalid AlphaEarth dimensions: {alphaearth.shape}'
                    })
                    continue
                
                # Collect statistics
                dem_stats['min'].append(float(np.min(dem)))
                dem_stats['max'].append(float(np.max(dem)))
                dem_stats['mean'].append(float(np.mean(dem)))
                dem_stats['std'].append(float(np.std(dem)))
                
                alphaearth_stats['channels'].append(ae_channels)
                alphaearth_stats['min'].append(float(np.min(alphaearth)))
                alphaearth_stats['max'].append(float(np.max(alphaearth)))
                alphaearth_stats['mean'].append(float(np.mean(alphaearth)))
                
                # Water coverage
                water_pixels = np.sum(hydro_mask == 1)
                total_pixels = hydro_mask.size
                water_coverage.append(water_pixels / total_pixels)
                
                valid_patches += 1
                
        except Exception as e:
            invalid_patches.append({
                'file': os.path.basename(patch_file),
                'error': f'Exception loading patch: {str(e)}'
            })
    
    # Load normalization stats if available
    normalization_stats = None
    if has_stats:
        try:
            with open(stats_file, 'r') as f:
                normalization_stats = json.load(f)
        except Exception as e:
            normalization_stats = {'error': f'Failed to load: {str(e)}'}
    
    # Determine status
    if valid_patches == 0:
        status = 'ALL_INVALID'
    elif len(invalid_patches) > 0:
        status = 'PARTIALLY_VALID'
    else:
        status = 'VALID'
    
    return {
        'huc_code': huc_code,
        'status': status,
        'patch_count': patch_count,
        'valid_patches_sampled': valid_patches,
        'invalid_patches_sampled': len(invalid_patches),
        'has_normalization_stats': has_stats,
        'normalization_stats': normalization_stats,
        'invalid_patch_details': invalid_patches,
        
        # Data statistics from sampled patches
        'dem_elevation_range': f"{np.mean(dem_stats['min']):.1f} to {np.mean(dem_stats['max']):.1f}m" if dem_stats['min'] else "N/A",
        'dem_mean_elevation': f"{np.mean(dem_stats['mean']):.1f}m" if dem_stats['mean'] else "N/A", 
        'dem_elevation_std': f"{np.mean(dem_stats['std']):.1f}m" if dem_stats['std'] else "N/A",
        
        'alphaearth_channels': alphaearth_stats['channels'][0] if alphaearth_stats['channels'] else "N/A",
        'alphaearth_value_range': f"{np.mean(alphaearth_stats['min']):.3f} to {np.mean(alphaearth_stats['max']):.3f}" if alphaearth_stats['min'] else "N/A",
        
        'water_coverage_percent': f"{np.mean(water_coverage)*100:.1f}%" if water_coverage else "N/A",
        'water_coverage_range': f"{np.min(water_coverage)*100:.1f}% to {np.max(water_coverage)*100:.1f}%" if len(water_coverage) > 1 else "N/A"
    }

def main():
    print("🔍 Validating Patch Data for DEM Sanity Analysis")
    print("=" * 80)
    
    # Define paths
    representative_huc_file = "/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/test_huc_representative.txt"
    minimal_huc_file = "/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/test_huc_minimal.txt"
    data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    
    # Load HUC lists
    huc_lists = {}
    
    try:
        with open(representative_huc_file, 'r') as f:
            huc_lists['representative'] = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except FileNotFoundError:
        print(f"❌ Representative HUC file not found: {representative_huc_file}")
        return 1
    
    try:
        with open(minimal_huc_file, 'r') as f:
            huc_lists['minimal'] = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except FileNotFoundError:
        print(f"❌ Minimal HUC file not found: {minimal_huc_file}")
        return 1
    
    all_results = []
    
    # Validate each HUC set
    for set_name, huc_codes in huc_lists.items():
        print(f"\n🗺️ Validating {set_name.upper()} HUC set ({len(huc_codes)} HUCs):")
        print("-" * 60)
        
        valid_count = 0
        
        for huc_code in huc_codes:
            print(f"Checking {huc_code}...", end=" ")
            
            result = validate_huc_patches(huc_code, data_path)
            result['huc_set'] = set_name
            all_results.append(result)
            
            if result['status'] == 'VALID':
                print(f"✅ VALID ({result['patch_count']} patches, {result['water_coverage_percent']} water)")
                valid_count += 1
            elif result['status'] == 'PARTIALLY_VALID':
                print(f"⚠️ PARTIALLY VALID ({result['valid_patches_sampled']}/{result['valid_patches_sampled'] + result['invalid_patches_sampled']} patches OK)")
                valid_count += 1
            else:
                print(f"❌ {result['status']} - {result.get('error', 'Unknown error')}")
        
        print(f"\n📊 {set_name.upper()} Summary: {valid_count}/{len(huc_codes)} HUCs are usable")
    
    # Save detailed results
    output_dir = Path("/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create DataFrame and save CSV
    df = pd.DataFrame(all_results)
    csv_path = output_dir / "huc_validation_results.csv"
    df.to_csv(csv_path, index=False)
    
    # Create detailed report
    report_path = output_dir / "huc_validation_report.txt"
    with open(report_path, 'w') as f:
        f.write("DEM Sanity Analysis - HUC Validation Report\n")
        f.write("=" * 80 + "\n\n")
        
        for result in all_results:
            f.write(f"HUC: {result['huc_code']} ({result['huc_set']})\n")
            f.write(f"Status: {result['status']}\n")
            f.write(f"Patches: {result['patch_count']}\n")
            f.write(f"DEM Elevation: {result['dem_elevation_range']}\n")
            f.write(f"AlphaEarth Channels: {result['alphaearth_channels']}\n")
            f.write(f"Water Coverage: {result['water_coverage_percent']}\n")
            
            if result['invalid_patch_details']:
                f.write("Issues found:\n")
                for issue in result['invalid_patch_details']:
                    f.write(f"  - {issue['file']}: {issue['error']}\n")
            
            f.write("\n" + "-" * 40 + "\n\n")
    
    # Print final summary
    print("\n" + "=" * 80)
    print("📋 VALIDATION SUMMARY")
    print("=" * 80)
    
    by_set = df.groupby('huc_set')
    for set_name, group in by_set:
        valid_hucs = group[group['status'].isin(['VALID', 'PARTIALLY_VALID'])]
        total_patches = group['patch_count'].sum()
        print(f"{set_name.upper()}: {len(valid_hucs)}/{len(group)} HUCs valid, {total_patches:,} total patches")
    
    print(f"\n📄 Detailed results saved to:")
    print(f"   CSV: {csv_path}")
    print(f"   Report: {report_path}")
    
    # Check if we can proceed with analysis
    representative_valid = df[(df['huc_set'] == 'representative') & (df['status'].isin(['VALID', 'PARTIALLY_VALID']))]
    minimal_valid = df[(df['huc_set'] == 'minimal') & (df['status'].isin(['VALID', 'PARTIALLY_VALID']))]
    
    print(f"\n🚀 ANALYSIS READINESS:")
    if len(representative_valid) >= 8:
        print("✅ Representative analysis ready (sufficient valid HUCs)")
    else:
        print(f"⚠️ Representative analysis may be limited ({len(representative_valid)}/10 HUCs valid)")
    
    if len(minimal_valid) >= 2:
        print("✅ Minimal analysis ready (sufficient valid HUCs)")
    else:
        print(f"❌ Minimal analysis not viable ({len(minimal_valid)}/3 HUCs valid)")
    
    print("=" * 80)
    
    return 0

if __name__ == "__main__":
    exit(main())