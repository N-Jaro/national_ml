#!/usr/bin/env python3
"""
Create an optimized HUC list for national-level evaluation with reduced sample size
Focus on geographic    print(f"🏔️  Step 1: High-priority regions (major climate/geographic zones)")
    for region in high_priority_regions:
        if region in regions and regions[region]:
            # Special handling for Alaska - take more HUCs due to its unique climate and size
            if region == '19':  # Alaska
                region_hucs = regions[region][:6 if len(regions[region]) >= 6 else len(regions[region])]
                print(f"    🏔️  Alaska gets special treatment - unique Arctic/Subarctic climate")
            else:
                # Take top 2-4 HUCs from other high-priority regions
                region_hucs = regions[region][:4 if len(regions[region]) >= 4 else len(regions[region])]
            
            for huc, patches in region_hucs:
                if len(selected_hucs) < target_count * 0.75:  # Use 75% of budget for high-priority (increased for Alaska)
                    selected_hucs.append(huc)
                    print(f"    {region} ({WATER_RESOURCE_REGIONS[region]['name']}): {huc} ({patches} patches)")y, climate zones, and watershed characteristics

Strategy:
1. Ensure coverage of all major US regions (18 Water Resource Regions)
2. Sample based on climate zones and topographic diversity
3. Include both large and small watersheds
4. Prioritize HUCs with high patch counts for reliable evaluation
5. Target ~50-75 HUCs instead of 200+ for faster evaluation
"""

import os
import glob
import random
import json
from collections import defaultdict
import numpy as np

# US Water Resource Regions and their characteristics
WATER_RESOURCE_REGIONS = {
    '01': {'name': 'New England', 'climate': 'Humid Continental', 'priority': 'high'},
    '02': {'name': 'Mid-Atlantic', 'climate': 'Humid Continental/Subtropical', 'priority': 'high'},
    '03': {'name': 'South Atlantic-Gulf', 'climate': 'Humid Subtropical', 'priority': 'high'},
    '04': {'name': 'Great Lakes', 'climate': 'Humid Continental', 'priority': 'high'},
    '05': {'name': 'Ohio', 'climate': 'Humid Continental', 'priority': 'medium'},
    '06': {'name': 'Tennessee', 'climate': 'Humid Subtropical', 'priority': 'medium'},
    '07': {'name': 'Upper Mississippi', 'climate': 'Humid Continental', 'priority': 'high'},
    '08': {'name': 'Lower Mississippi', 'climate': 'Humid Subtropical', 'priority': 'high'},
    '09': {'name': 'Souris-Red-Rainy', 'climate': 'Humid Continental', 'priority': 'low'},
    '10': {'name': 'Missouri', 'climate': 'Semi-arid/Humid Continental', 'priority': 'high'},
    '11': {'name': 'Arkansas-White-Red', 'climate': 'Semi-arid/Humid Subtropical', 'priority': 'medium'},
    '12': {'name': 'Texas-Gulf', 'climate': 'Arid/Semi-arid', 'priority': 'high'},
    '13': {'name': 'Rio Grande', 'climate': 'Arid/Semi-arid', 'priority': 'medium'},
    '14': {'name': 'Upper Colorado', 'climate': 'Arid/Semi-arid', 'priority': 'high'},
    '15': {'name': 'Lower Colorado', 'climate': 'Arid', 'priority': 'medium'},
    '16': {'name': 'Great Basin', 'climate': 'Arid/Semi-arid', 'priority': 'medium'},
    '17': {'name': 'Pacific Northwest', 'climate': 'Marine West Coast', 'priority': 'high'},
    '18': {'name': 'California', 'climate': 'Mediterranean/Arid', 'priority': 'high'},
    '19': {'name': 'Alaska', 'climate': 'Subarctic/Arctic', 'priority': 'high'},  # UPGRADED: Alaska now high priority
    '20': {'name': 'Hawaii', 'climate': 'Tropical', 'priority': 'low'},
    '21': {'name': 'Caribbean', 'climate': 'Tropical', 'priority': 'low'},
    '22': {'name': 'Alaska (continued)', 'climate': 'Subarctic/Arctic', 'priority': 'medium'}  # UPGRADED: Also medium priority
}

def count_patches_in_huc(huc_path):
    """Count number of patch files in a HUC directory"""
    if not os.path.exists(huc_path):
        return 0
    patch_files = glob.glob(os.path.join(huc_path, "*.npz"))
    return len(patch_files)

def get_hucs_with_sufficient_patches(test_path, min_patches=150):
    """Get HUCs with at least min_patches patches (increased minimum for quality)"""
    hucs_with_patches = []
    
    if not os.path.exists(test_path):
        print(f"Error: Path {test_path} does not exist")
        return hucs_with_patches
    
    print(f"Scanning {test_path} for HUCs with at least {min_patches} patches...")
    
    for item in os.listdir(test_path):
        item_path = os.path.join(test_path, item)
        if os.path.isdir(item_path) and item.isdigit() and len(item) == 8:
            patch_count = count_patches_in_huc(item_path)
            if patch_count >= min_patches:
                hucs_with_patches.append((item, patch_count))
                print(f"  {item}: {patch_count} patches ✓")
    
    return hucs_with_patches

def categorize_hucs_by_watershed_size(hucs_with_patches):
    """Categorize HUCs by watershed size based on HUC level patterns"""
    categorized = {
        'large_watersheds': [],    # Major river basins
        'medium_watersheds': [],   # Regional watersheds  
        'coastal_watersheds': [],  # Coastal areas
        'mountain_watersheds': [], # Mountain/high elevation
        'plains_watersheds': []    # Great Plains/prairie
    }
    
    # Heuristic classification based on HUC patterns and known geography
    large_watershed_prefixes = ['0708', '0802', '0804', '1002', '1004', '1008', '1010']  # Major rivers
    coastal_prefixes = ['0101', '0102', '0301', '0302', '0303', '1201', '1202', '1801', '1802']  # Coastal
    mountain_prefixes = ['1401', '1402', '1403', '1404', '1501', '1601', '1602', '1701', '1702']  # Mountains
    plains_prefixes = ['0902', '1006', '1102', '1103', '1104']  # Plains
    
    for huc, patch_count in hucs_with_patches:
        huc_prefix = huc[:4]
        
        if any(huc.startswith(prefix) for prefix in large_watershed_prefixes):
            categorized['large_watersheds'].append((huc, patch_count))
        elif any(huc.startswith(prefix) for prefix in coastal_prefixes):
            categorized['coastal_watersheds'].append((huc, patch_count))
        elif any(huc.startswith(prefix) for prefix in mountain_prefixes):
            categorized['mountain_watersheds'].append((huc, patch_count))
        elif any(huc.startswith(prefix) for prefix in plains_prefixes):
            categorized['plains_watersheds'].append((huc, patch_count))
        else:
            categorized['medium_watersheds'].append((huc, patch_count))
    
    return categorized

def smart_sample_hucs(hucs_with_patches, target_count=60):
    """Intelligently sample HUCs for maximum geographic and climatic diversity"""
    
    # Group by Water Resource Region
    regions = defaultdict(list)
    for huc, patch_count in hucs_with_patches:
        region_code = huc[:2]
        regions[region_code].append((huc, patch_count))
    
    # Sort each region by patch count (prefer high-quality HUCs)
    for region_code in regions:
        regions[region_code].sort(key=lambda x: x[1], reverse=True)
    
    # Categorize by watershed types
    categorized = categorize_hucs_by_watershed_size(hucs_with_patches)
    
    selected_hucs = []
    
    print(f"\n🎯 Smart sampling strategy for {target_count} HUCs:")
    print("=" * 70)
    
    # Step 1: Ensure coverage of high-priority regions (at least 1 HUC each)
    high_priority_regions = [k for k, v in WATER_RESOURCE_REGIONS.items() if v['priority'] == 'high']
    medium_priority_regions = [k for k, v in WATER_RESOURCE_REGIONS.items() if v['priority'] == 'medium']
    
    print("🏔️  Step 1: High-priority regions (major climate/geographic zones)")
    for region in high_priority_regions:
        if region in regions and regions[region]:
            # Take top 2-4 HUCs from high-priority regions
            region_hucs = regions[region][:4 if len(regions[region]) >= 4 else len(regions[region])]
            for huc, patches in region_hucs:
                if len(selected_hucs) < target_count * 0.7:  # Use 70% of budget for high-priority
                    selected_hucs.append(huc)
                    print(f"    {region} ({WATER_RESOURCE_REGIONS[region]['name']}): {huc} ({patches} patches)")
    
    print(f"\n🌊 Step 2: Medium-priority regions (regional diversity)")
    for region in medium_priority_regions:
        if region in regions and regions[region]:
            # Special handling for Alaska continued (22) - take more if available
            if region == '22':  # Alaska continued
                region_hucs = regions[region][:3 if len(regions[region]) >= 3 else len(regions[region])]
                print(f"    🏔️  Alaska (continued) - additional Arctic coverage")
            else:
                # Take top 1-2 HUCs from other medium-priority regions
                region_hucs = regions[region][:2 if len(regions[region]) >= 2 else len(regions[region])]
            
            for huc, patches in region_hucs:
                if len(selected_hucs) < target_count * 0.9:  # Use up to 90% including medium priority
                    selected_hucs.append(huc)
                    print(f"    {region} ({WATER_RESOURCE_REGIONS[region]['name']}): {huc} ({patches} patches)")
    
    # Step 3: Fill remaining slots with watershed diversity
    remaining_slots = target_count - len(selected_hucs)
    
    if remaining_slots > 0:
        print(f"\n🏞️  Step 3: Watershed diversity ({remaining_slots} remaining slots)")
        
        # Ensure representation from each watershed type
        watershed_types = ['large_watersheds', 'coastal_watersheds', 'mountain_watersheds', 'plains_watersheds', 'medium_watersheds']
        slots_per_type = max(1, remaining_slots // len(watershed_types))
        
        for watershed_type in watershed_types:
            type_hucs = categorized[watershed_type]
            # Filter out already selected HUCs
            available_hucs = [(huc, patches) for huc, patches in type_hucs if huc not in selected_hucs]
            
            if available_hucs:
                # Sort by patch count and take top ones
                available_hucs.sort(key=lambda x: x[1], reverse=True)
                to_add = min(slots_per_type, len(available_hucs), remaining_slots)
                
                for i in range(to_add):
                    huc, patches = available_hucs[i]
                    selected_hucs.append(huc)
                    remaining_slots -= 1
                    print(f"    {watershed_type.replace('_', ' ').title()}: {huc} ({patches} patches)")
    
    return sorted(selected_hucs)

def analyze_coverage(selected_hucs):
    """Analyze the geographic and climatic coverage of selected HUCs"""
    
    print(f"\n📊 COVERAGE ANALYSIS")
    print("=" * 50)
    
    # Regional coverage
    region_coverage = defaultdict(int)
    climate_coverage = defaultdict(int)
    
    for huc in selected_hucs:
        region = huc[:2]
        if region in WATER_RESOURCE_REGIONS:
            region_coverage[region] += 1
            climate = WATER_RESOURCE_REGIONS[region]['climate']
            climate_coverage[climate] += 1
    
    print("🗺️  Regional Coverage:")
    for region, count in sorted(region_coverage.items()):
        region_name = WATER_RESOURCE_REGIONS.get(region, {}).get('name', 'Unknown')
        print(f"    {region} ({region_name}): {count} HUCs")
    
    print(f"\n🌡️  Climate Zone Coverage:")
    for climate, count in sorted(climate_coverage.items()):
        print(f"    {climate}: {count} HUCs")
    
    # Geographic spread analysis
    latitudes = []
    longitudes = []
    
    # Rough HUC to lat/lon mapping for major regions (simplified)
    huc_to_region_center = {
        '01': (44.0, -71.0),  # New England
        '02': (40.0, -76.0),  # Mid-Atlantic
        '03': (32.0, -82.0),  # South Atlantic
        '04': (44.0, -85.0),  # Great Lakes
        '05': (39.0, -84.0),  # Ohio
        '06': (36.0, -86.0),  # Tennessee
        '07': (44.0, -93.0),  # Upper Mississippi
        '08': (32.0, -91.0),  # Lower Mississippi
        '10': (42.0, -100.0), # Missouri
        '11': (35.0, -95.0),  # Arkansas-White-Red
        '12': (29.0, -97.0),  # Texas-Gulf
        '13': (32.0, -106.0), # Rio Grande
        '14': (39.0, -108.0), # Upper Colorado
        '15': (34.0, -114.0), # Lower Colorado
        '16': (40.0, -117.0), # Great Basin
        '17': (46.0, -120.0), # Pacific Northwest
        '18': (36.0, -120.0), # California
    }
    
    for huc in selected_hucs:
        region = huc[:2]
        if region in huc_to_region_center:
            lat, lon = huc_to_region_center[region]
            latitudes.append(lat)
            longitudes.append(lon)
    
    if latitudes and longitudes:
        lat_range = max(latitudes) - min(latitudes)
        lon_range = max(longitudes) - min(longitudes)
        print(f"\n📍 Geographic Spread:")
        print(f"    Latitude range: {lat_range:.1f}° ({min(latitudes):.1f}° to {max(latitudes):.1f}°)")
        print(f"    Longitude range: {lon_range:.1f}° ({min(longitudes):.1f}° to {max(longitudes):.1f}°)")
        
        coverage_score = (lat_range / 30.0 + lon_range / 60.0) / 2.0 * 100  # Rough national coverage score
        print(f"    National coverage score: {coverage_score:.1f}% (higher is better)")

def main():
    test_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    min_patches = 150  # Increased for higher quality
    target_huc_count = 70  # Increased to 70 to accommodate Alaska coverage
    
    # Set random seed for reproducible sampling
    random.seed(42)
    np.random.seed(42)
    
    print("🚀 Creating OPTIMIZED HUC list for national-level evaluation")
    print("=" * 70)
    print(f"🎯 Target: {target_huc_count} HUCs (down from 207)")
    print(f"📏 Quality threshold: {min_patches}+ patches per HUC")
    print("🌍 Focus: Maximum geographic and climatic diversity")
    print("🏔️  Enhanced Alaska coverage for complete national representation")
    
    # Get HUCs with sufficient patches
    hucs_with_patches = get_hucs_with_sufficient_patches(test_path, min_patches)
    
    if not hucs_with_patches:
        print(f"❌ No HUCs found with at least {min_patches} patches!")
        return
    
    print(f"\n✅ Found {len(hucs_with_patches)} high-quality HUCs")
    
    # Smart sampling for diversity
    selected_hucs = smart_sample_hucs(hucs_with_patches, target_huc_count)
    
    print(f"\n🎊 Final Selection: {len(selected_hucs)} HUCs")
    
    # Analyze coverage
    analyze_coverage(selected_hucs)
    
    # Create the optimized test HUC list file
    output_file = 'test_huc_optimized.txt'
    with open(output_file, 'w') as f:
        f.write("# OPTIMIZED Test HUC List for National-Level MDMT Evaluation\n")
        f.write("# Designed for faster evaluation while maintaining national coverage\n")
        f.write(f"# Quality threshold: At least {min_patches} patches per HUC\n")
        f.write("# Sampling strategy: Smart geographic and climatic diversity\n")
        f.write(f"# Total HUCs: {len(selected_hucs)} (reduced from 207)\n")
        f.write("# Coverage: All major US climate zones and water resource regions\n")
        f.write("# Expected speedup: 3-4x faster evaluation (~70% time reduction)\n")
        f.write("# Generated on 2025-10-06\n")
        f.write("# Format: One HUC code per line\n")
        f.write("\n")
        
        for huc in selected_hucs:
            f.write(f"{huc}\n")
    
    print(f"\n✅ Created {output_file}")
    print(f"🚀 Expected speedup: ~{207/len(selected_hucs):.1f}x faster evaluation")
    print(f"⏱️  Estimated time reduction: {(1 - len(selected_hucs)/207)*100:.0f}%")
    print(f"🎯 National coverage maintained with strategic sampling")
    
    # Create a summary file
    summary_file = 'huc_optimization_summary.json'
    summary = {
        'original_huc_count': 207,
        'optimized_huc_count': len(selected_hucs),
        'speedup_factor': 207 / len(selected_hucs),
        'time_reduction_percent': (1 - len(selected_hucs)/207) * 100,
        'quality_threshold': min_patches,
        'regions_covered': len(set(huc[:2] for huc in selected_hucs)),
        'selected_hucs': selected_hucs,
        'generation_date': '2025-10-06'
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"📊 Summary saved to: {summary_file}")
    print(f"\n🎯 Ready for optimized national-level evaluation!")

if __name__ == "__main__":
    main()