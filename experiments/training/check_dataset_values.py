#!/usr/bin/env python3
"""
Quick script to check the actual values in the DEM+Thermal dataset.
"""

import os
import sys
import numpy as np
from collections import Counter

# Add the experiments directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data.patchDataLoader_dem_thermal import MultimodalPatchDataset_DEM_Thermal

def check_dataset_values():
    base_path = '/u/nathanj/national_ml/data/processed/patch_dataset'
    huc_codes = ['03030005']
    
    dataset = MultimodalPatchDataset_DEM_Thermal(
        base_path=base_path,
        huc_codes=huc_codes
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) == 0:
        print("No data found!")
        return
    
    # Sample a few examples to check value ranges
    water_values = []
    d8_values = []
    
    print("Sampling first 10 examples...")
    for i in range(min(10, len(dataset))):
        try:
            sample = dataset[i]
            hydro_mask = sample['hydro_mask'].numpy()
            flow_dir = sample['flow_dir'].numpy()
            
            water_unique = np.unique(hydro_mask)
            d8_unique = np.unique(flow_dir)
            
            water_values.extend(water_unique.tolist())
            d8_values.extend(d8_unique.tolist())
            
            print(f"Sample {i}:")
            print(f"  Hydro mask values: {water_unique}")
            print(f"  Flow dir values: {d8_unique}")
            print(f"  Flow dir min/max: {flow_dir.min()}, {flow_dir.max()}")
            
        except Exception as e:
            print(f"Error processing sample {i}: {e}")
    
    print("\nOverall value ranges:")
    print(f"Water mask unique values: {sorted(set(water_values))}")
    print(f"D8 flow direction unique values: {sorted(set(d8_values))}")
    print(f"D8 min: {min(d8_values) if d8_values else 'N/A'}")
    print(f"D8 max: {max(d8_values) if d8_values else 'N/A'}")
    
    # Count frequencies
    d8_counter = Counter(d8_values)
    print(f"D8 value frequencies: {dict(d8_counter.most_common())}")

if __name__ == '__main__':
    check_dataset_values()