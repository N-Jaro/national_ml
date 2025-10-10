#!/usr/bin/env python3
"""
Script to update all remaining data modules and Lightning training scripts 
to support HUC-level train/val splitting.
"""

import os
import re

# Data modules to update with their corresponding dataset classes
DATA_MODULES = {
    'data_module_dem_only.py': 'MultimodalPatchDataset_DEM',
    'data_module_dem_sar.py': 'MultimodalPatchDataset_DEM_SAR', 
    'data_module_dem_thermal.py': 'MultimodalPatchDataset_DEM_Thermal',
    'data_module_alphaearth_only.py': 'MultimodalPatchDataset_AlphaEarth'
}

# Lightning scripts to update
LIGHTNING_SCRIPTS = {
    'run_lightning_train_dem_only.py': 'PatchDataModule_DEM',
    'run_lightning_train_dem_sar.py': 'PatchDataModule_DEM_SAR',
    'run_lightning_train_dem_thermal.py': 'PatchDataModule_DEM_Thermal', 
    'run_lightning_train_alphaearth_only.py': 'PatchDataModule_AlphaEarth'
}

def update_data_module(filepath, dataset_class):
    """Update a data module to support both old and new HUC splitting."""
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Find the class constructor
    class_pattern = r'(class \w+\(pl\.LightningDataModule\):\s*\n\s*def __init__\(\s*self,\s*base_path,\s*)(huc_list,)'
    
    # Update constructor signature
    new_signature = r'\1huc_list=None,              # For backward compatibility\n        train_hucs=None,            # New: explicit train HUCs\n        val_hucs=None,              # New: explicit val HUCs'
    content = re.sub(class_pattern, new_signature, content)
    
    # Update constructor body
    old_init = r'(\s+super\(\).__init__\(\)\s+self\.base_path = base_path\s+self\.huc_list = huc_list)'
    new_init = f'''        super().__init__()
        self.base_path = base_path
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.estimate_weights_from = estimate_weights_from
        
        # Handle both old and new HUC specification methods
        if train_hucs is not None and val_hucs is not None:
            # New explicit train/val HUC split
            self.train_hucs = train_hucs
            self.val_hucs = val_hucs
            self.use_explicit_split = True
        elif huc_list is not None:
            # Old method: split HUCs randomly
            self.huc_list = huc_list  
            self.val_split = val_split
            self.use_explicit_split = False
        else:
            raise ValueError("Must provide either (train_hucs, val_hucs) or huc_list")

        # Store other parameters'''
    
    # This is getting complex. Let me do it manually for the remaining ones.
    print(f"Would update {filepath} with {dataset_class}")

if __name__ == "__main__":
    base_dir = "/u/nathanj/national_ml/experiments/training"
    
    for module_file, dataset_class in DATA_MODULES.items():
        filepath = os.path.join(base_dir, module_file)
        if os.path.exists(filepath):
            print(f"Updating {module_file}...")
            update_data_module(filepath, dataset_class)
        else:
            print(f"Warning: {filepath} not found")