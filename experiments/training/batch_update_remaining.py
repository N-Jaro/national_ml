#!/usr/bin/env python3

import re

# Remaining data modules to update
modules = [
    ('data_module_dem_sar.py', 'MultimodalPatchDataset_DEM_SAR'),
    ('data_module_dem_thermal.py', 'MultimodalPatchDataset_DEM_Thermal'),  
    ('data_module_alphaearth_only.py', 'MultimodalPatchDataset_AlphaEarth')
]

def update_module(filepath, dataset_class):
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Update constructor signature
    old_sig = r'(class \w+\(pl\.LightningDataModule\):\s*def __init__\(\s*self,\s*base_path,\s*)huc_list,'
    new_sig = r'\1huc_list=None,              # For backward compatibility\n        train_hucs=None,            # New: explicit train HUCs\n        val_hucs=None,              # New: explicit val HUCs,'
    content = re.sub(old_sig, new_sig, content, flags=re.DOTALL)
    
    # Update initialization part
    old_init = r'(\s+super\(\).__init__\(\)\s+self\.base_path = base_path\s+self\.huc_list = huc_list\s+self\.batch_size = batch_size\s+self\.num_workers = num_workers\s+self\.val_split = val_split)'
    new_init = '''        super().__init__()
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
            raise ValueError("Must provide either (train_hucs, val_hucs) or huc_list")'''
    content = re.sub(old_init, new_init, content, flags=re.DOTALL)
    
    with open(filepath, 'w') as f:
        f.write(content)
    print(f"Updated {filepath}")

for module_file, dataset_class in modules:
    update_module(module_file, dataset_class)
