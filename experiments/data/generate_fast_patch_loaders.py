#!/usr/bin/env python3
"""
Generator script to create fast patch loaders for all MDMT variants.
"""

import os
from pathlib import Path

# Configuration for each variant
VARIANT_CONFIGS = {
    'dem_sar': {
        'description': 'DEM + SAR model',
        'required_keys': ['dem', 'sar', 'hydro_mask', 'flow_dir'],
        'modality_keys': ['dem', 'sar'],
        'stats_mapping': {
            'dem': {
                'mean_key': 'elevation_mean',
                'std_key': 'elevation_stdDev'
            },
            'sar': {
                'mean_key': 'VV_mean', 
                'std_key': 'VV_stdDev'
            }
        },
        'return_format': """
            return {
                'dem': torch.from_numpy(dem).to(self.dtype_inputs),
                'sar': torch.from_numpy(sar).to(self.dtype_inputs),
                'hydro_mask': torch.from_numpy(hydro_mask).to(self.dtype_inputs),
                'flow_dir': torch.from_numpy(flow_dir).long(),
                'path': file_path
            }"""
    },
    'dem_optical': {
        'description': 'DEM + Optical model',
        'required_keys': ['dem', 'optical', 'hydro_mask', 'flow_dir'],
        'modality_keys': ['dem', 'optical'],
        'stats_mapping': {
            'dem': {
                'mean_key': 'elevation_mean',
                'std_key': 'elevation_stdDev'
            },
            'optical': {
                'mean_key': 'Red_mean',  # Assuming Red band for optical
                'std_key': 'Red_stdDev'
            }
        },
        'return_format': """
            return {
                'dem': torch.from_numpy(dem).to(self.dtype_inputs),
                'optical': torch.from_numpy(optical).to(self.dtype_inputs),
                'hydro_mask': torch.from_numpy(hydro_mask).to(self.dtype_inputs),
                'flow_dir': torch.from_numpy(flow_dir).long(),
                'path': file_path
            }"""
    },
    'dem_alphaearth': {
        'description': 'DEM + AlphaEarth model',
        'required_keys': ['dem', 'alphaearth', 'hydro_mask', 'flow_dir'],
        'modality_keys': ['dem', 'alphaearth'],
        'stats_mapping': {
            'dem': {
                'mean_key': 'elevation_mean',
                'std_key': 'elevation_stdDev'
            }
            # AlphaEarth doesn't need normalization - pre-processed embeddings
        },
        'special_processing': {
            'alphaearth': """
            # Handle AlphaEarth dimensions
            if alphaearth.ndim == 2:
                # (H,W) -> add channel dimension for single-channel
                alphaearth = alphaearth[None, :, :]  # (1,H,W)
            elif alphaearth.ndim == 3:
                # (H,W,C) -> (C,H,W)
                alphaearth = alphaearth.transpose(2, 0, 1)
            
            # Ensure we have the expected number of channels (64 for AlphaEarth)
            if alphaearth.shape[0] != self.alphaearth_channels:
                self._skipped_files.add(file_path)
                return None"""
        },
        'init_params': 'alphaearth_channels: int = 64,',
        'init_assignments': 'self.alphaearth_channels = alphaearth_channels',
        'return_format': """
            return {
                'dem': torch.from_numpy(dem).to(self.dtype_inputs),
                'alphaearth': torch.from_numpy(alphaearth).to(self.dtype_inputs),
                'hydro_mask': torch.from_numpy(hydro_mask).to(self.dtype_inputs),
                'flow_dir': torch.from_numpy(flow_dir).long(),
                'path': file_path
            }"""
    },
    'ls6b': {
        'description': 'Landsat 6B model',
        'required_keys': ['landsat', 'hydro_mask', 'flow_dir'],
        'modality_keys': ['landsat'],
        'stats_mapping': {
            'landsat': {
                'mean_key': 'B1_mean',  # Assuming B1 band for landsat
                'std_key': 'B1_stdDev'
            }
        },
        'special_processing': {
            'landsat': """
            # Handle Landsat dimensions - can be 3D with multiple bands
            if landsat.ndim == 2:
                # (H,W) -> add channel dimension
                landsat = landsat[None, :, :]  # (1,H,W)
            elif landsat.ndim == 3:
                # (H,W,C) -> (C,H,W)
                landsat = landsat.transpose(2, 0, 1)"""
        },
        'return_format': """
            return {
                'landsat': torch.from_numpy(landsat).to(self.dtype_inputs),
                'hydro_mask': torch.from_numpy(hydro_mask).to(self.dtype_inputs),
                'flow_dir': torch.from_numpy(flow_dir).long(),
                'path': file_path
            }"""
    }
}

def generate_fast_patch_loader(variant_name: str, config: dict) -> str:
    """Generate a fast patch loader for a specific variant."""
    
    # Generate the stats parsing function
    modality_keys = config['modality_keys']
    stats_dict_init = ", ".join([f'"{key}": None' for key in modality_keys])
    
    stats_parsing = []
    for key in modality_keys:
        if key in config['stats_mapping']:
            mapping = config['stats_mapping'][key]
            stats_parsing.append(f'''    if "{key}" in stats_json:
        {key[0]} = stats_json["{key}"]
        out["{key}"] = {{
            "mean": np.array({key[0]}.get("{mapping['mean_key']}", 0.0), dtype=np.float32),
            "std":  np.array({key[0]}.get("{mapping['std_key']}", 1.0), dtype=np.float32),
        }}''')
    
    stats_parsing_code = "\n".join(stats_parsing)
    
    # Generate the required keys check
    required_keys_str = str(config['required_keys'])
    
    # Generate shape validation
    shape_validation = []
    for key in config['required_keys']:
        if key not in ['hydro_mask', 'flow_dir']:  # These are always 2D
            shape_validation.append(f"{key} = data['{key}']")
    
    shape_validation.append("hydro_mask = data['hydro_mask']")
    shape_validation.append("flow_dir = data['flow_dir']")
    
    # Check all are 2D (basic validation)
    shape_check_vars = " and ".join([f"{key}.ndim == 2" for key in ['hydro_mask', 'flow_dir']])
    for key in modality_keys:
        if key != 'alphaearth' and key != 'landsat':  # These can be 3D
            shape_check_vars += f" and {key}.ndim == 2"
    
    # Generate processing code
    processing_code = []
    for key in modality_keys:
        processing_code.append(f"{key} = data['{key}'].astype(np.float32)")
    processing_code.append("hydro_mask = data['hydro_mask'].astype(np.float32)")
    processing_code.append("flow_dir = data['flow_dir'].astype(np.int64)")
    
    # Generate normalization code
    normalization_code = []
    for key in modality_keys:
        if key in config['stats_mapping']:
            normalization_code.append(f"""                if huc_stats["{key}"] is not None:
                    {key} = _zscore({key}, huc_stats["{key}"]["mean"], huc_stats["{key}"]["std"])""")
    
    normalization_block = ""
    if normalization_code:
        normalization_block = f"""            # Apply normalization if available
            huc_stats = self.huc_norm.get(huc_code)
            if huc_stats:
{chr(10).join(normalization_code)}"""
    
    # Generate special processing if needed
    special_processing_block = ""
    if 'special_processing' in config:
        for key, code in config['special_processing'].items():
            special_processing_block += code + "\n"
    
    # Add channel dimensions for single-channel modalities (except special cases)
    channel_processing = []
    for key in modality_keys:
        if key not in config.get('special_processing', {}):
            if key not in ['alphaearth', 'landsat']:  # These have special handling
                channel_processing.append(f"            # Add channel dimension: (H,W) -> (1,H,W)")
                channel_processing.append(f"            {key} = {key}[None, :, :]")
    
    channel_processing_block = "\n".join(channel_processing)
    
    # Get init parameters and assignments
    init_params = config.get('init_params', '')
    init_assignments = config.get('init_assignments', '')
    
    template = f'''import os
import glob
import json
from typing import List, Optional, Tuple, Dict, Any

import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader

def _zscore(arr: np.ndarray, mean: np.ndarray, std: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    return (arr - mean) / (np.maximum(std, eps))

def _parse_huc_stats(stats_json: Dict[str, Any]) -> Dict[str, Dict[str, np.ndarray]]:
    """
    Convert your normalization_stats.json schema into broadcastable numpy arrays.
    For {config['description']}, we need:
      {chr(10).join([f"  {key}: {config['stats_mapping'].get(key, {}).get('mean_key', 'N/A')}" for key in modality_keys])}
    """
    out = {{{stats_dict_init}}}

{stats_parsing_code}
    return out

class MultimodalPatchDataset_{variant_name.upper()}_Fast(Dataset):
    """
    FAST version: Loads patches on-demand with lazy validation.
    
    Instead of pre-validating all files, this version:
    1. Just finds all patch_*.npz files during init (fast glob)
    2. Validates and loads data only when __getitem__ is called
    3. Skips invalid files silently and moves to next valid index
    
    This is much faster for large datasets since validation happens lazily.
    """
    def __init__(
        self,
        base_path: str,
        huc_codes: List[str],
        {init_params}
        dtype_inputs: torch.dtype = torch.float32,
        stats_filename: str = "normalization_stats.json",
        warn_missing_stats: bool = True,
    ):
        self.base_path = base_path
        self.huc_codes = list(huc_codes)
        {init_assignments}
        self.dtype_inputs = dtype_inputs
        self.stats_filename = stats_filename
        self.warn_missing_stats = warn_missing_stats

        # Cache of per-HUC normalization dicts
        self.huc_norm: Dict[str, Optional[Dict[str, Dict[str, np.ndarray]]]] = {{}}
        
        # FAST: Just gather file paths without validation
        self.files = self._gather_all_files()  # Much faster - no file loading
        
        # Cache for valid files we've already validated
        self._validated_cache = {{}}
        self._skipped_files = set()

        if len(self.files) == 0:
            raise RuntimeError("No patch files found in specified HUCs.")

    def _load_huc_stats(self, huc_code: str) -> Optional[Dict[str, Dict[str, np.ndarray]]]:
        """Load per-HUC normalization stats."""
        if huc_code in self.huc_norm:
            return self.huc_norm[huc_code]
            
        stats_path = os.path.join(self.base_path, huc_code, self.stats_filename)
        
        if not os.path.exists(stats_path):
            if self.warn_missing_stats:
                print(f"[WARN] No normalization stats found at {{stats_path}}")
            self.huc_norm[huc_code] = None
            return None
            
        try:
            with open(stats_path, 'r') as f:
                stats_json = json.load(f)
                
            parsed_stats = _parse_huc_stats(stats_json)
            self.huc_norm[huc_code] = parsed_stats
            return parsed_stats
            
        except Exception as e:
            if self.warn_missing_stats:
                print(f"[WARN] Error loading stats from {{stats_path}}: {{e}}")
            self.huc_norm[huc_code] = None
            return None

    def _gather_all_files(self) -> List[Tuple[str, str]]:
        """FAST: Just collect (file_path, huc_code) pairs without validation."""
        all_files = []
        
        for huc_code in self.huc_codes:
            # Load normalization stats for this HUC
            self._load_huc_stats(huc_code)
            
            huc_dir = os.path.join(self.base_path, huc_code)
            if not os.path.isdir(huc_dir):
                continue
                
            # FAST: Just glob files, no validation
            patch_files = glob.glob(os.path.join(huc_dir, "patch_*.npz"))
            
            for patch_file in patch_files:
                all_files.append((patch_file, huc_code))
                    
        return all_files

    def _validate_and_load_file(self, file_path: str, huc_code: str) -> Optional[Dict[str, np.ndarray]]:
        """Validate and load a single file on-demand."""
        if file_path in self._validated_cache:
            return self._validated_cache[file_path]
            
        if file_path in self._skipped_files:
            return None
            
        try:
            with np.load(file_path) as data:
                # Required keys for {config['description']}
                required_keys = {required_keys_str}
                
                if not all(k in data for k in required_keys):
                    self._skipped_files.add(file_path)
                    return None
                    
                # Quick shape validation
                {chr(10).join([f"                {var}" for var in shape_validation])}
                
                # Basic shape validation - hydro_mask and flow_dir should be 2D
                if not (hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                    self._skipped_files.add(file_path)
                    return None
                    
                # All 2D arrays should have same spatial shape
                base_shape = hydro_mask.shape
                {chr(10).join([f"                if {key}.ndim == 2 and {key}.shape != base_shape:" for key in modality_keys if key not in ['alphaearth', 'landsat']])}
                {chr(10).join([f"                    self._skipped_files.add(file_path)" for key in modality_keys if key not in ['alphaearth', 'landsat']])}
                {chr(10).join([f"                    return None" for key in modality_keys if key not in ['alphaearth', 'landsat']])}
                
                # Load and cache the data
                result = {{
                    {chr(10).join([f"                    '{key}': {key}.copy()," for key in config['required_keys']])}
                }}
                
                # Cache for future use (optional - can disable if memory is limited)
                # self._validated_cache[file_path] = result
                
                return result
                
        except Exception as e:
            print(f"[WARN] Error reading {{file_path}}: {{e}}")
            self._skipped_files.add(file_path)
            return None

    def __len__(self) -> int:
        return len(self.files)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        """Load and validate file on-demand."""
        
        # Try to get a valid file starting from idx
        max_attempts = min(50, len(self.files))  # Limit attempts to avoid infinite loops
        
        for attempt in range(max_attempts):
            current_idx = (idx + attempt) % len(self.files)
            file_path, huc_code = self.files[current_idx]
            
            data = self._validate_and_load_file(file_path, huc_code)
            if data is None:
                continue  # Try next file
                
            # Process valid data
            {chr(10).join([f"            {line}" for line in processing_code])}
            
{normalization_block}
            
{special_processing_block}
            
{channel_processing_block}

            {config['return_format']}
        
        # If we couldn't find a valid file after max_attempts, raise an error
        raise RuntimeError(f"Could not find valid data after {{max_attempts}} attempts starting from index {{idx}}")

# Keep the original class for compatibility, but make it an alias to the fast version
MultimodalPatchDataset_{variant_name.upper()} = MultimodalPatchDataset_{variant_name.upper()}_Fast'''
    
    return template

def main():
    """Generate all fast patch loaders."""
    output_dir = Path("/u/nathanj/national_ml/experiments/data")
    
    for variant_name, config in VARIANT_CONFIGS.items():
        loader_code = generate_fast_patch_loader(variant_name, config)
        output_file = output_dir / f"patchDataLoader_{variant_name}_fast.py"
        
        with open(output_file, 'w') as f:
            f.write(loader_code)
        
        # Make sure the file is readable
        os.chmod(output_file, 0o644)
        
        print(f"Generated: {output_file}")
    
    print(f"\nGenerated {len(VARIANT_CONFIGS)} fast patch loaders:")
    for variant_name in VARIANT_CONFIGS.keys():
        print(f"  - patchDataLoader_{variant_name}_fast.py")

if __name__ == "__main__":
    main()