import os
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
    For DEM + Thermal model, we only need:
      dem:     elevation_mean, elevation_stdDev
      thermal: ST_B10_mean/stdDev
    """
    out = {"dem": None, "thermal": None}

    if "dem" in stats_json:
        d = stats_json["dem"]
        out["dem"] = {
            "mean": np.array(d.get("elevation_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("elevation_stdDev", 1.0), dtype=np.float32),
        }
    if "thermal" in stats_json:
        t = stats_json["thermal"]
        out["thermal"] = {
            "mean": np.array(t.get("ST_B10_mean", 0.0), dtype=np.float32),
            "std":  np.array(t.get("ST_B10_stdDev", 1.0), dtype=np.float32),
        }
    return out

class MultimodalPatchDataset_DEM_Thermal_Fast(Dataset):
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
        dtype_inputs: torch.dtype = torch.float32,
        stats_filename: str = "normalization_stats.json",
        warn_missing_stats: bool = True,
    ):
        self.base_path = base_path
        self.huc_codes = list(huc_codes)
        self.dtype_inputs = dtype_inputs
        self.stats_filename = stats_filename
        self.warn_missing_stats = warn_missing_stats

        # Cache of per-HUC normalization dicts
        self.huc_norm: Dict[str, Optional[Dict[str, Dict[str, np.ndarray]]]] = {}
        
        # FAST: Just gather file paths without validation
        self.files = self._gather_all_files()  # Much faster - no file loading
        
        # Cache for valid files we've already validated
        self._validated_cache = {}
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
                print(f"[WARN] No normalization stats found at {stats_path}")
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
                print(f"[WARN] Error loading stats from {stats_path}: {e}")
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
                # Required keys for DEM + Thermal model
                required_keys = ['dem', 'thermal', 'hydro_mask', 'flow_dir']
                
                if not all(k in data for k in required_keys):
                    self._skipped_files.add(file_path)
                    return None
                    
                # Quick shape validation
                dem = data['dem']
                thermal = data['thermal']
                hydro_mask = data['hydro_mask']
                flow_dir = data['flow_dir']
                
                # All should be 2D with same shape
                if not (dem.ndim == 2 and thermal.ndim == 2 and 
                       hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                    self._skipped_files.add(file_path)
                    return None
                    
                if not (dem.shape == thermal.shape == hydro_mask.shape == flow_dir.shape):
                    self._skipped_files.add(file_path)
                    return None
                
                # Load and cache the data
                result = {
                    'dem': dem.copy(),
                    'thermal': thermal.copy(),
                    'hydro_mask': hydro_mask.copy(),
                    'flow_dir': flow_dir.copy()
                }
                
                # Cache for future use (optional - can disable if memory is limited)
                # self._validated_cache[file_path] = result
                
                return result
                
        except Exception as e:
            print(f"[WARN] Error reading {file_path}: {e}")
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
            dem = data['dem'].astype(np.float32)
            thermal = data['thermal'].astype(np.float32)
            hydro_mask = data['hydro_mask'].astype(np.float32)
            flow_dir = data['flow_dir'].astype(np.int64)
            
            # Apply normalization if available
            huc_stats = self.huc_norm.get(huc_code)
            if huc_stats:
                if huc_stats["dem"] is not None:
                    dem = _zscore(dem, huc_stats["dem"]["mean"], huc_stats["dem"]["std"])
                if huc_stats["thermal"] is not None:
                    thermal = _zscore(thermal, huc_stats["thermal"]["mean"], huc_stats["thermal"]["std"])
            
            # Add channel dimension: (H,W) -> (1,H,W)
            dem = dem[None, :, :]
            thermal = thermal[None, :, :]

            return {
                'dem': torch.from_numpy(dem).to(self.dtype_inputs),
                'thermal': torch.from_numpy(thermal).to(self.dtype_inputs),
                'hydro_mask': torch.from_numpy(hydro_mask).to(self.dtype_inputs),
                'flow_dir': torch.from_numpy(flow_dir).long(),
                'path': file_path
            }
        
        # If we couldn't find a valid file after max_attempts, raise an error
        raise RuntimeError(f"Could not find valid data after {max_attempts} attempts starting from index {idx}")

# Keep the original class for compatibility, but make it an alias to the fast version
MultimodalPatchDataset_DEM_Thermal = MultimodalPatchDataset_DEM_Thermal_Fast