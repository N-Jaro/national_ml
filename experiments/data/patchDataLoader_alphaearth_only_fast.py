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
    For AlphaEarth-only model, we don't need normalization as embeddings are pre-processed.
    This function is kept for compatibility but AlphaEarth stats are not used.
    """
    out = {"alphaearth": None}
    
    # AlphaEarth embeddings don't need normalization - they are already pre-processed
    # Return None to indicate no normalization should be applied
    return out

class MultimodalPatchDataset_AlphaEarth_Only_Fast(Dataset):
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
        alphaearth_channels: int = 64,
        dtype_inputs: torch.dtype = torch.float32,
        stats_filename: str = "normalization_stats.json",
        warn_missing_stats: bool = True,
    ):
        self.base_path = base_path
        self.huc_codes = list(huc_codes)
        self.alphaearth_channels = alphaearth_channels
        self.dtype_inputs = dtype_inputs
        self.stats_filename = stats_filename
        self.warn_missing_stats = warn_missing_stats

        # Cache of per-HUC normalization dicts (not used for AlphaEarth but kept for compatibility)
        self.huc_norm: Dict[str, Optional[Dict[str, Dict[str, np.ndarray]]]] = {}
        
        # FAST: Just gather file paths without validation
        self.files = self._gather_all_files()  # Much faster - no file loading
        
        # Cache for valid files we've already validated
        self._validated_cache = {}
        self._skipped_files = set()

        if len(self.files) == 0:
            raise RuntimeError("No patch files found in specified HUCs.")

    def _load_huc_stats(self, huc_code: str) -> Optional[Dict[str, Dict[str, np.ndarray]]]:
        """Load per-HUC normalization stats (not used for AlphaEarth but kept for compatibility)."""
        if huc_code in self.huc_norm:
            return self.huc_norm[huc_code]
            
        # AlphaEarth doesn't need normalization, so we don't load stats
        # Just cache None for this HUC to avoid repeated checks
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

    def _validate_and_load_file(self, file_path: str) -> Optional[Dict[str, np.ndarray]]:
        """Validate and load a single file on-demand."""
        if file_path in self._validated_cache:
            return self._validated_cache[file_path]
            
        if file_path in self._skipped_files:
            return None
            
        try:
            with np.load(file_path) as data:
                # Required keys for AlphaEarth-only model
                required_keys = ['alphaearth', 'hydro_mask', 'flow_dir']
                
                if not all(k in data for k in required_keys):
                    self._skipped_files.add(file_path)
                    return None
                    
                # Quick shape validation
                alphaearth = data['alphaearth']
                hydro_mask = data['hydro_mask']
                flow_dir = data['flow_dir']
                
                # Hydro and flow should be 2D with same shape
                if not (hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                    self._skipped_files.add(file_path)
                    return None
                if not (hydro_mask.shape == flow_dir.shape):
                    self._skipped_files.add(file_path)
                    return None
                    
                # AlphaEarth can be 2D (H,W) or 3D (H,W,C)
                if alphaearth.ndim == 2:
                    if alphaearth.shape != hydro_mask.shape:
                        self._skipped_files.add(file_path)
                        return None
                elif alphaearth.ndim == 3:
                    if alphaearth.shape[:2] != hydro_mask.shape:
                        self._skipped_files.add(file_path)
                        return None
                else:
                    self._skipped_files.add(file_path)
                    return None
                
                # Load and cache the data
                result = {
                    'alphaearth': alphaearth.copy(),
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
            
            data = self._validate_and_load_file(file_path)
            if data is None:
                continue  # Try next file
                
            # Process valid data
            alphaearth = data['alphaearth'].astype(np.float32)
            hydro_mask = data['hydro_mask'].astype(np.float32)
            flow_dir = data['flow_dir'].astype(np.int64)
            
            # Handle AlphaEarth dimensions
            if alphaearth.ndim == 2:
                # (H,W) -> add channel dimension for single-channel
                alphaearth = alphaearth[None, :, :]  # (1,H,W)
            elif alphaearth.ndim == 3:
                # (H,W,C) -> (C,H,W)
                alphaearth = alphaearth.transpose(2, 0, 1)
            
            # Ensure we have the expected number of channels
            if alphaearth.shape[0] != self.alphaearth_channels:
                print(f"[WARN] Expected {self.alphaearth_channels} channels, got {alphaearth.shape[0]} in {file_path}")
                continue  # Try next file

            return {
                'alphaearth': torch.from_numpy(alphaearth).to(self.dtype_inputs),
                'hydro_mask': torch.from_numpy(hydro_mask).to(self.dtype_inputs),
                'flow_dir': torch.from_numpy(flow_dir).long(),
                'path': file_path
            }
        
        # If we couldn't find a valid file after max_attempts, raise an error
        raise RuntimeError(f"Could not find valid data after {max_attempts} attempts starting from index {idx}")

# Keep the original class for compatibility, but make it an alias to the fast version
MultimodalPatchDataset_AlphaEarth_Only = MultimodalPatchDataset_AlphaEarth_Only_Fast