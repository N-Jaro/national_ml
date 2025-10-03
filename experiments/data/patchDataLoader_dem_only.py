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
    For DEM-only model, we only need:
      dem: elevation_mean, elevation_stdDev
    """
    out = {"dem": None}

    if "dem" in stats_json:
        d = stats_json["dem"]
        out["dem"] = {
            "mean": np.array(d.get("elevation_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("elevation_stdDev", 1.0), dtype=np.float32),
        }
    return out

class MultimodalPatchDataset_DEM_Only(Dataset):
    """
    Loads <base_path>/<huc_code>/patch_*.npz with per-HUC normalization:
    reads <base_path>/<huc_code>/normalization_stats.json when present.

    Keeps sample only if required keys exist and shapes match:
      - 'dem', 'hydro_mask', 'flow_dir' (2D same HxW)

    Returns dict:
      dem:          (1,H,W) float32
      hydro_mask:   (H,W)   float32
      flow_dir:     (H,W)   int64
      path:         str
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

        # Cache of per-HUC normalization dicts (or None if not found)
        self.huc_norm: Dict[str, Optional[Dict[str, Dict[str, np.ndarray]]]] = {}
        self.files = self._gather_valid_files()  # also populates huc_norm

        if len(self.files) == 0:
            raise RuntimeError("No valid patch files found after filtering required keys.")

    def _load_huc_stats(self, huc_code: str) -> Optional[Dict[str, Dict[str, np.ndarray]]]:
        """Load per-HUC normalization stats."""
        if huc_code in self.huc_norm:
            return self.huc_norm[huc_code]
            
        huc_dir = os.path.join(self.base_path, huc_code)
        stats_path = os.path.join(huc_dir, self.stats_filename)
        
        if os.path.isfile(stats_path):
            try:
                with open(stats_path, 'r') as f:
                    stats_raw = json.load(f)
                    parsed = _parse_huc_stats(stats_raw)
                    self.huc_norm[huc_code] = parsed
                    return parsed
            except Exception as e:
                if self.warn_missing_stats:
                    print(f"Warning: Error loading stats for HUC {huc_code}: {e}")
        else:
            if self.warn_missing_stats:
                print(f"Warning: No normalization stats found for HUC {huc_code} at {stats_path}")
        
        self.huc_norm[huc_code] = None
        return None

    def _gather_valid_files(self) -> List[Tuple[str, str]]:
        """
        Gather (file_path, huc_code) pairs, filtering out samples that don't have
        the required keys or have mismatched shapes.
        """
        valid_files = []
        
        for huc_code in self.huc_codes:
            huc_dir = os.path.join(self.base_path, huc_code)
            if not os.path.isdir(huc_dir):
                print(f"Warning: HUC directory not found: {huc_dir}")
                continue
            
            # Load normalization stats for this HUC
            self._load_huc_stats(huc_code)
            
            pattern = os.path.join(huc_dir, "patch_*.npz")
            huc_files = glob.glob(pattern)
            
            for fpath in huc_files:
                try:
                    data = np.load(fpath)
                    required_keys = ['dem', 'hydro_mask', 'flow_dir']
                    
                    # Check if all required keys exist
                    if not all(k in data.files for k in required_keys):
                        continue
                    
                    # Check shapes match
                    dem_shape = data['dem'].shape
                    hydro_shape = data['hydro_mask'].shape
                    flow_shape = data['flow_dir'].shape
                    
                    if dem_shape != hydro_shape or dem_shape != flow_shape:
                        continue
                    
                    # Check that arrays are 2D
                    if len(dem_shape) != 2:
                        continue
                    
                    valid_files.append((fpath, huc_code))
                    
                except Exception as e:
                    print(f"Warning: Error checking file {fpath}: {e}")
                    continue
        
        print(f"DEM-only dataset: Found {len(valid_files)} valid patches across {len(self.huc_codes)} HUCs")
        return valid_files

    def __len__(self) -> int:
        return len(self.files)

    def __getitem__(self, idx: int) -> Dict[str, Any]:
        fpath, huc_code = self.files[idx]
        
        try:
            data = np.load(fpath)
            
            # Extract arrays
            dem = data['dem'].astype(np.float32)           # (H, W)
            hydro_mask = data['hydro_mask'].astype(np.float32)  # (H, W)
            flow_dir = data['flow_dir'].astype(np.int64)   # (H, W)
            
            # Apply per-HUC normalization if available
            norm_stats = self.huc_norm.get(huc_code)
            if norm_stats and norm_stats["dem"]:
                dem = _zscore(dem, norm_stats["dem"]["mean"], norm_stats["dem"]["std"])
            
            # Convert to tensors and add channel dimension for DEM
            dem_tensor = torch.from_numpy(dem[None, :, :]).to(self.dtype_inputs)  # (1, H, W)
            hydro_tensor = torch.from_numpy(hydro_mask[None, :, :]).to(self.dtype_inputs)  # (1, H, W)
            flow_tensor = torch.from_numpy(flow_dir).to(torch.int64)  # (H, W)
            
            return {
                'dem': dem_tensor,
                'hydro_mask': hydro_tensor,
                'flow_dir': flow_tensor,
                'path': fpath,
            }
            
        except Exception as e:
            print(f"Error loading {fpath}: {e}")
            raise

if __name__ == "__main__":
    # Test the dataset
    base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
    huc_codes = ["03030005"]
    
    dataset = MultimodalPatchDataset_DEM_Only(
        base_path=base_path,
        huc_codes=huc_codes
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) > 0:
        sample = dataset[0]
        print("Sample keys:", sample.keys())
        print("DEM shape:", sample['dem'].shape)
        print("Hydro mask shape:", sample['hydro_mask'].shape)
        print("Flow dir shape:", sample['flow_dir'].shape)
        print("DEM dtype:", sample['dem'].dtype)
        print("Hydro mask dtype:", sample['hydro_mask'].dtype)
        print("Flow dir dtype:", sample['flow_dir'].dtype)
        
        # Check value ranges
        print("DEM range:", sample['dem'].min().item(), "to", sample['dem'].max().item())
        print("Hydro mask range:", sample['hydro_mask'].min().item(), "to", sample['hydro_mask'].max().item())
        print("Flow dir unique values:", torch.unique(sample['flow_dir']).tolist())