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

class MultimodalPatchDataset_DEM_Thermal(Dataset):
    """
    Loads <base_path>/<huc_code>/patch_*.npz with per-HUC normalization:
    reads <base_path>/<huc_code>/normalization_stats.json when present.

    Keeps sample only if required keys exist and shapes match:
      - 'dem', 'hydro_mask', 'flow_dir' (2D same HxW)
      - 'thermal' (2D same HxW) - now required for this dataset

    Returns dict:
      dem:          (1,H,W) float32
      thermal:      (1,H,W) float32
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
                    raw_stats = json.load(f)
                parsed = _parse_huc_stats(raw_stats)
                self.huc_norm[huc_code] = parsed
                return parsed
            except Exception as e:
                if self.warn_missing_stats:
                    print(f"[WARN] Failed to load {stats_path}: {e}")
                self.huc_norm[huc_code] = None
                return None
        else:
            if self.warn_missing_stats:
                print(f"[WARN] No stats file at {stats_path}")
            self.huc_norm[huc_code] = None
            return None

    def _gather_valid_files(self) -> List[Tuple[str, str]]:
        """Collect (file_path, huc_code) pairs that contain required keys."""
        valid_files = []
        
        for huc_code in self.huc_codes:
            # Load normalization stats for this HUC
            self._load_huc_stats(huc_code)
            
            huc_dir = os.path.join(self.base_path, huc_code)
            if not os.path.isdir(huc_dir):
                continue
                
            patch_files = glob.glob(os.path.join(huc_dir, "patch_*.npz"))
            
            for patch_file in patch_files:
                try:
                    with np.load(patch_file) as data:
                        # Required keys for DEM + Thermal model
                        required_keys = ['dem', 'thermal', 'hydro_mask', 'flow_dir']
                        
                        if not all(k in data for k in required_keys):
                            continue
                            
                        # Check shapes match
                        dem = data['dem']
                        thermal = data['thermal']
                        hydro_mask = data['hydro_mask']
                        flow_dir = data['flow_dir']
                        
                        if not (dem.shape == thermal.shape == hydro_mask.shape == flow_dir.shape):
                            continue
                            
                        if len(dem.shape) != 2:
                            continue
                            
                        valid_files.append((patch_file, huc_code))
                        
                except Exception as e:
                    print(f"[WARN] Error reading {patch_file}: {e}")
                    continue
                    
        return valid_files

    def __len__(self) -> int:
        return len(self.files)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        file_path, huc_code = self.files[idx]
        huc_stats = self.huc_norm[huc_code]
        
        with np.load(file_path) as data:
            dem = data['dem'].astype(np.float32)
            thermal = data['thermal'].astype(np.float32)
            hydro_mask = data['hydro_mask'].astype(np.float32)
            flow_dir = data['flow_dir']
            
            H, W = dem.shape
            
            # Normalize DEM
            if huc_stats and huc_stats["dem"]:
                dem = _zscore(dem, huc_stats["dem"]["mean"], huc_stats["dem"]["std"])
            
            # Normalize Thermal
            if huc_stats and huc_stats["thermal"]:
                thermal = _zscore(thermal, huc_stats["thermal"]["mean"], huc_stats["thermal"]["std"])
            
            # Convert to torch tensors with proper shapes
            dem_tensor = torch.from_numpy(dem).unsqueeze(0).to(self.dtype_inputs)  # (1, H, W)
            thermal_tensor = torch.from_numpy(thermal).unsqueeze(0).to(self.dtype_inputs)  # (1, H, W)
            hydro_mask_tensor = torch.from_numpy(hydro_mask).to(self.dtype_inputs)  # (H, W)
            flow_dir_tensor = torch.from_numpy(flow_dir).to(torch.int64)  # (H, W)
            
            return {
                'dem': dem_tensor,
                'thermal': thermal_tensor,
                'hydro_mask': hydro_mask_tensor,
                'flow_dir': flow_dir_tensor,
                'path': file_path,
            }


def create_dataloader_dem_thermal(
    base_path: str,
    huc_codes: List[str],
    batch_size: int = 8,
    shuffle: bool = True,
    num_workers: int = 4,
    **dataset_kwargs
) -> DataLoader:
    """Create a DataLoader for DEM + Thermal model."""
    dataset = MultimodalPatchDataset_DEM_Thermal(
        base_path=base_path,
        huc_codes=huc_codes,
        **dataset_kwargs
    )
    
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=True
    )


if __name__ == '__main__':
    # Example usage
    base_path = "/path/to/your/patch/data"
    huc_codes = ["10020007"]  # Example HUC codes
    
    dataset = MultimodalPatchDataset_DEM_Thermal(
        base_path=base_path,
        huc_codes=huc_codes,
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) > 0:
        sample = dataset[0]
        print(f"Sample keys: {sample.keys()}")
        print(f"DEM shape: {sample['dem'].shape}")
        print(f"Thermal shape: {sample['thermal'].shape}")
        print(f"Hydro mask shape: {sample['hydro_mask'].shape}")
        print(f"Flow dir shape: {sample['flow_dir'].shape}")