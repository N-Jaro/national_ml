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
    For DEM + Optical model, we only need:
      dem:     elevation_mean, elevation_stdDev  
      optical: SR_B2_mean/stdDev, SR_B3_mean/stdDev, SR_B4_mean/stdDev, etc.
    """
    out = {"dem": None, "optical": None}

    if "dem" in stats_json:
        d = stats_json["dem"]
        out["dem"] = {
            "mean": np.array(d.get("elevation_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("elevation_stdDev", 1.0), dtype=np.float32),
        }
    if "optical" in stats_json:
        o = stats_json["optical"]
        # Support 6-band optical (B2,B3,B4,B5,B6,B7)
        mean = [
            o.get("SR_B2_mean", 0.0), o.get("SR_B3_mean", 0.0), o.get("SR_B4_mean", 0.0),
            o.get("SR_B5_mean", 0.0), o.get("SR_B6_mean", 0.0), o.get("SR_B7_mean", 0.0)
        ]
        std = [
            o.get("SR_B2_stdDev", 1.0), o.get("SR_B3_stdDev", 1.0), o.get("SR_B4_stdDev", 1.0),
            o.get("SR_B5_stdDev", 1.0), o.get("SR_B6_stdDev", 1.0), o.get("SR_B7_stdDev", 1.0)
        ]
        out["optical"] = {
            "mean": np.array(mean, dtype=np.float32).reshape(1, 1, 6),
            "std":  np.array(std,  dtype=np.float32).reshape(1, 1, 6),
        }
    return out

class MultimodalPatchDataset_DEM_Optical(Dataset):
    """
    Loads <base_path>/<huc_code>/patch_*.npz with per-HUC normalization:
    reads <base_path>/<huc_code>/normalization_stats.json when present.

    Keeps sample only if required keys exist and shapes match:
      - 'dem', 'hydro_mask', 'flow_dir' (2D same HxW)
      - 'optical' (2D or 3D same H,W,[C]) - now required for this dataset

    Returns dict:
      dem:          (1,H,W) float32
      optical:      (C,H,W) float32 (configurable channels, default 6)
      hydro_mask:   (H,W)   float32
      flow_dir:     (H,W)   int64
      path:         str
    """
    def __init__(
        self,
        base_path: str,
        huc_codes: List[str],
        optical_channels: int = 6,
        dtype_inputs: torch.dtype = torch.float32,
        stats_filename: str = "normalization_stats.json",
        warn_missing_stats: bool = True,
    ):
        self.base_path = base_path
        self.huc_codes = list(huc_codes)
        self.optical_channels = optical_channels
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
                        # Required keys for DEM + Optical model
                        required_keys = ['dem', 'optical', 'hydro_mask', 'flow_dir']
                        
                        if not all(k in data for k in required_keys):
                            continue
                            
                        # Check shapes match
                        dem = data['dem']
                        optical = data['optical']
                        hydro_mask = data['hydro_mask']
                        flow_dir = data['flow_dir']
                        
                        # DEM, hydro, flow should be 2D with same shape
                        if not (dem.ndim == 2 and hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                            continue
                        if not (dem.shape == hydro_mask.shape == flow_dir.shape):
                            continue
                            
                        # Optical can be 2D (H,W) or 3D (H,W,C)
                        if optical.ndim == 2:
                            if optical.shape != dem.shape:
                                continue
                        elif optical.ndim == 3:
                            if optical.shape[:2] != dem.shape:
                                continue
                        else:
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
            optical = data['optical'].astype(np.float32)
            hydro_mask = data['hydro_mask'].astype(np.float32)
            flow_dir = data['flow_dir']
            
            H, W = dem.shape
            
            # Handle optical data - ensure it has the right number of channels
            if optical.ndim == 2:
                # Broadcast single channel to desired number of channels
                optical = np.repeat(optical[..., None], self.optical_channels, axis=2)
            elif optical.ndim == 3:
                c = optical.shape[2]
                if c < self.optical_channels:
                    # Pad with zeros if not enough channels
                    pad = np.zeros((H, W, self.optical_channels - c), dtype=np.float32)
                    optical = np.concatenate([optical, pad], axis=2)
                elif c > self.optical_channels:
                    # Truncate if too many channels
                    optical = optical[:, :, :self.optical_channels]
            
            # Normalize DEM
            if huc_stats and huc_stats["dem"]:
                dem = _zscore(dem, huc_stats["dem"]["mean"], huc_stats["dem"]["std"])
            
            # Normalize Optical
            if huc_stats and huc_stats["optical"]:
                optical = _zscore(optical, huc_stats["optical"]["mean"], huc_stats["optical"]["std"])
            
            # Convert to torch tensors with proper shapes
            dem_tensor = torch.from_numpy(dem).unsqueeze(0).to(self.dtype_inputs)  # (1, H, W)
            optical_tensor = torch.from_numpy(optical).permute(2, 0, 1).to(self.dtype_inputs)  # (C, H, W)
            hydro_mask_tensor = torch.from_numpy(hydro_mask).to(self.dtype_inputs)  # (H, W)
            flow_dir_tensor = torch.from_numpy(flow_dir).to(torch.int64)  # (H, W)
            
            return {
                'dem': dem_tensor,
                'optical': optical_tensor,
                'hydro_mask': hydro_mask_tensor,
                'flow_dir': flow_dir_tensor,
                'path': file_path,
            }


def create_dataloader_dem_optical(
    base_path: str,
    huc_codes: List[str],
    batch_size: int = 8,
    shuffle: bool = True,
    num_workers: int = 4,
    optical_channels: int = 6,
    **dataset_kwargs
) -> DataLoader:
    """Create a DataLoader for DEM + Optical model."""
    dataset = MultimodalPatchDataset_DEM_Optical(
        base_path=base_path,
        huc_codes=huc_codes,
        optical_channels=optical_channels,
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
    
    dataset = MultimodalPatchDataset_DEM_Optical(
        base_path=base_path,
        huc_codes=huc_codes,
        optical_channels=6,
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) > 0:
        sample = dataset[0]
        print(f"Sample keys: {sample.keys()}")
        print(f"DEM shape: {sample['dem'].shape}")
        print(f"Optical shape: {sample['optical'].shape}")
        print(f"Hydro mask shape: {sample['hydro_mask'].shape}")
        print(f"Flow dir shape: {sample['flow_dir'].shape}")