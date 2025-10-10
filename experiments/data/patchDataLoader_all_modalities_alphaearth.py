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
    For All Modalities + AlphaEarth model, we need:
      dem: elevation_mean, elevation_stdDev  
      optical: landsat_mean[6], landsat_stdDev[6] for B2-B7
      thermal: landsat_B10_mean, landsat_B10_stdDev
      sar: sentinel1_VV_mean, sentinel1_VV_stdDev
      alphaearth: No normalization needed (embeddings are pre-processed)
    """
    out = {"dem": None, "optical": None, "thermal": None, "sar": None, "alphaearth": None}

    if "dem" in stats_json:
        d = stats_json["dem"]
        out["dem"] = {
            "mean": np.array(d.get("elevation_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("elevation_stdDev", 1.0), dtype=np.float32),
        }
    
    if "landsat" in stats_json:
        d = stats_json["landsat"]
        # Optical bands B2-B7 (6 bands)
        optical_mean = d.get("landsat_mean", [0.0] * 6)[:6]  
        optical_std = d.get("landsat_stdDev", [1.0] * 6)[:6]
        out["optical"] = {
            "mean": np.array(optical_mean, dtype=np.float32),
            "std":  np.array(optical_std, dtype=np.float32),
        }
        
        # Thermal band B10 (1 band)
        thermal_mean = d.get("landsat_B10_mean", 0.0)
        thermal_std = d.get("landsat_B10_stdDev", 1.0)
        out["thermal"] = {
            "mean": np.array(thermal_mean, dtype=np.float32),
            "std":  np.array(thermal_std, dtype=np.float32),
        }
    
    if "sentinel1" in stats_json:
        d = stats_json["sentinel1"]
        out["sar"] = {
            "mean": np.array(d.get("sentinel1_VV_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("sentinel1_VV_stdDev", 1.0), dtype=np.float32),
        }
    
    # AlphaEarth embeddings don't need normalization - they are already pre-processed
    # Return None to indicate no normalization should be applied
    out["alphaearth"] = None
    
    return out

class MultimodalPatchDataset_All_Modalities_AlphaEarth(Dataset):
    """
    Loads <base_path>/<huc_code>/patch_*.npz with per-HUC normalization for All Modalities + AlphaEarth model:
    reads <base_path>/<huc_code>/normalization_stats.json when present.

    Keeps sample only if required keys exist and shapes match:
      - 'dem', 'optical', 'thermal', 'sar', 'alphaearth', 'hydro_mask', 'flow_dir'
      - DEM: 2D elevation data (normalized)
      - Optical: 3D Landsat B2-B7 data (6 bands, normalized)
      - Thermal: 2D Landsat B10 thermal data (normalized)
      - SAR: 2D Sentinel-1 VV data (normalized)
      - AlphaEarth: 3D embedding data (raw values, no normalization)

    Returns dict:
      dem:          (1,H,W) float32 - normalized elevation data
      optical:      (6,H,W) float32 - normalized Landsat B2-B7 bands
      thermal:      (1,H,W) float32 - normalized Landsat B10 thermal
      sar:          (1,H,W) float32 - normalized Sentinel-1 VV
      alphaearth:   (64,H,W) float32 - raw embedding values (fixed 64 channels)
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
        self.alphaearth_channels = 64  # Fixed to 64 channels
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
                        # Required keys for All Modalities + AlphaEarth model
                        required_keys = ['dem', 'optical', 'thermal', 'sar', 'alphaearth', 'hydro_mask', 'flow_dir']
                        
                        if not all(k in data for k in required_keys):
                            continue
                            
                        # Check shapes match
                        dem = data['dem']
                        optical = data['optical']
                        thermal = data['thermal']
                        sar = data['sar']
                        alphaearth = data['alphaearth']
                        hydro_mask = data['hydro_mask']
                        flow_dir = data['flow_dir']
                        
                        # DEM, thermal, SAR, hydro, flow should be 2D with same shape
                        if not (dem.ndim == 2 and thermal.ndim == 2 and sar.ndim == 2 and 
                               hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                            continue
                        if not (dem.shape == thermal.shape == sar.shape == hydro_mask.shape == flow_dir.shape):
                            continue
                            
                        # Optical can be 2D (H,W) for single band or 3D (H,W,C) for multi-band
                        if optical.ndim == 2:
                            if optical.shape != dem.shape:
                                continue
                        elif optical.ndim == 3:
                            if optical.shape[:2] != dem.shape:
                                continue
                        else:
                            continue
                            
                        # AlphaEarth can be 2D (H,W) or 3D (H,W,C)
                        if alphaearth.ndim == 2:
                            if alphaearth.shape != dem.shape:
                                continue
                        elif alphaearth.ndim == 3:
                            if alphaearth.shape[:2] != dem.shape:
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
            thermal = data['thermal'].astype(np.float32)
            sar = data['sar'].astype(np.float32)
            alphaearth = data['alphaearth'].astype(np.float32)
            hydro_mask = data['hydro_mask'].astype(np.float32)
            flow_dir = data['flow_dir']
            
            H, W = dem.shape
            
            # Handle Optical data - ensure it has 6 bands for Landsat B2-B7
            if optical.ndim == 2:
                # If 2D, assume single-band and replicate to 6 bands
                optical = np.repeat(optical[..., None], 6, axis=2)
            elif optical.ndim == 3:
                c = optical.shape[2]
                if c < 6:
                    # Pad with zeros if not enough channels
                    pad = np.zeros((H, W, 6 - c), dtype=np.float32)
                    optical = np.concatenate([optical, pad], axis=2)
                elif c > 6:
                    # Truncate to first 6 channels
                    optical = optical[:, :, :6]
            
            # Handle AlphaEarth data - ensure it has the right number of channels
            if alphaearth.ndim == 2:
                # If 2D, assume single-band and replicate to desired channels
                # (This is unlikely for real AlphaEarth data but handles edge cases)
                alphaearth = np.repeat(alphaearth[..., None], self.alphaearth_channels, axis=2)
            elif alphaearth.ndim == 3:
                c = alphaearth.shape[2]
                if c < self.alphaearth_channels:
                    # Pad with zeros if not enough channels
                    pad = np.zeros((H, W, self.alphaearth_channels - c), dtype=np.float32)
                    alphaearth = np.concatenate([alphaearth, pad], axis=2)
                elif c > self.alphaearth_channels:
                    # Truncate if too many channels
                    alphaearth = alphaearth[:, :, :self.alphaearth_channels]
            
            # Apply normalization based on modality
            if huc_stats:
                # Normalize DEM
                if huc_stats["dem"]:
                    dem = _zscore(dem, huc_stats["dem"]["mean"], huc_stats["dem"]["std"])
                
                # Normalize Optical (6 bands)
                if huc_stats["optical"]:
                    for band in range(6):
                        if band < optical.shape[2]:
                            optical[:, :, band] = _zscore(
                                optical[:, :, band], 
                                huc_stats["optical"]["mean"][band], 
                                huc_stats["optical"]["std"][band]
                            )
                
                # Normalize Thermal
                if huc_stats["thermal"]:
                    thermal = _zscore(thermal, huc_stats["thermal"]["mean"], huc_stats["thermal"]["std"])
                
                # Normalize SAR
                if huc_stats["sar"]:
                    sar = _zscore(sar, huc_stats["sar"]["mean"], huc_stats["sar"]["std"])
            
            # AlphaEarth embeddings are used as raw values (no normalization)
            
            # Convert to torch tensors with proper shapes
            dem_tensor = torch.from_numpy(dem).unsqueeze(0).to(self.dtype_inputs)  # (1, H, W)
            optical_tensor = torch.from_numpy(optical).permute(2, 0, 1).to(self.dtype_inputs)  # (6, H, W)
            thermal_tensor = torch.from_numpy(thermal).unsqueeze(0).to(self.dtype_inputs)  # (1, H, W)
            sar_tensor = torch.from_numpy(sar).unsqueeze(0).to(self.dtype_inputs)  # (1, H, W)
            alphaearth_tensor = torch.from_numpy(alphaearth).permute(2, 0, 1).to(self.dtype_inputs)  # (C, H, W)
            hydro_mask_tensor = torch.from_numpy(hydro_mask).to(self.dtype_inputs)  # (H, W)
            flow_dir_tensor = torch.from_numpy(flow_dir).to(torch.int64)  # (H, W)
            
            return {
                'dem': dem_tensor,
                'optical': optical_tensor,
                'thermal': thermal_tensor,
                'sar': sar_tensor,
                'alphaearth': alphaearth_tensor,
                'hydro_mask': hydro_mask_tensor,
                'flow_dir': flow_dir_tensor,
                'path': file_path,
            }


def create_dataloader_all_modalities_alphaearth(
    base_path: str,
    huc_codes: List[str],
    batch_size: int = 8,
    shuffle: bool = True,
    num_workers: int = 4,
    **dataset_kwargs
) -> DataLoader:
    """Create a DataLoader for All Modalities + AlphaEarth model (fixed 64 channels)."""
    dataset = MultimodalPatchDataset_All_Modalities_AlphaEarth(
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
    base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
    huc_codes = ["10020007"]  # Example HUC codes
    
    dataset = MultimodalPatchDataset_All_Modalities_AlphaEarth(
        base_path=base_path,
        huc_codes=huc_codes,
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) > 0:
        sample = dataset[0]
        print(f"Sample keys: {sample.keys()}")
        print(f"DEM shape: {sample['dem'].shape}")
        print(f"Optical shape: {sample['optical'].shape}")
        print(f"Thermal shape: {sample['thermal'].shape}")
        print(f"SAR shape: {sample['sar'].shape}")
        print(f"AlphaEarth shape: {sample['alphaearth'].shape}")
        print(f"Hydro mask shape: {sample['hydro_mask'].shape}")
        print(f"Flow dir shape: {sample['flow_dir'].shape}")
        print(f"Total input channels: {1 + 6 + 1 + 1 + 64} = 73")

    print("\nComprehensive All Modalities + AlphaEarth data loader handles:")
    print("- DEM: 1 channel (normalized elevation)")
    print("- Optical: 6 channels (normalized Landsat B2-B7)")
    print("- Thermal: 1 channel (normalized Landsat B10)")
    print("- SAR: 1 channel (normalized Sentinel-1 VV)")
    print("- AlphaEarth: 64 channels (raw embeddings)")
    print("This is the most comprehensive MDMT data pipeline!")