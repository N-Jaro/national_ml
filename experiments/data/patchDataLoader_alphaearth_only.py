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

class MultimodalPatchDataset_AlphaEarth_Only(Dataset):
    """
    Loads <base_path>/<huc_code>/patch_*.npz for AlphaEarth-only model.
    
    AlphaEarth embeddings are already pre-processed, so no normalization is applied.
    Uses raw embedding values directly.

    Keeps sample only if required keys exist and shapes match:
      - 'alphaearth', 'hydro_mask', 'flow_dir' (2D same HxW for mask/flow, 3D for alphaearth)

    Returns dict:
      alphaearth:   (C,H,W) float32 (64 channels by default) - raw embedding values
      hydro_mask:   (H,W)   float32
      flow_dir:     (H,W)   int64
      path:         str
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
        self.files = self._gather_valid_files()  # populates file list

        if len(self.files) == 0:
            raise RuntimeError("No valid patch files found after filtering required keys.")

    def _load_huc_stats(self, huc_code: str) -> Optional[Dict[str, Dict[str, np.ndarray]]]:
        """Load per-HUC normalization stats (not used for AlphaEarth but kept for compatibility)."""
        if huc_code in self.huc_norm:
            return self.huc_norm[huc_code]
            
        # AlphaEarth doesn't need normalization, so we don't load stats
        # Just cache None for this HUC to avoid repeated checks
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
                        # Required keys for AlphaEarth-only model
                        required_keys = ['alphaearth', 'hydro_mask', 'flow_dir']
                        
                        if not all(k in data for k in required_keys):
                            continue
                            
                        # Check shapes match
                        alphaearth = data['alphaearth']
                        hydro_mask = data['hydro_mask']
                        flow_dir = data['flow_dir']
                        
                        # Hydro and flow should be 2D with same shape
                        if not (hydro_mask.ndim == 2 and flow_dir.ndim == 2):
                            continue
                        if not (hydro_mask.shape == flow_dir.shape):
                            continue
                            
                        # AlphaEarth can be 2D (H,W) or 3D (H,W,C)
                        if alphaearth.ndim == 2:
                            if alphaearth.shape != hydro_mask.shape:
                                continue
                        elif alphaearth.ndim == 3:
                            if alphaearth.shape[:2] != hydro_mask.shape:
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
        # huc_stats not used for AlphaEarth (no normalization needed)
        
        with np.load(file_path) as data:
            alphaearth = data['alphaearth'].astype(np.float32)
            hydro_mask = data['hydro_mask'].astype(np.float32)
            flow_dir = data['flow_dir']
            
            H, W = hydro_mask.shape
            
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
            
            # AlphaEarth embeddings are already pre-processed, no normalization needed
            # Skip normalization and use raw embedding values directly
            
            # Convert to torch tensors with proper shapes
            alphaearth_tensor = torch.from_numpy(alphaearth).permute(2, 0, 1).to(self.dtype_inputs)  # (C, H, W)
            hydro_mask_tensor = torch.from_numpy(hydro_mask).to(self.dtype_inputs)  # (H, W)
            flow_dir_tensor = torch.from_numpy(flow_dir).to(torch.int64)  # (H, W)
            
            return {
                'alphaearth': alphaearth_tensor,
                'hydro_mask': hydro_mask_tensor,
                'flow_dir': flow_dir_tensor,
                'path': file_path,
            }


def create_dataloader_alphaearth_only(
    base_path: str,
    huc_codes: List[str],
    batch_size: int = 8,
    shuffle: bool = True,
    num_workers: int = 4,
    alphaearth_channels: int = 64,
    **dataset_kwargs
) -> DataLoader:
    """Create a DataLoader for AlphaEarth-only model."""
    dataset = MultimodalPatchDataset_AlphaEarth_Only(
        base_path=base_path,
        huc_codes=huc_codes,
        alphaearth_channels=alphaearth_channels,
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
    huc_codes = ["03030005"]  # Example HUC codes
    
    dataset = MultimodalPatchDataset_AlphaEarth_Only(
        base_path=base_path,
        huc_codes=huc_codes,
        alphaearth_channels=64,
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) > 0:
        sample = dataset[0]
        print(f"Sample keys: {sample.keys()}")
        print(f"AlphaEarth shape: {sample['alphaearth'].shape}")
        print(f"Hydro mask shape: {sample['hydro_mask'].shape}")
        print(f"Flow dir shape: {sample['flow_dir'].shape}")
        
        # Test different channel configurations
        print("\n--- Testing Different Channel Configurations ---")
        
        # Test with 32 channels
        dataset_32 = MultimodalPatchDataset_AlphaEarth_Only(
            base_path=base_path,
            huc_codes=huc_codes,
            alphaearth_channels=32,
        )
        if len(dataset_32) > 0:
            sample_32 = dataset_32[0]
            print(f"32-channel AlphaEarth shape: {sample_32['alphaearth'].shape}")
        
        # Test with 128 channels
        dataset_128 = MultimodalPatchDataset_AlphaEarth_Only(
            base_path=base_path,
            huc_codes=huc_codes,
            alphaearth_channels=128,
        )
        if len(dataset_128) > 0:
            sample_128 = dataset_128[0]
            print(f"128-channel AlphaEarth shape: {sample_128['alphaearth'].shape}")