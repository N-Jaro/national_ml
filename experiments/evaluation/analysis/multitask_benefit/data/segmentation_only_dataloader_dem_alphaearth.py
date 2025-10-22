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
    For DEM + AlphaEarth model, we need:
      dem: elevation_mean, elevation_stdDev  
      alphaearth: No normalization needed (embeddings are pre-processed)
    """
    out = {"dem": None, "alphaearth": None}

    if "dem" in stats_json:
        d = stats_json["dem"]
        out["dem"] = {
            "mean": np.array(d.get("elevation_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("elevation_stdDev", 1.0), dtype=np.float32),
        }
    
    # AlphaEarth embeddings don't need normalization - they are already pre-processed
    # Return None to indicate no normalization should be applied
    out["alphaearth"] = None
    
    return out

class SegmentationOnlyPatchDataset_DEM_AlphaEarth(Dataset):
    """
    Segmentation-Only Dataset for DEM + AlphaEarth model.
    
    Identical to MultimodalPatchDataset_DEM_AlphaEarth but emphasizes that this is for
    SEGMENTATION-ONLY training (only uses 'hydro_mask', ignores 'flow_dir' during training).
    
    Loads <base_path>/<huc_code>/patch_*.npz with per-HUC normalization:
    reads <base_path>/<huc_code>/normalization_stats.json when present.

    Keeps sample only if required keys exist and shapes match:
      - 'dem', 'alphaearth', 'hydro_mask' (flow_dir present but not used in loss)
      - DEM: 2D elevation data (normalized)
      - AlphaEarth: 3D embedding data (raw values, no normalization)

    Returns dict:
      dem:          (1,H,W) float32 - normalized elevation data
      alphaearth:   (C,H,W) float32 - raw embedding values (configurable channels, default 64)
      hydro_mask:   (H,W)   float32 - ONLY ground truth used for training
      flow_dir:     (H,W)   int64 - present but ignored during training
      path:         str
    """

    def __init__(
        self,
        base_path: str,
        huc_codes: Optional[List[str]] = None,
        expected_alphaearth_channels: int = 64,
        debug: bool = False
    ):
        """
        Args:
            base_path: Root directory of patch data
            huc_codes: List of HUC codes to include, None = all available
            expected_alphaearth_channels: Expected AlphaEarth embedding dimensions (32, 64, 128)
            debug: Print debugging information
        """
        self.base_path = base_path
        self.expected_alphaearth_channels = expected_alphaearth_channels
        self.debug = debug

        # Find all HUC codes
        all_huc_dirs = [d for d in os.listdir(base_path) 
                       if os.path.isdir(os.path.join(base_path, d)) and len(d) == 8]
        
        if huc_codes is not None:
            all_huc_dirs = [h for h in all_huc_dirs if h in huc_codes]

        if self.debug:
            print(f"Found HUC directories: {all_huc_dirs}")
            print(f"Expected AlphaEarth channels: {expected_alphaearth_channels}")

        # Collect all patch files and load normalization stats
        self.samples = []
        self.normalization_stats = {}

        for huc_code in all_huc_dirs:
            huc_dir = os.path.join(base_path, huc_code)
            
            # Load normalization stats
            stats_file = os.path.join(huc_dir, "normalization_stats.json")
            if os.path.exists(stats_file):
                with open(stats_file, 'r') as f:
                    stats_json = json.load(f)
                self.normalization_stats[huc_code] = _parse_huc_stats(stats_json)
                if self.debug:
                    print(f"Loaded normalization stats for {huc_code}")
            else:
                print(f"Warning: No normalization stats found for {huc_code} at {stats_file}")
                self.normalization_stats[huc_code] = {"dem": None, "alphaearth": None}

            # Find patch files
            patch_files = glob.glob(os.path.join(huc_dir, "patch_*.npz"))
            
            if self.debug:
                print(f"HUC {huc_code}: Found {len(patch_files)} patches")

            for patch_file in patch_files:
                self.samples.append({
                    'huc_code': huc_code,
                    'path': patch_file
                })

        if self.debug:
            print(f"Total samples loaded: {len(self.samples)}")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        sample_info = self.samples[idx]
        huc_code = sample_info['huc_code']
        path = sample_info['path']

        # Load the .npz file
        try:
            data = np.load(path)
        except Exception as e:
            print(f"Error loading {path}: {e}")
            # Return dummy data to prevent crashes
            return self._get_dummy_sample(path)
        
        # Check for required keys
        required_keys = ['dem', 'alphaearth', 'hydro_mask', 'flow_dir']
        if not all(k in data for k in required_keys):
            missing = [k for k in required_keys if k not in data]
            if self.debug:
                print(f"Missing keys in {path}: {missing}")
            return self._get_dummy_sample(path)

        # Extract data arrays
        try:
            dem_raw = data['dem'].astype(np.float32)          # (H, W)
            alphaearth_raw = data['alphaearth'].astype(np.float32)  # (H, W, C) or (C, H, W)
            hydro_mask = data['hydro_mask'].astype(np.float32)      # (H, W)
            flow_dir = data['flow_dir'].astype(np.int64)            # (H, W)
            
            # Handle AlphaEarth shape - convert (H, W, C) to (C, H, W) if needed
            if len(alphaearth_raw.shape) == 3 and alphaearth_raw.shape[2] == self.expected_alphaearth_channels:
                alphaearth_raw = np.transpose(alphaearth_raw, (2, 0, 1))  # (H, W, C) -> (C, H, W)
            
        except Exception as e:
            if self.debug:
                print(f"Error extracting arrays from {path}: {e}")
            return self._get_dummy_sample(path)

        # Validate shapes
        if len(dem_raw.shape) != 2:
            if self.debug:
                print(f"Invalid DEM shape {dem_raw.shape} in {path}")
            return self._get_dummy_sample(path)
        
        if len(alphaearth_raw.shape) != 3:
            if self.debug:
                print(f"Invalid AlphaEarth shape {alphaearth_raw.shape} in {path}")
            return self._get_dummy_sample(path)
        
        if alphaearth_raw.shape[0] != self.expected_alphaearth_channels:
            if self.debug:
                print(f"AlphaEarth channel mismatch: expected {self.expected_alphaearth_channels}, got {alphaearth_raw.shape[0]} in {path}")
            return self._get_dummy_sample(path)

        # Apply normalization
        stats = self.normalization_stats.get(huc_code, {"dem": None, "alphaearth": None})
        
        # Normalize DEM if stats available
        dem_norm = dem_raw.copy()
        if stats["dem"] is not None:
            dem_norm = _zscore(dem_raw, stats["dem"]["mean"], stats["dem"]["std"])
        
        # AlphaEarth: no normalization (embeddings are pre-processed)
        alphaearth_norm = alphaearth_raw.copy()

        # Expand DEM to (1, H, W) format
        if len(dem_norm.shape) == 2:
            dem_norm = np.expand_dims(dem_norm, axis=0)  # (1, H, W)

        # Convert to tensors
        return {
            'dem': torch.from_numpy(dem_norm),           # (1, H, W)
            'alphaearth': torch.from_numpy(alphaearth_norm),  # (C, H, W)
            'hydro_mask': torch.from_numpy(hydro_mask),       # (H, W) - PRIMARY TARGET
            'flow_dir': torch.from_numpy(flow_dir),           # (H, W) - present but ignored
            'path': path
        }

    def _get_dummy_sample(self, path: str):
        """Return dummy sample to prevent crashes"""
        H, W = 224, 224
        return {
            'dem': torch.zeros(1, H, W, dtype=torch.float32),
            'alphaearth': torch.zeros(self.expected_alphaearth_channels, H, W, dtype=torch.float32),
            'hydro_mask': torch.zeros(H, W, dtype=torch.float32),
            'flow_dir': torch.zeros(H, W, dtype=torch.int64),
            'path': path
        }

# --- DataLoader Utilities ---

def create_segmentation_only_dataloader(
    base_path: str,
    huc_codes: Optional[List[str]] = None,
    batch_size: int = 8,
    shuffle: bool = True,
    num_workers: int = 4,
    expected_alphaearth_channels: int = 64,
    debug: bool = False
) -> DataLoader:
    """
    Create DataLoader for segmentation-only training.
    
    Args:
        base_path: Root directory of patch data
        huc_codes: List of HUC codes to include
        batch_size: Batch size for training
        shuffle: Whether to shuffle data
        num_workers: Number of worker processes
        expected_alphaearth_channels: AlphaEarth embedding dimensions
        debug: Enable debugging output
        
    Returns:
        DataLoader for segmentation-only training
    """
    
    dataset = SegmentationOnlyPatchDataset_DEM_AlphaEarth(
        base_path=base_path,
        huc_codes=huc_codes,
        expected_alphaearth_channels=expected_alphaearth_channels,
        debug=debug
    )
    
    if debug:
        print(f"Created segmentation-only dataset with {len(dataset)} samples")
    
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=True,
        drop_last=True
    )

# --- Example Usage ---
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Segmentation-Only DEM+AlphaEarth DataLoader")
    parser.add_argument('--data-path', type=str, required=True, help='Path to patch dataset')
    parser.add_argument('--huc-codes', type=str, nargs='+', help='HUC codes to test')
    parser.add_argument('--batch-size', type=int, default=4, help='Batch size')
    parser.add_argument('--alphaearth-channels', type=int, default=64, help='AlphaEarth channels')
    
    args = parser.parse_args()
    
    print("🔬 Testing Segmentation-Only DEM+AlphaEarth DataLoader")
    print("=" * 60)
    
    # Create dataloader
    dataloader = create_segmentation_only_dataloader(
        base_path=args.data_path,
        huc_codes=args.huc_codes,
        batch_size=args.batch_size,
        shuffle=True,
        expected_alphaearth_channels=args.alphaearth_channels,
        debug=True
    )
    
    print(f"\n📊 DataLoader created with {len(dataloader.dataset)} samples")
    print(f"🎯 Focus: SEGMENTATION-ONLY training (hydro_mask target)")
    
    # Test first batch
    try:
        batch = next(iter(dataloader))
        
        print(f"\n✅ Successfully loaded batch:")
        print(f"  DEM: {batch['dem'].shape} - {batch['dem'].dtype}")
        print(f"  AlphaEarth: {batch['alphaearth'].shape} - {batch['alphaearth'].dtype}")
        print(f"  Hydro Mask: {batch['hydro_mask'].shape} - {batch['hydro_mask'].dtype}")
        print(f"  Flow Dir: {batch['flow_dir'].shape} - {batch['flow_dir'].dtype} (present but ignored)")
        
        # Check normalization
        print(f"\n📈 Data Statistics:")
        print(f"  DEM: mean={batch['dem'].mean():.4f}, std={batch['dem'].std():.4f}")
        print(f"  AlphaEarth: mean={batch['alphaearth'].mean():.4f}, std={batch['alphaearth'].std():.4f}")
        print(f"  Hydro Mask: min={batch['hydro_mask'].min():.4f}, max={batch['hydro_mask'].max():.4f}")
        
        print(f"\n🎯 Ready for segmentation-only training!")
        print(f"   Training target: hydro_mask only")
        print(f"   Flow direction ignored during loss computation")
        
    except Exception as e:
        print(f"\n❌ Error loading batch: {e}")
        import traceback
        traceback.print_exc()