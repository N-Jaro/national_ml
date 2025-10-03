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
    Expected keys (based on your example):
      dem:     elevation_mean, elevation_stdDev
      optical: SR_B2_mean/stdDev, SR_B3_mean/stdDev, SR_B4_mean/stdDev
      thermal: ST_B10_mean/stdDev
      sar:     VV_mean/stdDev
    """
    out = {"dem": None, "optical": None, "thermal": None, "sar": None}

    if "dem" in stats_json:
        d = stats_json["dem"]
        out["dem"] = {
            "mean": np.array(d.get("elevation_mean", 0.0), dtype=np.float32),
            "std":  np.array(d.get("elevation_stdDev", 1.0), dtype=np.float32),
        }
    if "optical" in stats_json:
        o = stats_json["optical"]
        mean = [o.get("SR_B2_mean", 0.0), o.get("SR_B3_mean", 0.0), o.get("SR_B4_mean", 0.0)]
        std  = [o.get("SR_B2_stdDev", 1.0), o.get("SR_B3_stdDev", 1.0), o.get("SR_B4_stdDev", 1.0)]
        out["optical"] = {
            "mean": np.array(mean, dtype=np.float32).reshape(1, 1, 3),
            "std":  np.array(std,  dtype=np.float32).reshape(1, 1, 3),
        }
    if "thermal" in stats_json:
        t = stats_json["thermal"]
        out["thermal"] = {
            "mean": np.array(t.get("ST_B10_mean", 0.0), dtype=np.float32),
            "std":  np.array(t.get("ST_B10_stdDev", 1.0), dtype=np.float32),
        }
    if "sar" in stats_json:
        s = stats_json["sar"]
        out["sar"] = {
            "mean": np.array(s.get("VV_mean", 0.0), dtype=np.float32),
            "std":  np.array(s.get("VV_stdDev", 1.0), dtype=np.float32),
        }
    return out

class MultimodalPatchDataset(Dataset):
    """
    Loads <base_path>/<huc_code>/patch_*.npz with per-HUC normalization:
    reads <base_path>/<huc_code>/normalization_stats.json when present.

    Keeps sample only if required keys exist and shapes match:
      - 'dem', 'hydro_mask', 'flow_dir' (2D same HxW)

    Missing optional keys are zero-filled (in normalized space):
      - 'optical' -> zeros (H,W,3)
      - 'thermal' -> zeros (H,W)
      - 'sar'     -> zeros (H,W)

    Returns dict:
      m1 dem:       (1,H,W) float32
      m2 optical:   (3,H,W) float32  (B2,B3,B4)
      m3 thermal:   (1,H,W) float32
      m4 sar:       (1,H,W) float32
      hydro_mask:   (H,W)   float32
      flow_dir:     (H,W)   int64
      path:         str
    """
    def __init__(
        self,
        base_path: str,
        huc_codes: List[str],
        optical_channels: int = 3,
        dtype_inputs: torch.dtype = torch.float32,
        stats_filename: str = "normalization_stats.json",
        warn_missing_stats: bool = True,
    ):
        self.base_path = base_path
        self.huc_codes = list(huc_codes)
        self.optical_channels = optical_channels
        assert self.optical_channels == 3, "Model expects optical with 3 channels (SR_B2,B3,B4)."
        self.dtype_inputs = dtype_inputs
        self.stats_filename = stats_filename
        self.warn_missing_stats = warn_missing_stats

        # Cache of per-HUC normalization dicts (or None if not found)
        self.huc_norm: Dict[str, Optional[Dict[str, Dict[str, np.ndarray]]]] = {}
        self.files = self._gather_valid_files()  # also populates huc_norm

        if len(self.files) == 0:
            raise RuntimeError("No valid patch files found after filtering required keys.")

    def _load_huc_norm(self, huc: str) -> Optional[Dict[str, Dict[str, np.ndarray]]]:
        if huc in self.huc_norm:
            return self.huc_norm[huc]
        stats_path = os.path.join(self.base_path, huc, self.stats_filename)
        if os.path.exists(stats_path):
            try:
                with open(stats_path, "r") as f:
                    js = json.load(f)
                norm = _parse_huc_stats(js)
                self.huc_norm[huc] = norm
                return norm
            except Exception as e:
                print(f"[WARN] Failed to read stats for HUC {huc}: {e}")
        else:
            if self.warn_missing_stats:
                print(f"[WARN] No normalization stats file for HUC {huc}: {stats_path}")
        self.huc_norm[huc] = None
        return None

    def _gather_valid_files(self) -> List[str]:
        files = []
        for huc in self.huc_codes:
            # ensure stats cache is set (even if None)
            self._load_huc_norm(huc)
            pattern = os.path.join(self.base_path, huc, "patch_*.npz")
            for f in sorted(glob.glob(pattern)):
                try:
                    with np.load(f) as npz:
                        required = ("dem", "hydro_mask", "flow_dir")
                        if not all(k in npz for k in required):
                            print(f"[SKIP] Missing required key(s) in: {f}")
                            continue
                        dem = npz["dem"]; hydro = npz["hydro_mask"]; flow = npz["flow_dir"]
                        if not (dem.ndim == hydro.ndim == flow.ndim == 2):
                            print(f"[SKIP] Required arrays must be 2D (H,W): {f}")
                            continue
                        if not (dem.shape == hydro.shape == flow.shape):
                            print(f"[SKIP] Shape mismatch among dem/hydro_mask/flow_dir: {f}")
                            continue
                        files.append(f)
                except Exception as e:
                    print(f"[SKIP] Error reading {f}: {e}")
        return files

    def __len__(self):
        return len(self.files)

    @staticmethod
    def _to_chw(arr: np.ndarray) -> np.ndarray:
        if arr.ndim == 2:
            return arr[None, ...]
        elif arr.ndim == 3:
            return np.transpose(arr, (2, 0, 1))
        raise ValueError(f"Unexpected array ndim={arr.ndim}")

    @staticmethod
    def _huc_from_path(path: str) -> str:
        # expects .../<base_path>/<huc>/patch_*.npz
        return os.path.basename(os.path.dirname(path))

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        fpath = self.files[idx]
        huc = self._huc_from_path(fpath)
        norm = self._load_huc_norm(huc)  # may be None

        with np.load(fpath) as npz:
            dem = npz["dem"].astype(np.float32)                 # (H,W)
            hydro_mask = npz["hydro_mask"].astype(np.float32)   # (H,W)
            flow_dir = npz["flow_dir"].astype(np.int64)         # (H,W)

            H, W = dem.shape

            # ----- optical -----
            if "optical" in npz:
                optical = npz["optical"].astype(np.float32)     # (H,W) or (H,W,C)
                if optical.ndim == 2:
                    optical = np.repeat(optical[..., None], self.optical_channels, axis=2)
                elif optical.ndim == 3:
                    c = optical.shape[2]
                    if c < self.optical_channels:
                        pad = np.zeros((H, W, self.optical_channels - c), dtype=np.float32)
                        optical = np.concatenate([optical, pad], axis=2)
                    elif c > self.optical_channels:
                        optical = optical[:, :, :self.optical_channels]
                else:
                    raise ValueError(f"optical ndim={optical.ndim} in {fpath}")
            else:
                optical = np.zeros((H, W, self.optical_channels), dtype=np.float32)

            thermal = npz["thermal"].astype(np.float32) if "thermal" in npz else np.zeros((H, W), dtype=np.float32)
            sar     = npz["sar"].astype(np.float32)     if "sar"     in npz else np.zeros((H, W), dtype=np.float32)

            # ----- per-HUC normalization -----
            if norm is not None:
                if norm["dem"] is not None:
                    dem = _zscore(dem, norm["dem"]["mean"], norm["dem"]["std"])
                if norm["optical"] is not None:
                    optical = _zscore(optical, norm["optical"]["mean"], norm["optical"]["std"])
                if norm["thermal"] is not None:
                    thermal = _zscore(thermal, norm["thermal"]["mean"], norm["thermal"]["std"])
                if norm["sar"] is not None:
                    sar = _zscore(sar, norm["sar"]["mean"], norm["sar"]["std"])
            # else: leave raw values; you could print once if desired

            # Convert to CHW
            m1 = self._to_chw(dem)       # (1,H,W)
            m2 = self._to_chw(optical)   # (3,H,W)
            m3 = self._to_chw(thermal)   # (1,H,W)
            m4 = self._to_chw(sar)       # (1,H,W)

        sample = {
            "m1": torch.from_numpy(m1).to(self.dtype_inputs),
            "m2": torch.from_numpy(m2).to(self.dtype_inputs),
            "m3": torch.from_numpy(m3).to(self.dtype_inputs),
            "m4": torch.from_numpy(m4).to(self.dtype_inputs),
            "hydro_mask": torch.from_numpy(hydro_mask),
            "flow_dir": torch.from_numpy(flow_dir),
            "path": fpath,
        }
        return sample

# -----------------------
# Example (quick test)
# -----------------------
# if __name__ == "__main__":
#     base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
#     huc_list = ["03030005"]

#     ds = MultimodalPatchDataset(
#         base_path=base_path,
#         huc_codes=huc_list,
#         optical_channels=3,
#         dtype_inputs=torch.float32,
#         stats_filename="normalization_stats.json",
#     )

#     dl = DataLoader(ds, batch_size=2, shuffle=True, num_workers=4, pin_memory=True)
#     batch = next(iter(dl))
#     print("Batch paths[0]:", batch["path"][0])
#     for k in ("m1","m2","m3","m4","hydro_mask","flow_dir"):
#         t = batch[k]
#         print(f"{k}: shape={tuple(t.shape)}, dtype={t.dtype}, mean={t.float().mean().item():.3f}, std={t.float().std().item():.3f}")
