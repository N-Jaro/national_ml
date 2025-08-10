import os
import glob
from typing import List, Optional, Tuple, Dict

import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader

class MultimodalPatchDataset(Dataset):
    """
    Loads patch .npz files under <base_path>/<huc_code>/patch_*.npz and yields
    modality tensors compatible with MultimodalMultitaskModel.forward(m1,m2,m3,m4).

    Requirements to keep a sample:
      - Keys present: 'dem', 'hydro_mask', 'flow_dir'
      - dem/hydro_mask/flow_dir must be 2D with identical (H,W)
    Missing optional keys are zero-filled:
      - 'optical' -> zeros (H,W,3)
      - 'thermal' -> zeros (H,W)
      - 'sar'     -> zeros (H,W)

    Returns a dict with:
      m1: dem (1,H,W) float32
      m2: optical (3,H,W) float32
      m3: thermal (1,H,W) float32
      m4: sar (1,H,W) float32
      hydro_mask: (H,W) float32
      flow_dir:   (H,W) int64
      path: str
    """
    def __init__(
        self,
        base_path: str,
        huc_codes: List[str],
        optical_channels: int = 3,
        dtype_inputs: torch.dtype = torch.float32,
        normalize_dem: Optional[Tuple[float, float]] = None,  # (mean, std)
    ):
        self.base_path = base_path
        self.huc_codes = list(huc_codes)
        self.optical_channels = optical_channels
        assert self.optical_channels == 3, "Model expects optical with 3 channels."
        self.dtype_inputs = dtype_inputs
        self.normalize_dem = normalize_dem

        self.files = self._gather_valid_files()
        if len(self.files) == 0:
            raise RuntimeError("No valid patch files found after filtering required keys.")

    def _gather_valid_files(self) -> List[str]:
        files = []
        for huc in self.huc_codes:
            pattern = os.path.join(self.base_path, huc, "patch_*.npz")
            for f in sorted(glob.glob(pattern)):
                try:
                    with np.load(f) as npz:
                        # Required keys
                        required = ("dem", "hydro_mask", "flow_dir")
                        if not all(k in npz for k in required):
                            print(f"[SKIP] Missing required key(s) in: {f}")
                            continue

                        dem = npz["dem"]
                        hydro = npz["hydro_mask"]
                        flow = npz["flow_dir"]

                        # Basic validations
                        if not (dem.ndim == hydro.ndim == flow.ndim == 2):
                            print(f"[SKIP] Required arrays must be 2D (H,W): {f}")
                            continue
                        if not (dem.shape == hydro.shape == flow.shape):
                            print(f"[SKIP] Shape mismatch among dem/hydro_mask/flow_dir: {f}")
                            continue

                        # If we pass checks, keep file
                        files.append(f)
                except Exception as e:
                    print(f"[SKIP] Error reading {f}: {e}")
        return files

    def __len__(self):
        return len(self.files)

    @staticmethod
    def _to_chw(arr: np.ndarray) -> np.ndarray:
        # (H,W) -> (1,H,W); (H,W,C) -> (C,H,W)
        if arr.ndim == 2:
            return arr[None, ...]
        elif arr.ndim == 3:
            return np.transpose(arr, (2, 0, 1))
        raise ValueError(f"Unexpected array ndim={arr.ndim}")

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        fpath = self.files[idx]
        with np.load(fpath) as npz:
            # Required
            dem = npz["dem"].astype(np.float32)          # (H,W)
            hydro_mask = npz["hydro_mask"].astype(np.float32)  # (H,W)
            flow_dir = npz["flow_dir"].astype(np.int64)  # (H,W), keep D8 codes

            H, W = dem.shape

            # Optional normalization for DEM
            if self.normalize_dem is not None:
                mean, std = self.normalize_dem
                std = 1.0 if std == 0 else std
                dem = (dem - mean) / std

            # Optional modalities (zero-fill if missing)
            if "optical" in npz:
                optical = npz["optical"].astype(np.float32)
                # Enforce (H,W,3)
                if optical.ndim == 2:
                    # replicate to 3 channels
                    optical = np.repeat(optical[..., None], self.optical_channels, axis=2)
                elif optical.ndim == 3:
                    c = optical.shape[2]
                    if c < self.optical_channels:
                        # pad channels with zeros
                        pad = np.zeros((H, W, self.optical_channels - c), dtype=np.float32)
                        optical = np.concatenate([optical, pad], axis=2)
                    elif c > self.optical_channels:
                        # slice extra channels
                        optical = optical[:, :, :self.optical_channels]
                else:
                    raise ValueError(f"optical has unexpected ndim={optical.ndim} in {fpath}")
            else:
                optical = np.zeros((H, W, self.optical_channels), dtype=np.float32)

            thermal = npz["thermal"].astype(np.float32) if "thermal" in npz else np.zeros((H, W), dtype=np.float32)
            sar = npz["sar"].astype(np.float32) if "sar" in npz else np.zeros((H, W), dtype=np.float32)

            # Convert to CHW
            m1 = self._to_chw(dem)       # (1,H,W)
            m2 = self._to_chw(optical)   # (3,H,W)
            m3 = self._to_chw(thermal)   # (1,H,W)
            m4 = self._to_chw(sar)       # (1,H,W)

        # Torch tensors
        sample = {
            "m1": torch.from_numpy(m1).to(self.dtype_inputs),          # dem
            "m2": torch.from_numpy(m2).to(self.dtype_inputs),          # optical
            "m3": torch.from_numpy(m3).to(self.dtype_inputs),          # thermal
            "m4": torch.from_numpy(m4).to(self.dtype_inputs),          # sar
            "hydro_mask": torch.from_numpy(hydro_mask),                # (H,W) float32
            "flow_dir": torch.from_numpy(flow_dir),                    # (H,W) int64
            "path": fpath
        }
        return sample


# -----------------------
# Example usage / sanity check with your model
# -----------------------
if __name__ == "__main__":
    # Adjust as needed
    base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
    huc_list = ["03030005"]

    ds = MultimodalPatchDataset(
        base_path=base_path,
        huc_codes=huc_list,
        optical_channels=3,      # model expects 3 for modality2
        dtype_inputs=torch.float32,
        normalize_dem=None       # or (mean, std)
    )

    dl = DataLoader(ds, batch_size=2, shuffle=True, num_workers=4, pin_memory=True)

    # Grab one batch and run through your model
    batch = next(iter(dl))
    print("Batch paths[0]:", batch["path"][0])
    for k in ("m1","m2","m3","m4","hydro_mask","flow_dir"):
        t = batch[k]
        print(f"{k}: shape={tuple(t.shape)}, dtype={t.dtype}")

    # ---- Plug into your model ----
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = MultimodalMultitaskModel(n_classes_task1=1, n_classes_task2=1).to(device)

    m1 = batch["m1"].to(device)  # (B,1,H,W)
    m2 = batch["m2"].to(device)  # (B,3,H,W)
    m3 = batch["m3"].to(device)  # (B,1,H,W)
    m4 = batch["m4"].to(device)  # (B,1,H,W)

    with torch.no_grad():
        out1, out2 = model(m1, m2, m3, m4)
    print("Model outputs:", out1.shape, out2.shape)  # expect (B,1,H,W) each
