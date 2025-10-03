# data_module_dem_sar.py
import torch
import numpy as np
import pytorch_lightning as pl
from torch.utils.data import DataLoader, random_split

from data.patchDataLoader_dem_sar import MultimodalPatchDataset_DEM_SAR

VALID_D8_CODES = [1, 2, 4, 8, 16, 32, 64, 128]

class PatchDataModule_DEM_SAR(pl.LightningDataModule):
    def __init__(
        self,
        base_path,
        huc_list,
        batch_size=4,
        num_workers=4,
        val_split=0.1,
        estimate_weights_from=64,   # number of samples for quick stats
    ):
        super().__init__()
        self.base_path = base_path
        self.huc_list = huc_list
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.val_split = val_split
        self.estimate_weights_from = estimate_weights_from

        # to be set in setup()
        self.train_ds = None
        self.val_ds = None
        self.water_pos_weight = None
        self.d8_class_weights = None

    def setup(self, stage=None):
        # full dataset (with per-HUC normalization inside loader)
        full = MultimodalPatchDataset_DEM_SAR(
            base_path=self.base_path,
            huc_codes=self.huc_list,
            dtype_inputs=torch.float32,
            stats_filename="normalization_stats.json",
        )
        n_total = len(full)
        n_val = max(1, int(n_total * self.val_split))
        n_train = n_total - n_val
        self.train_ds, self.val_ds = random_split(full, [n_train, n_val])
        print(f"Using {n_train} train + {n_val} val patches (total={n_total})")

        # ---- estimate weights on a subset of training patches ----
        N = min(self.estimate_weights_from, len(self.train_ds))
        loader = DataLoader(
            self.train_ds,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
        )

        pos = neg = 0
        counts = np.zeros(len(VALID_D8_CODES), dtype=np.float64)
        seen = 0

        for batch in loader:
            y = batch["hydro_mask"]  # (B,H,W)
            pos += (y == 1).sum().item()
            neg += (y == 0).sum().item()

            flow = batch["flow_dir"].long()  # (B,H,W)
            for i, code in enumerate(VALID_D8_CODES):
                counts[i] += (flow == code).sum().item()

            seen += y.shape[0]
            if seen >= N:
                break

        # water pos_weight = neg/pos
        self.water_pos_weight = float(neg / max(pos, 1))
        print(f"[INFO] Water pos_weight = {self.water_pos_weight:.3f}")

        # d8 class weights = inverse frequency normalized
        counts = torch.tensor(counts, dtype=torch.float32)
        inv = counts.sum() / torch.clamp(counts, min=1.0)
        inv = inv / inv.mean()
        self.d8_class_weights = inv.tolist()
        print(f"[INFO] D8 class weights = {self.d8_class_weights}")

    def train_dataloader(self):
        return DataLoader(
            self.train_ds,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
            pin_memory=True,
        )

    def val_dataloader(self):
        return DataLoader(
            self.val_ds,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True,
        )