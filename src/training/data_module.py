# data_module.py

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytorch_lightning as pl
from torch.utils.data import DataLoader, random_split
from data.patchDataLoader import MultimodalPatchDataset

class PatchDataModule(pl.LightningDataModule):
    def __init__(
        self,
        base_path: str,
        huc_list,
        batch_size: int = 4,
        num_workers: int = 4,
        val_split: float = 0.1
    ):
        super().__init__()
        self.base_path = base_path
        self.huc_list = huc_list
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.val_split = val_split

    def setup(self, stage=None):
        full = MultimodalPatchDataset(
            base_path=self.base_path,
            huc_codes=self.huc_list,
            optical_channels=3,
            dtype_inputs=None,   # uses np -> torch default float32 in dataset
            normalize_dem=None
        )
        n_total = len(full)
        n_val = max(1, int(n_total * self.val_split))
        n_train = n_total - n_val
        self.train_ds, self.val_ds = random_split(full, [n_train, n_val])
        print(f"Using {n_train} train + {n_val} val patches (total={n_total})")

    def train_dataloader(self):
        return DataLoader(
            self.train_ds,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
            pin_memory=True
        )

    def val_dataloader(self):
        return DataLoader(
            self.val_ds,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True
        )
