# run_lightning_train.py
import torch
torch.set_float32_matmul_precision("high")

import argparse
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping

from train_mdmt_lightning import MDMTLitModule
from data_module import PatchDataModule

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_path", type=str, required=True,
                        help="Root folder with HUC_code subfolders containing patch_*.npz")
    parser.add_argument("--hucs", type=str, default="03030005",
                        help="Comma-separated HUC codes, e.g., 03030005,03030006")
    parser.add_argument("--batch_size", type=int, default=4)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--num_workers", type=int, default=4)
    parser.add_argument("--val_split", type=float, default=0.1)
    parser.add_argument("--precision", type=str, default="32", choices=["16", "32", "64", "bf16"])
    args = parser.parse_args()

    huc_list = [h.strip() for h in args.hucs.split(",") if h.strip()]

    # Data
    dm = PatchDataModule(
        base_path=args.base_path,
        huc_list=huc_list,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        val_split=args.val_split
    )

    # Model
    lit = MDMTLitModule(lr=args.lr)

    # Callbacks
    ckpt = ModelCheckpoint(
        monitor="val_loss",
        mode="min",
        save_top_k=3,
        filename="mdmt-{epoch:02d}-{val_loss:.4f}"
    )
    es = EarlyStopping(monitor="val_loss", mode="min", patience=5)

    trainer = pl.Trainer(
        max_epochs=args.epochs,
        accelerator="auto",
        devices=1,                  # use 1 GPU; set "auto" or 2+ if you want DDP
        precision=args.precision,   # "16", "32", "bf16"
        logger=False,               # <— disable TB logger
        log_every_n_steps=10,
        callbacks=[ckpt, es],
    )

    trainer.fit(lit, datamodule=dm)

if __name__ == "__main__":
    main()
