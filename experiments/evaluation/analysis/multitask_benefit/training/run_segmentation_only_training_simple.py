#!/usr/bin/env python3
"""
Segmentation-Only DEM+AlphaEarth Training Script
Using the exact same infrastructure as run_lightning_train_dem_alphaearth.py
but modified to only train water segmentation (no flow direction).
"""

import os
import argparse
import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping
from pytorch_lightning.loggers import WandbLogger

# Import the exact same data module as the working MDMT script
from data_module_dem_alphaearth import PatchDataModule_DEM_AlphaEarth

# Import our segmentation-only Lightning module
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from train_segmentation_only_dem_alphaearth_lightning import MDMT_SegmentationOnly_DEM_AlphaEarth_LitModule

def main():
    parser = argparse.ArgumentParser(description="Train Segmentation-Only DEM + AlphaEarth Model")
    
    # Copy exact arguments from run_lightning_train_dem_alphaearth.py
    parser.add_argument("--base_path", type=str, default="/u/nathanj/national_ml/data/processed/patch_dataset")
    parser.add_argument("--train_hucs", type=str, required=True, 
                       help="Training HUC codes, comma-separated (e.g. 03030005,03040206)")
    parser.add_argument("--val_hucs", type=str, required=True,
                       help="Validation HUC codes, comma-separated (e.g. 16040204,08020301)")
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--epochs", type=int, default=500)
    parser.add_argument("--lr", type=float, default=1e-4)
    parser.add_argument("--patience", type=float, default=15)
    parser.add_argument("--num_workers", type=int, default=8)
    parser.add_argument("--val_split", type=float, default=0.1)
    parser.add_argument("--precision", type=str, default="32", choices=["16", "32", "64", "bf16"])
    parser.add_argument("--alphaearth_channels", type=int, default=64, help="Number of AlphaEarth embedding channels")
    parser.add_argument("--seed", type=int, default=222324, help="Random seed for reproducibility")
    
    # Optimizer
    parser.add_argument("--optimizer", type=str, default="AdamW", choices=["Adam", "AdamW"], 
                       help="Optimizer to use (Adam or AdamW)")
    
    # W&B arguments
    parser.add_argument("--wandb_project", type=str, default="national_ml_segmentation_only_dem_alphaearth")
    parser.add_argument("--wandb_run", type=str, default=None)
    parser.add_argument("--wandb_mode", type=str, default="online", choices=["online", "offline", "disabled"])
    parser.add_argument("--wandb_dir", type=str, default="./lightning_logs")

    args = parser.parse_args()

    # Set random seed for reproducibility
    pl.seed_everything(args.seed, workers=True)

    # Use tensor cores effectively on H100
    torch.set_float32_matmul_precision("high")

    # Prepare data with HUC-level train/val split (exactly like MDMT)
    train_hucs = [h.strip() for h in args.train_hucs.split(",") if h.strip()]
    val_hucs = [h.strip() for h in args.val_hucs.split(",") if h.strip()]
    
    print(f"Train HUCs ({len(train_hucs)}): {train_hucs}")
    print(f"Val HUCs ({len(val_hucs)}): {val_hucs}")
    
    # Use the exact same data module as MDMT DEM+AlphaEarth
    dm = PatchDataModule_DEM_AlphaEarth(
        base_path=args.base_path,
        train_hucs=train_hucs,
        val_hucs=val_hucs,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        alphaearth_channels=args.alphaearth_channels,
    )
    # Compute stats (pos_weight & class weights) - same as multitask
    dm.setup()

    # Segmentation-only model (same data, different output)
    lit = MDMT_SegmentationOnly_DEM_AlphaEarth_LitModule(
        lr=args.lr,
        optimizer=args.optimizer,
        alphaearth_channels=args.alphaearth_channels,
        water_pos_weight=dm.water_pos_weight,  # Use same class balancing
    )

    # W&B logger setup (exactly like MDMT)
    os.makedirs(args.wandb_dir, exist_ok=True)
    os.environ["WANDB_DIR"] = os.path.abspath(args.wandb_dir)

    wandb_logger = WandbLogger(
        project=args.wandb_project,
        name=args.wandb_run,
        mode=args.wandb_mode,
        save_dir=args.wandb_dir,
        log_model=False,
    )
    wandb_logger.experiment.config.update(vars(args))

    # Checkpoint directory
    if args.wandb_run:
        checkpoint_dir = os.path.join(args.wandb_dir, args.wandb_run, "checkpoints")
    else:
        checkpoint_dir = os.path.join(args.wandb_dir, "checkpoints")

    # Callbacks (exactly like MDMT)
    ckpt = ModelCheckpoint(
        dirpath=checkpoint_dir,
        filename="segonly-dem-alphaearth-{epoch:02d}-{val_loss:.4f}",
        monitor="val_loss",
        mode="min",
        save_top_k=3
    )
    es = EarlyStopping(monitor="val_loss", mode="min", patience=args.patience, verbose=True)

    # Trainer setup (exactly like MDMT)
    trainer = pl.Trainer(
        max_epochs=args.epochs,
        accelerator="auto",
        devices=1,
        precision=args.precision,
        logger=wandb_logger,
        log_every_n_steps=10,
        callbacks=[ckpt, es],
    )

    print(f"Starting segmentation-only training...")
    print(f"Model: Segmentation-Only DEM + AlphaEarth (same data as multitask)")
    print(f"Train HUCs: {train_hucs}")
    print(f"Val HUCs: {val_hucs}")
    print(f"Epochs: {args.epochs}, Batch size: {args.batch_size}, LR: {args.lr}")
    print(f"Purpose: Single-task baseline for multitask benefit analysis")

    # Train model
    trainer.fit(lit, datamodule=dm)

    print("Training completed!")
    if hasattr(ckpt, 'best_model_path'):
        print(f"Best checkpoint: {ckpt.best_model_path}")

if __name__ == "__main__":
    main()