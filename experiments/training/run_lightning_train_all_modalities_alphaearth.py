#!/usr/bin/env python3
"""
CLI script for training All Modalities + AlphaEarth MDMT model.

This script combines DEM, Optical (6-band Landsat), Thermal (Landsat B10), 
SAR (Sentinel-1), and AlphaEarth embeddings (64 channels by default).

Total input channels: 1 + 6 + 1 + 1 + 64 = 73 channels

Usage:
    python run_lightning_train_all_modalities_alphaearth.py --hucs "10020007,03030005" --epochs 50 --batch_size 8

For SLURM:
    sbatch submit_train_all_modalities_alphaearth_single.sh
"""

import os
import sys
import argparse
from typing import List
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping

# Add experiments directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from training.train_all_modalities_alphaearth_lightning import MDMT_All_Modalities_AlphaEarth_LitModule
from data.patchDataLoader_all_modalities_alphaearth import create_dataloader_all_modalities_alphaearth


class AllModalitiesAlphaEarthDataModule(pl.LightningDataModule):
    def __init__(
        self,
        base_path: str,
        train_hucs: List[str],
        val_hucs: List[str],
        batch_size: int = 8,
        num_workers: int = 4,
    ):
        super().__init__()
        self.base_path = base_path
        self.train_hucs = train_hucs
        self.val_hucs = val_hucs
        self.batch_size = batch_size
        self.num_workers = num_workers

    def train_dataloader(self):
        return create_dataloader_all_modalities_alphaearth(
            base_path=self.base_path,
            huc_codes=self.train_hucs,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
        )

    def val_dataloader(self):
        return create_dataloader_all_modalities_alphaearth(
            base_path=self.base_path,
            huc_codes=self.val_hucs,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
        )


def main():
    parser = argparse.ArgumentParser(description="Train All Modalities + AlphaEarth MDMT model")
    
    # Data arguments
    parser.add_argument("--base_path", type=str, 
                       default="/u/nathanj/national_ml/data/processed/patch_dataset",
                       help="Base path to patch dataset")
    parser.add_argument("--train_hucs", type=str, required=True,
                       help="Comma-separated list of training HUC codes (e.g., '10020007,03030005')")
    parser.add_argument("--val_hucs", type=str, required=True,
                       help="Comma-separated list of validation HUC codes (e.g., '16040204,08020301')")
    
    # Model arguments
    parser.add_argument("--base_channels", type=int, default=64,
                       help="Base channel count for model")
    
    # Training arguments
    parser.add_argument("--epochs", type=int, default=50,
                       help="Number of training epochs")
    parser.add_argument("--batch_size", type=int, default=8,
                       help="Batch size")
    parser.add_argument("--lr", type=float, default=1e-3,
                       help="Learning rate")
    parser.add_argument("--optimizer", type=str, default="AdamW",
                       choices=["AdamW", "Adam"],
                       help="Optimizer type")
    
    # Loss weighting arguments
    parser.add_argument("--water_loss_scale", type=float, default=1.0,
                       help="Scaling factor for water segmentation loss")
    parser.add_argument("--d8_loss_scale", type=float, default=0.5,
                       help="Scaling factor for D8 flow direction loss")
    parser.add_argument("--d8_label_smoothing", type=float, default=0.05,
                       help="Label smoothing for D8 classification")
    
    # System arguments
    parser.add_argument("--num_workers", type=int, default=4,
                       help="Number of data loader workers")
    parser.add_argument("--gpus", type=int, default=1,
                       help="Number of GPUs to use")
    parser.add_argument("--precision", type=str, default="32",
                       choices=["16", "32"],
                       help="Training precision")
    
    # Logging arguments
    parser.add_argument("--wandb_project", type=str, 
                       default="national_ml_all_modalities_alphaearth",
                       help="W&B project name")
    parser.add_argument("--wandb_mode", type=str, default="online",
                       choices=["online", "offline", "disabled"],
                       help="W&B logging mode")
    parser.add_argument("--experiment_name", type=str, default=None,
                       help="Experiment name for logging")
    
    # Checkpoint arguments
    parser.add_argument("--checkpoint_dir", type=str, 
                       default="/u/nathanj/national_ml/outputs/models/all_modalities_alphaearth",
                       help="Directory to save checkpoints")
    parser.add_argument("--resume_from", type=str, default=None,
                       help="Path to checkpoint to resume from")
    
    # Early stopping
    parser.add_argument("--early_stopping_patience", type=int, default=15,
                       help="Early stopping patience (epochs)")
    
    # Random seed
    parser.add_argument("--seed", type=int, default=42,
                       help="Random seed")

    args = parser.parse_args()

    # Set random seed
    pl.seed_everything(args.seed, workers=True)

    # Parse HUC codes
    train_hucs = [huc.strip() for huc in args.train_hucs.split(",")]
    val_hucs = [huc.strip() for huc in args.val_hucs.split(",")]
    
    print(f"Train HUCs ({len(train_hucs)}): {train_hucs}")  
    print(f"Val HUCs ({len(val_hucs)}): {val_hucs}")
    print(f"Total HUCs: {len(train_hucs) + len(val_hucs)}")

    # Create data module
    data_module = AllModalitiesAlphaEarthDataModule(
        base_path=args.base_path,
        train_hucs=train_hucs,
        val_hucs=val_hucs,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
    )

    # Create model
    model = MDMT_All_Modalities_AlphaEarth_LitModule(
        lr=args.lr,
        optimizer=args.optimizer,
        base_channels=args.base_channels,
        water_loss_scale=args.water_loss_scale,
        d8_loss_scale=args.d8_loss_scale,
        d8_label_smoothing=args.d8_label_smoothing,
    )

    # Setup experiment name
    all_hucs = train_hucs + val_hucs
    if args.experiment_name is None:
        hucs_str = "_".join(all_hucs[:3])  # First 3 HUCs for name
        if len(all_hucs) > 3:
            hucs_str += f"_plus{len(all_hucs)-3}"
        args.experiment_name = f"all_modalities_alphaearth_{hucs_str}_seed{args.seed}"

    # Setup logging
    if args.wandb_mode != "disabled":
        logger = WandbLogger(
            project=args.wandb_project,
            name=args.experiment_name,
            mode=args.wandb_mode,
            tags=["all_modalities", "alphaearth", "ae64", f"seed{args.seed}"],
        )
        # Log hyperparameters
        logger.log_hyperparams({
            "model": "all_modalities_alphaearth",
            "total_input_channels": 73,  # 1 + 6 + 1 + 1 + 64
            "alphaearth_channels": 64,
            "modalities": ["dem", "optical", "thermal", "sar", "alphaearth"],
            "hucs": all_hucs,
            "train_hucs": train_hucs,
            "val_hucs": val_hucs,
            **vars(args)
        })
    else:
        logger = None

    # Setup callbacks
    callbacks = []
    
    # Model checkpointing
    os.makedirs(args.checkpoint_dir, exist_ok=True)
    checkpoint_callback = ModelCheckpoint(
        dirpath=args.checkpoint_dir,
        filename=f"{args.experiment_name}_{{epoch:02d}}_{{val_loss:.4f}}",
        monitor="val_loss",
        mode="min",
        save_top_k=3,
        save_last=True,
    )
    callbacks.append(checkpoint_callback)
    
    # Early stopping
    early_stop_callback = EarlyStopping(
        monitor="val_loss",
        mode="min",
        patience=args.early_stopping_patience,
        verbose=True,
    )
    callbacks.append(early_stop_callback)

    # Setup trainer
    trainer = pl.Trainer(
        max_epochs=args.epochs,
        devices=args.gpus,
        accelerator="gpu" if args.gpus > 0 else "cpu",
        precision=args.precision,
        logger=logger,
        callbacks=callbacks,
        deterministic=False,  # Set to False to avoid deterministic algorithm issues with CUDA
        log_every_n_steps=50,
        val_check_interval=0.5,  # Check validation twice per epoch
        gradient_clip_val=1.0,   # Gradient clipping for stability
        enable_progress_bar=True,
    )

    print(f"Starting training: {args.experiment_name}")
    print(f"Model: All Modalities + AlphaEarth (73 total channels)")
    print(f"Modalities: DEM(1) + Optical(6) + Thermal(1) + SAR(1) + AlphaEarth(64)")
    print(f"Train HUCs: {train_hucs}")
    print(f"Val HUCs: {val_hucs}")
    print(f"Epochs: {args.epochs}, Batch size: {args.batch_size}, LR: {args.lr}")
    print(f"Loss scaling: Water={args.water_loss_scale}, D8={args.d8_loss_scale}")

    # Train model
    if args.resume_from:
        trainer.fit(model, data_module, ckpt_path=args.resume_from)
    else:
        trainer.fit(model, data_module)

    print("Training completed!")
    
    # Print best checkpoint info
    if hasattr(checkpoint_callback, 'best_model_path'):
        print(f"Best checkpoint: {checkpoint_callback.best_model_path}")
        print(f"Best val_loss: {checkpoint_callback.best_model_score}")


if __name__ == "__main__":
    main()