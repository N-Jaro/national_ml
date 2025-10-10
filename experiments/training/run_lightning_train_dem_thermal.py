import os
import argparse
import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping
from pytorch_lightning.loggers import WandbLogger

from train_dem_thermal_lightning import MDMT_DEM_Thermal_LitModule
from data_module_dem_thermal import MDMT_DEM_Thermal_DataModule


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_path", type=str, default="/u/nathanj/national_ml/data/processed/patch_dataset/")
    parser.add_argument("--train_hucs", type=str, required=True, 
                       help="Training HUC codes, comma-separated (e.g. 03030005,03040206)")
    parser.add_argument("--val_hucs", type=str, required=True,
                       help="Validation HUC codes, comma-separated (e.g. 16040204,08020301)")
    parser.add_argument("--batch_size", type=int, default=4)
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--patience", type=float, default=15)
    parser.add_argument("--num_workers", type=int, default=4)
    parser.add_argument("--val_split", type=float, default=0.1)
    parser.add_argument("--precision", type=str, default="16", choices=["16", "32", "64", "bf16"])
    parser.add_argument("--water_loss_scale", type=float, default=1.0, help="Scale factor for water (task1) loss (default: 1.0)")
    parser.add_argument("--d8_loss_scale", type=float, default=0.5, help="Scale factor for D8 (task2) loss (default: 0.5)")
    parser.add_argument("--no_dynamic_weighter", action="store_true", help="Disable uncertainty-based dynamic loss weighting")

    # --- NEW OPTION HERE ---
    parser.add_argument("--optimizer", type=str, default="AdamW", choices=["Adam", "AdamW"], help="Optimizer to use (Adam or AdamW)")
    
    # --- W&B flags ---
    parser.add_argument("--wandb_project", type=str, default="mdmt-hydro-dem-thermal")
    parser.add_argument("--wandb_run", type=str, default=None)  # None -> auto name
    parser.add_argument("--wandb_mode", type=str, default="online", choices=["online", "offline", "disabled"])
    parser.add_argument("--wandb_dir", type=str, default="./lightning_logs")  # where to store run files on disk
    
    args = parser.parse_args()

    # Use tensor cores effectively on H100
    torch.set_float32_matmul_precision("high")

    # Prepare data with HUC-level train/val split
    train_hucs = [h.strip() for h in args.train_hucs.split(",") if h.strip()]
    val_hucs = [h.strip() for h in args.val_hucs.split(",") if h.strip()]
    
    print(f"Train HUCs ({len(train_hucs)}): {train_hucs}")
    print(f"Val HUCs ({len(val_hucs)}): {val_hucs}")
    
    dm = MDMT_DEM_Thermal_DataModule(
        base_path=args.base_path,
        train_hucs=train_hucs,
        val_hucs=val_hucs,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
    )
    # compute stats (pos_weight & class weights) before creating the module
    dm.setup()

    # Model
    lit = MDMT_DEM_Thermal_LitModule(
        lr=args.lr,
        optimizer=args.optimizer, # <-- Pass the optimizer choice
        water_pos_weight=dm.water_pos_weight,
        d8_class_weights=dm.d8_class_weights,
        d8_label_smoothing=0.05,
        # <<< new knobs >>>
        water_loss_scale=args.water_loss_scale,
        d8_loss_scale=args.d8_loss_scale,
        use_dynamic_weighter=not args.no_dynamic_weighter,
    )
    # --- W&B logger ---
    os.makedirs(args.wandb_dir, exist_ok=True)
    os.environ["WANDB_DIR"] = os.path.abspath(args.wandb_dir)

    wandb_logger = WandbLogger(
        project=args.wandb_project,
        name=args.wandb_run,
        mode=args.wandb_mode,       # "offline" works well on HPC; later: `wandb sync ./lightning_logs`
        save_dir=args.wandb_dir,
        log_model=False,            # set True if you want checkpoint upload
    )
    # save CLI args to the run config
    wandb_logger.experiment.config.update(vars(args))

    # If args.wandb_run is not None, use it to create a unique directory structure
    # This assumes args.wandb_dir is the base path (e.g., /path/to/wandb_logs)
    if args.wandb_run:
        checkpoint_dir = os.path.join(args.wandb_dir, args.wandb_run, "checkpoints")
    else:
        # Fallback if wandb_run is None (e.g., use the generic base path)
        checkpoint_dir = os.path.join(args.wandb_dir, "checkpoints")

    # Callbacks
    ckpt = ModelCheckpoint(
        dirpath=checkpoint_dir,
        filename="mdmt-dem-thermal-{epoch:02d}-{val_loss:.4f}",
        monitor="val_loss",
        mode="min",
        save_top_k=3
    )
    es = EarlyStopping(monitor="val_loss", mode="min", patience=args.patience, verbose=True)

    trainer = pl.Trainer(
        max_epochs=args.epochs,
        accelerator="auto",
        devices=1,                  # set to "auto" or an int>1 for multi-GPU
        precision=args.precision,   # "16", "32", "bf16"
        logger=wandb_logger,        # <--- W&B is active
        log_every_n_steps=10,
        callbacks=[ckpt, es],
    )

    trainer.fit(lit, datamodule=dm)

if __name__ == "__main__":
    main()