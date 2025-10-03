import os
import argparse
import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping
from pytorch_lightning.loggers import WandbLogger

from train_mdmt_lightning import MDMTLitModule
from data_module import PatchDataModule

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_path", type=str, default="/u/nathanj/national_ml/data/processed/patch_dataset/")
    parser.add_argument("--hucs", type=str, required=True, help="One or more HUC codes, comma-separated (e.g. 03030005,03040206,03050108)")
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
    parser.add_argument("--wandb_project", type=str, default="mdmt-hydro")
    parser.add_argument("--wandb_run", type=str, default=None)  # None -> auto name
    parser.add_argument("--wandb_mode", type=str, default="online", choices=["online", "offline", "disabled"])
    parser.add_argument("--wandb_dir", type=str, default="./lightning_logs")  # where to store run files on disk

    args = parser.parse_args()

    # Use tensor cores effectively on H100
    torch.set_float32_matmul_precision("high")

    # Prepare data
    huc_list = [h.strip() for h in args.hucs.split(",") if h.strip()]
    dm = PatchDataModule(
        base_path=args.base_path,
        huc_list=huc_list,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        val_split=args.val_split,
    )
    # compute stats (pos_weight & class weights) before creating the module
    dm.setup()

    # Model
    lit = MDMTLitModule(
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
        filename="mdmt-{epoch:02d}-{val_loss:.4f}",
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
