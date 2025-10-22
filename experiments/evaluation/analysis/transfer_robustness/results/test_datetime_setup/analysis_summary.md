# Transfer and Robustness Analysis Results

## Analysis Overview

This analysis addresses two key research questions:

- **4a) Transfer**: Does DEM improve terrain generalization? (AE-only vs DEM+AE)
- **4b) Robustness**: How robust is the model to missing optical data? (clean vs masked)

## Model Checkpoints Used

- **alphaearth_only**: `mdmt-alphaearth-only-epoch=28-val_loss=0.8024.ckpt`
- **dem_alphaearth**: `mdmt-dem-alphaearth-epoch=40-val_loss=0.4377.ckpt`
- **all_modalities**: `all_modalities_alphaearth_task3_seed456_epoch=09_val_loss=0.5666.ckpt`

## Analysis Configuration

- **Terrain thresholds**: Low <35.0m, High >113.9m
- **Cloud coverage**: 30.0%
- **Patches per HUC**: 20
- **Device**: cuda