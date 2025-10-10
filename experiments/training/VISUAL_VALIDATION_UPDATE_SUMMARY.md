# Visual Validation Update Summary

## Overview
Updated all 8 MDMT variant Lightning modules to save visual validation outputs **every 5 epochs** instead of every 10 epochs, providing more frequent monitoring of training progress.

## Changes Made

### Visual Logging Frequency
- **Previous**: Most variants logged visual validation every 10 epochs (`self.current_epoch % 10 != 0`)
- **Updated**: All variants now log visual validation every 5 epochs (`self.current_epoch % 5 != 0`)

### Files Updated

#### 1. All Modalities + AlphaEarth (73 channels)
- **File**: `train_all_modalities_alphaearth_lightning.py`
- **Change**: Line 280: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 2. All Modalities (9 channels)
- **File**: `train_all_modalities_lightning.py` 
- **Change**: Line 288: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 3. DEM + AlphaEarth (65 channels)
- **File**: `train_dem_alphaearth_lightning.py`
- **Change**: Line 267: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 4. DEM + Optical (7 channels)
- **File**: `train_dem_optical_lightning.py`
- **Change**: Line 263: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 5. DEM + SAR (2 channels)
- **File**: `train_dem_sar_lightning.py`
- **Change**: Line 261: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 6. DEM + Thermal (2 channels)
- **File**: `train_dem_thermal_lightning.py`
- **Change**: Line 266: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 7. AlphaEarth Only (64 channels)
- **File**: `train_alphaearth_only_lightning.py`
- **Change**: Line 266: `% 10` → `% 5`
- **Comment**: Updated to "every 5 epochs"

#### 8. DEM Only (1 channel)
- **File**: `train_dem_only_lightning.py`
- **Status**: ✅ Already logging every 5 epochs (Line 194: `% 5 == 0`)
- **Action**: No changes needed

## Impact

### Training Monitoring
- **Increased Frequency**: Visual validation outputs now generated 2x more frequently
- **Better Progress Tracking**: More granular monitoring of:
  - Water segmentation predictions vs ground truth
  - D8 flow direction predictions vs ground truth  
  - Input modality visualizations (DEM, optical RGB, thermal, SAR, AlphaEarth)

### W&B Logging
- **Visual Timeline**: Denser timeline of prediction quality evolution
- **Early Detection**: Faster identification of training issues (overfitting, mode collapse, etc.)
- **Comparative Analysis**: Better comparison across variants with consistent 5-epoch intervals

### Resource Impact
- **Minimal Overhead**: Visual logging only occurs on first validation batch, first sample
- **Storage**: Slight increase in W&B storage usage (doubled visual logs)
- **Performance**: No impact on training speed (validation already running every epoch)

## Validation Pattern

All variants now follow this consistent pattern:
```python
def _log_val_visuals(self, batch):
    """Log a few samples (first val batch) every 5 epochs to W&B."""
    if not isinstance(self.logger, WandbLogger):
        return
    if self.global_rank != 0:  # avoid duplicate logs under DDP
        return
    if (self.current_epoch % 5) != 0:
        return
    # ... visualization code ...
```

## Benefits for Comparative Analysis

### Consistent Monitoring
- All 8 variants now have identical visual logging frequency
- Enables direct comparison of training progression across satellite data fusion approaches
- Facilitates early stopping decisions based on visual quality assessment

### Production Readiness
- Enhanced monitoring for full-scale comparative analysis (80 jobs = 8 variants × 10 seeds)
- Better debugging capabilities for single development runs
- Improved documentation of model behavior across training epochs

## Next Steps
- Deploy updated Lightning modules in production training runs
- Monitor W&B dashboards for visual validation outputs every 5 epochs
- Use increased visual frequency for early identification of optimal stopping points
- Leverage visual outputs for research publication figures and model behavior analysis

---
**Date**: October 10, 2025  
**Update Type**: Visual Validation Enhancement  
**Scope**: All 8 MDMT variant Lightning modules  
**Impact**: 2x increased visual monitoring frequency for better training oversight