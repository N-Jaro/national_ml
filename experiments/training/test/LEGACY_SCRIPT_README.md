# Legacy Script: run_lightning_train_legacy.py

## ⚠️ **DEPRECATED - For Historical Reference Only**

This script is the **original/legacy** training script for All Modalities (no AlphaEarth) variant.

### **Why Deprecated**
- Uses **old random patch splitting** approach (`--val_split`)  
- Does **not support HUC-level geographic train/val separation**
- Uses old `PatchDataModule` instead of modern data architecture

### **Replaced By**
- **Production Script**: `run_lightning_train_all_modalities.py`
- **Modern Features**: HUC-level splitting, optimal HUC configuration, updated data modules

### **Historical Context**
- Original CLI for All Modalities (9 channels: DEM + 6-band Optical + Thermal + SAR)
- Used by original `submit_train_array.sh` before infrastructure updates
- Functional but superseded by superior geographic generalization approach

### **Usage** 
❌ **Do not use for production training**  
✅ **Keep for reference and backward compatibility understanding**

---
*Moved to test folder on 2025-10-10 during infrastructure cleanup*