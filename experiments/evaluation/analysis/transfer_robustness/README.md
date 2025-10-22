# Transfer/Robustness Analysis

**Analysis Date:** October 11, 2025  
**Purpose:** Evaluate All Modalities model performance across terrain complexity and optical data availability  

## Analysis Structure

### **4a Transfer Analysis**
- Test All Modalities model across terrain bins (low-relief vs high-relief)
- Terrain classification based on DEM relief: <35m (low) vs >114m (high)
- Metrics: mIoU, ClDice per terrain bin

### **4b Robustness Analysis**  
- Test All Modalities model with/without optical data (cloud simulation)
- Apply 30% cloud coverage masking to optical bands
- Metrics: Performance delta when optical is masked

## File Structure

```
transfer_robustness/
├── README.md                           # This file
├── config.py                          # Configuration parameters
├── terrain_classifier.py              # Terrain relief classification
├── optical_masker.py                  # Cloud simulation utilities
├── transfer_robustness_analysis.py    # Main analysis script
├── visualizer.py                      # 2x2 visualization generator
├── test_*.py                          # Unit tests for each component
├── run_transfer_robustness.sh         # SLURM execution script
└── results/                           # Output directory
    ├── transfer_results.json
    ├── robustness_results.json
    └── transfer_robustness_visualization.png
```

## Model & Data

- **Model**: All Modalities (DEM+SAR+Optical+Thermal+AlphaEarth)
- **Checkpoint**: `/u/nathanj/national_ml/outputs/models/all_modalities_alphaearth/all_modalities_alphaearth_task2_seed123_epoch=02_val_loss=0.6317.ckpt`
- **Test Data**: Same 10 representative HUCs from DEM sanity analysis
- **Test Path**: `/projects/bcrm/nathanj/data/processed/test/patch_dataset`

## Quick Testing Strategy

Each component will be tested individually before integration:
1. Test terrain classification on sample patches
2. Test optical masking on sample data
3. Test model loading and inference
4. Full pipeline integration
5. Results visualization