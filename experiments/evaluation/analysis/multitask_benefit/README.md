# Multitask Benefit Analysis

## Directory Structure
```
multitask_benefit/
├── README.md                           # This file
├── multitask_benefit_analysis.py      # Main connectivity comparison analysis
├── models/                             # Model architectures
│   └── segmentation_only_model_dem_alphaearth.py
├── data/                               # Data loaders
│   └── segmentation_only_dataloader_dem_alphaearth.py
├── training/                           # Training infrastructure
│   ├── train_segmentation_only_dem_alphaearth_lightning.py
│   └── run_segmentation_only_training.py
├── slurm_scripts/                      # SLURM job scripts
│   ├── run_multitask_benefit.sh        # Analysis scripts
│   ├── run_multitask_benefit_dev.sh
│   ├── submit_segmentation_only_training.sh
│   └── submit_segmentation_only_training_dev.sh
├── tests/                              # Testing and validation
│   ├── test_segmentation_only_complete.py
│   └── quick_test_multitask.py
└── results*/                           # Generated results (created at runtime)
```

## Purpose
Compare segmentation-only models vs multitask learning (segmentation + flow direction) to demonstrate that **multitask learning improves hydrological coherence and stream connectivity**.

## Key Metrics
- **Connectivity Score (ClDice)**: Measures topological connectivity preservation
- **Hydrological Consistency**: Validates flow direction coherence with watershed topology  
- **Connected Components**: Counts isolated water segments (fewer = better connectivity)
- **Prediction Confidence**: Model uncertainty in water vs non-water predictions

## Architecture Comparison
- **Segmentation-Only**: Trained only on water mask prediction
- **Multitask**: Trained on both water segmentation AND D8 flow direction

## Files

### Core Analysis
- `multitask_benefit_analysis.py` - Main analysis script with connectivity metrics
- `tests/quick_test_multitask.py` - Quick validation test (2 batches only)

### SLURM Scripts
- `slurm_scripts/run_multitask_benefit.sh` - Full production analysis 
- `slurm_scripts/run_multitask_benefit_dev.sh` - Development version (50 batches)

## Required Checkpoints

### Multitask Model
Location: `/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_*/checkpoints/`
- Trained on both water segmentation + flow direction
- Architecture: `MultimodalMultitaskModel_DEM_AlphaEarth`

### Segmentation-Only Model
**Architecture Files:**
- `models/segmentation_only_model_dem_alphaearth.py` - Identical backbone, segmentation-only head
- `data/segmentation_only_dataloader_dem_alphaearth.py` - Data loader for single-task training
- `training/train_segmentation_only_dem_alphaearth_lightning.py` - Lightning module

**Training Scripts:**
- `training/run_segmentation_only_training.py` - CLI training script
- `slurm_scripts/submit_segmentation_only_training.sh` - Full production training (48h)
- `slurm_scripts/submit_segmentation_only_training_dev.sh` - Development training (12h, limited data)

**Quick Test:**
```bash
python tests/test_segmentation_only_complete.py  # Validate before training
```

**Training Commands:**
```bash
# Development run (quick validation)
sbatch slurm_scripts/submit_segmentation_only_training_dev.sh

# Full production run
sbatch slurm_scripts/submit_segmentation_only_training.sh
```

## Quick Test Usage
```bash
cd /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit
python tests/quick_test_multitask.py
```

## Production Usage
```bash
# Set up SLURM logs directory
mkdir -p slurm_logs_multitask_benefit

# Submit full analysis
sbatch slurm_scripts/run_multitask_benefit.sh

# Or development version (faster)
sbatch slurm_scripts/run_multitask_benefit_dev.sh
```

## Expected Results

### Connectivity Improvements
- **Higher ClDice scores** for multitask models (better stream connectivity)
- **Fewer connected components** (less fragmentation)
- **Better hydrological consistency** (flow aligns with topology)

### Output Structure
```
results/
├── multitask_comparison_summary.csv      # Overall metrics comparison
├── per_huc_connectivity_scores.csv       # Detailed per-HUC results
└── connectivity_analysis_detailed.csv    # Full diagnostic metrics
```

## Key Insight
The multitask objective acts as a **topological regularizer** - training on flow direction forces the model to learn physically coherent watershed structure, improving segmentation quality even without explicit topology constraints.

## Integration with DEM Analysis
This analysis pairs perfectly with the DEM sanity analysis:
1. **DEM Analysis**: Proves topographic signal importance (physical reasoning)
2. **Multitask Analysis**: Proves multitask learning importance (architectural benefits)

Together they demonstrate both the **data foundation** and **training methodology** contributions to hydrologically-aware deep learning.