# AI Coding Assistant Instructions for National ML Project

## Project Overview

This is a **multimodal multitask deep learning** project for hydrographic feature delineation using satellite data. The core architecture processes multi-source satellite imagery (DEM, Landsat optical/thermal, SAR, AlphaEarth embeddings) through specialized encoders and fusion modules to predict water segmentation masks and D8 flow direction.

## Critical Architecture Patterns

### Model Variants & Data Flow
- **Pattern**: All models follow `MultimodalMultitaskModel_{Variant}` naming in `experiments/models/`
- **Data Loading**: Each model has matching `patchDataLoader_{variant}.py` with specific modality channels
- **Training**: Lightning modules in `experiments/training/` with paired data modules and CLI scripts
- **Key Insight**: Models use **per-HUC normalization** from `normalization_stats.json` files - never use global normalization

```python
# Example: DEM + Optical model expects these exact data keys:
batch = {
    'dem': tensor,      # (B, 1, 224, 224)
    'optical': tensor,  # (B, 6, 224, 224) - Landsat bands B2-B7
    'hydro_mask': tensor,    # Water segmentation ground truth
    'flow_dir': tensor       # D8 flow direction ground truth
}
```

### HUC-Based Processing Workflow
- **HUC Codes**: Use 8-digit USGS Hydrologic Unit Codes (e.g., "10020007", "03030005")
- **Patch System**: 224×224 pixel patches with 224-pixel stride (no overlap)
- **Multi-Stage Pipeline**: `huc_process.py` → `patch_process.py` → `stats_process.py` → training
- **Google Earth Engine**: Primary data source via `gee_tools.py` with project-specific GEE authentication

### Environment & Infrastructure
- **Environment Name**: `pytorch_gpu_cu118` (pinned versions for stability)
- **SLURM Integration**: All training uses `sbatch` submission with specific resource patterns:
  - Single jobs: 1 GPU, 16 cores, 100GB RAM, 12-48h
  - Array jobs: Multiple configurations with `SLURM_ARRAY_TASK_ID`
- **W&B Projects**: Separate projects per model variant (e.g., `national_ml_dem_optical`)

## Development Workflows

### Adding New Model Variants
1. Create model: `experiments/models/mdmt_{variant}.py` following encoder-fusion-decoder pattern
2. Create data loader: `experiments/data/patchDataLoader_{variant}.py` with proper normalization
3. Create Lightning module: `experiments/training/train_{variant}_lightning.py`
4. Create CLI: `experiments/training/run_lightning_train_{variant}.py`
5. Create SLURM scripts: `submit_train_{variant}_{array|single}.sh`
6. Test with: `python example_{variant}.py`

### Data Processing Commands
```bash
# Process new HUCs (from data/script/)
python rerun_pipeline.py --hucs "10020007,03030005" --force-reprocess

# Training (from experiments/training/)
python run_lightning_train_dem_optical.py --hucs "03030005" --epochs 50 --batch_size 16

# SLURM submission
sbatch submit_train_dem_optical_array.sh  # 10 runs with different seeds
```

### Attention Analysis & Evaluation
- **GradCAM Analysis**: Use scripts in `experiments/evaluation/visual_analysis/attention_map/`
- **Model Evaluation**: Each variant has dedicated evaluator in `experiments/evaluation/`
- **Key Pattern**: Evaluators expect specific checkpoint naming conventions

## Project-Specific Conventions

### File Naming Patterns
- Models: `mdmt_{modality1}_{modality2}.py` or `mdmt_{single_modality}_only.py`
- Data loaders: `patchDataLoader_{matching_model_suffix}.py`
- Training: `train_{matching_model_suffix}_lightning.py`
- SLURM jobs: `submit_train_{matching_model_suffix}_{array|single}.sh`

### Data Modality Specifications
```python
# Channel specifications (critical for model architecture):
MODALITY_CHANNELS = {
    'dem': 1,           # Digital Elevation Model
    'optical': 6,       # Landsat B2,B3,B4,B5,B6,B7
    'thermal': 1,       # Landsat thermal B10
    'sar': 1,          # Sentinel-1 VV polarization
    'alphaearth': 64,  # Google satellite embeddings (configurable: 32/64/128)
}
```

### Configuration Management
- **Base Config**: `data/script/config.py` with GEE settings and patch parameters
- **Training Config**: CLI args in Lightning scripts, not config files
- **Environment**: Use `pytorch_gpu_cu118_environment.yml` for exact reproduction
- **W&B Settings**: `wandb_settings.yaml` for offline/online logging

## Common Pitfalls & Solutions

### Environment Issues
- **NEVER** auto-update packages - use pinned versions in environment files
- **CUDA availability**: Only on GPU compute nodes, not head nodes
- **Memory errors**: Reduce batch size or num_workers for large AlphaEarth models

### Data Loading Issues
- **Missing normalization_stats.json**: Run `stats_process.py` for each HUC first
- **Wrong data keys**: Each model variant expects specific keys (check data loader)
- **AlphaEarth missing**: Ensure AlphaEarth processing enabled in pipeline config

### Training Issues
- **SLURM job failures**: Check `slurm_logs/` directory for error details
- **W&B offline**: Use `--wandb_mode offline` for development, sync later
- **Checkpoint loading**: Use exact model class name matching for resuming

## Key Files to Reference

### For Model Architecture:
- `experiments/models/mdmt_v1.py` - Original 4-modality reference
- `experiments/models/mdmt_dem_optical.py` - Clean 2-modality example
- `experiments/README_Complete_Implementation.md` - All variants documented

### For Data Processing:
- `data/script/rerun_pipeline.py` - Main processing orchestrator
- `data/script/gee_tools.py` - Google Earth Engine integration
- `experiments/data/patchDataLoader_base.py` - Base data loading patterns

### For Training:
- `experiments/training/run_lightning_train_dem_optical.py` - Complete training example
- `experiments/training/submit_train_dem_optical_array.sh` - SLURM job template
- `TRAINING_VALIDATION_RESULTS.md` - Validation procedures and benchmarks

## Quick Reference Commands

```bash
# Environment setup
conda activate pytorch_gpu_cu118
python validate_environment.py

# Development testing
cd experiments && python example_dem_optical.py --demo_only

# Production training
cd experiments/training && sbatch submit_train_dem_optical_array.sh

# Monitor jobs
squeue -u $USER && tail -f slurm_logs_dem_optical/slurm_*.out
```

Focus on the **modality-specific patterns** and **HUC-based processing** when making changes. Each model variant is a complete ecosystem (model + data loader + training + evaluation) that should be developed together.