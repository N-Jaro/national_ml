# Training Scripts for Model Variants

This directory contains complete training infrastructure for all model variants in the experiments folder.

## Available Training Scripts

### 1. **DEM + Optical Model**

#### Lightning Module
- `train_dem_optical_lightning.py` - PyTorch Lightning module for DEM + Optical training
- `data_module_dem_optical.py` - Specialized data module for DEM + Optical data
- `run_lightning_train_dem_optical.py` - CLI training script

#### Job Submission
- `submit_train_dem_optical_array.sh` - SLURM array job (10 runs with 4 concurrent)
- `submit_train_dem_optical_single.sh` - SLURM single job

### 2. **DEM + SAR Model**

#### Lightning Module
- `train_dem_sar_lightning.py` - PyTorch Lightning module for DEM + SAR training
- `data_module_dem_sar.py` - Specialized data module for DEM + SAR data
- `run_lightning_train_dem_sar.py` - CLI training script

#### Job Submission
- `submit_train_dem_sar_array.sh` - SLURM array job (10 runs with 4 concurrent)
- `submit_train_dem_sar_single.sh` - SLURM single job

### 3. **DEM + Thermal Model**

#### Lightning Module
- `train_dem_thermal_lightning.py` - PyTorch Lightning module for DEM + Thermal training
- `data_module_dem_thermal.py` - Specialized data module for DEM + Thermal data
- `run_lightning_train_dem_thermal.py` - CLI training script

#### Job Submission
- `submit_train_dem_thermal_array.sh` - SLURM array job (10 runs with 4 concurrent)
- `submit_train_dem_thermal_single.sh` - SLURM single job

### 4. **AlphaEarth-Only Model**

#### Lightning Module
- `train_alphaearth_only_lightning.py` - PyTorch Lightning module for AlphaEarth-only training
- `data_module_alphaearth_only.py` - Specialized data module for AlphaEarth embeddings
- `run_lightning_train_alphaearth_only.py` - CLI training script

#### Job Submission
- `submit_train_alphaearth_only_array.sh` - SLURM array job (3 channel configs with different batch sizes)
- `submit_train_alphaearth_only_single.sh` - SLURM single job

### 5. **Existing Models**

#### 4-Modality Models
- `train_mdmt_lightning.py` - Original 4-modality training (Landsat 6-band)
- `data_module.py` - Original data module
- `run_lightning_train.py` - Original CLI training script
- `submit_train_array.sh` - Original SLURM array job

#### Legacy DEM + Thermal
- `train_dem_thermal.py` - Legacy DEM + Thermal training script (non-Lightning)

## Usage Examples

### DEM + Optical Training

#### Local Development
```bash
# Single run for testing
python run_lightning_train_dem_optical.py \
    --hucs "03030005,03040206" \
    --batch_size 8 \
    --epochs 5 \
    --lr 1e-3 \
    --optical_channels 6 \
    --wandb_mode offline
```

#### Production Training (SLURM)
```bash
# Submit array job (10 runs, 4 concurrent)
sbatch submit_train_dem_optical_array.sh

# Submit single job
sbatch submit_train_dem_optical_single.sh
```

### DEM + SAR Training

#### Local Development
```bash
# Single run for testing
python run_lightning_train_dem_sar.py \
    --hucs "03030005,03040206" \
    --batch_size 8 \
    --epochs 5 \
    --lr 1e-3 \
    --wandb_mode offline
```

#### Production Training (SLURM)
```bash
# Submit array job (10 runs, 4 concurrent)
sbatch submit_train_dem_sar_array.sh

# Submit single job
sbatch submit_train_dem_sar_single.sh
```

### DEM + Thermal Training

#### Local Development
```bash
# Single run for testing
python run_lightning_train_dem_thermal.py \
    --hucs "03030005,03040206" \
    --batch_size 8 \
    --epochs 5 \
    --lr 1e-4 \
    --wandb_mode offline
```

#### Production Training (SLURM)
```bash
# Submit array job (10 runs, 4 concurrent)
sbatch submit_train_dem_thermal_array.sh

# Submit single job
sbatch submit_train_dem_thermal_single.sh
```

### AlphaEarth-Only Training

#### Local Development
```bash
# Single run for testing
python run_lightning_train_alphaearth_only.py \
    --hucs "03030005,03040206" \
    --batch_size 16 \
    --epochs 5 \
    --lr 1e-3 \
    --alphaearth_channels 64 \
    --wandb_mode offline
```

#### Production Training (SLURM)
```bash
# Submit array job (3 channel configurations)
sbatch submit_train_alphaearth_only_array.sh

# Submit single job
sbatch submit_train_alphaearth_only_single.sh
```

## Key Features

### 1. **Specialized Lightning Modules**
- **Model-Specific**: Each variant has its own Lightning module optimized for the specific architecture
- **Custom Visualizations**: Tailored W&B visualizations for each modality combination
- **Forward Pass**: Optimized forward pass for 2-modality models

### 2. **Specialized Data Modules**
- **Data Loading**: Custom data loaders for each model variant
- **Normalization**: Per-HUC statistical normalization
- **Class Balancing**: Automatic weight estimation for imbalanced datasets

### 3. **Comprehensive CLI Arguments**

#### Common Arguments
```bash
--base_path           # Path to patch dataset
--hucs               # Comma-separated HUC codes
--batch_size         # Training batch size
--epochs             # Maximum training epochs
--lr                 # Learning rate
--patience           # Early stopping patience
--num_workers        # DataLoader workers
--val_split          # Validation split ratio
--precision          # Training precision (16, 32, bf16)
--optimizer          # Optimizer choice (Adam, AdamW)
--water_loss_scale   # Task 1 loss scaling
--d8_loss_scale      # Task 2 loss scaling
--wandb_project      # W&B project name
--wandb_run          # W&B run name
--wandb_mode         # W&B mode (online, offline, disabled)
```

#### Model-Specific Arguments
```bash
# DEM + Optical only
--optical_channels      # Number of optical channels (default: 6)

# AlphaEarth-only
--alphaearth_channels   # Number of AlphaEarth embedding channels (default: 64)
```

### 4. **SLURM Integration**

#### Array Jobs
- **Multiple Runs**: 10 independent training runs
- **Concurrent Limit**: Maximum 4 jobs running simultaneously
- **Resource Allocation**: 1 GPU, 16 CPUs, 100GB RAM per job
- **Time Limit**: 48 hours per job

#### Single Jobs
- **Development**: Smaller HUC subset for faster testing
- **Shorter Training**: 100 epochs vs 500 for arrays
- **Quick Validation**: Test model variants before full training

### 5. **Logging & Monitoring**

#### W&B Integration
- **Project Separation**: Different W&B projects for each model variant
- **Run Naming**: Automatic timestamp and array ID naming
- **Comprehensive Metrics**: Loss, accuracy, Dice, IoU for both tasks
- **Visualizations**: Input/prediction panels every 10 epochs

#### SLURM Logs
- **Separate Logs**: Model-specific log files
- **Job Arrays**: Individual logs for each array task
- **Error Tracking**: Separate stdout and stderr files

## Model Architecture Differences

### DEM + Optical Model
```python
# Forward pass
dem = batch['dem']           # (B, 1, H, W) 
optical = batch['optical']   # (B, 6, H, W) - configurable channels
pred1, pred2 = model(dem, optical)
```

### DEM + SAR Model  
```python
# Forward pass
dem = batch['dem']     # (B, 1, H, W)
sar = batch['sar']     # (B, 1, H, W)
pred1, pred2 = model(dem, sar)
```

### AlphaEarth-Only Model
```python
# Forward pass
alphaearth = batch['alphaearth']  # (B, 64, H, W) - configurable channels
pred1, pred2 = model(alphaearth)
```

### Original 4-Modality Model
```python
# Forward pass
m1, m2, m3, m4 = batch['m1'], batch['m2'], batch['m3'], batch['m4']
pred1, pred2 = model(m1, m2, m3, m4)
```

## Performance Considerations

### Computational Efficiency
1. **DEM + SAR**: Fastest (2 single channels)
2. **DEM + Thermal**: Fast (2 single channels)
3. **DEM + Optical**: Moderate (1 + 6 channels)
4. **AlphaEarth-Only**: Moderate-High (64 embedding channels)
5. **4-Modality**: Slowest (1 + 6 + 1 + 1 channels)

### Memory Usage
- **2-Modality Models**: ~40% less memory than 4-modality
- **AlphaEarth-Only**: High memory usage due to 64-channel inputs
- **Batch Size**: Can use larger batch sizes with 2-modality models, smaller with AlphaEarth
- **GPU Utilization**: Better GPU memory efficiency (except AlphaEarth due to channel count)

### Training Speed
- **2-Modality**: ~60% faster per epoch
- **AlphaEarth-Only**: Moderate speed (single modality but 64 channels)
- **Data Loading**: Faster due to fewer modalities (AlphaEarth: no normalization needed)
- **Model Updates**: Fewer encoder parameters to update

## Best Practices

### 1. **Development Workflow**
```bash
# 1. Test locally with small dataset
python run_lightning_train_dem_optical.py --hucs "03030005" --epochs 5 --wandb_mode offline

# 2. Test single SLURM job
sbatch submit_train_dem_optical_single.sh

# 3. Submit full array job
sbatch submit_train_dem_optical_array.sh

# For AlphaEarth-only:
python run_lightning_train_alphaearth_only.py --hucs "03030005" --epochs 5 --alphaearth_channels 32 --wandb_mode offline
```

### 2. **Hyperparameter Selection**

#### Learning Rates
- **2-Modality Models**: Start with 1e-4 (higher than 4-modality due to simpler architecture)
- **4-Modality Models**: Start with 1e-5 (lower due to complexity)

#### Batch Sizes
- **2-Modality**: 32-64 (can use larger due to lower memory)
- **AlphaEarth-Only**: 8-24 (limited by 64-channel inputs)
- **4-Modality**: 16-32 (limited by GPU memory)

#### Loss Scaling
- **Water Loss Scale**: 1.0 (primary task)
- **D8 Loss Scale**: 0.5 (secondary task, harder to learn)

### 3. **Monitoring**

#### Key Metrics to Watch
- **val_loss**: Overall validation loss (early stopping)
- **val_dice**: Water segmentation performance
- **val_iou**: Water segmentation IoU
- **val_acc_micro**: D8 flow direction accuracy

#### W&B Panels
- Check visualization panels every 10 epochs
- Monitor loss curves for overfitting
- Compare different model variants

## AlphaEarth-Specific Considerations

### Key Differences
- **No Normalization**: AlphaEarth embeddings are used as raw values (no per-HUC normalization)
- **High Memory Usage**: 64-channel inputs require significantly more GPU memory
- **Channel Configurations**: Array job tests 32, 64, and 128 channel configurations
- **Batch Size Scaling**: Automatically adjusts batch size based on channel count

### Array Job Configuration
The AlphaEarth array job runs 3 different configurations:
```bash
# Configuration 0: 32 channels, batch size 24
# Configuration 1: 64 channels, batch size 16  
# Configuration 2: 128 channels, batch size 8
```

### Memory Requirements
- **32 channels**: ~8GB GPU memory
- **64 channels**: ~12GB GPU memory
- **128 channels**: ~20GB GPU memory

### Data Pipeline
- **No Stats Loading**: Skips normalization_stats.json parsing
- **Raw Embeddings**: Uses AlphaEarth values directly from patch files
- **Faster Loading**: No normalization computation during training

## Troubleshooting

### Common Issues
1. **CUDA Out of Memory**: Reduce batch size or use smaller models
   - **For AlphaEarth**: Try `--alphaearth_channels 32` and `--batch_size 8`
2. **Data Loading Errors**: Check HUC codes and data paths
   - **For AlphaEarth**: Ensure `alphaearth` key exists in patch NPZ files
3. **SLURM Job Failures**: Check slurm_logs/ for error details
4. **W&B Offline**: Set `--wandb_mode offline` for development
5. **AlphaEarth Missing**: Ensure AlphaEarth data was processed in patch creation pipeline

### Performance Issues
1. **Slow Training**: Reduce `num_workers` if I/O bound
2. **Poor Convergence**: Adjust learning rate or loss scaling
3. **Validation Plateau**: Increase patience or reduce learning rate

## File Structure
```
training/
├── train_dem_thermal_lightning.py           # DEM+Thermal Lightning module ✨ NEW
├── train_dem_optical_lightning.py           # DEM+Optical Lightning module
├── train_dem_sar_lightning.py               # DEM+SAR Lightning module  
├── train_alphaearth_only_lightning.py       # AlphaEarth-only Lightning module ✨ NEW
├── data_module_dem_thermal.py               # DEM+Thermal data module ✨ NEW
├── data_module_dem_optical.py               # DEM+Optical data module
├── data_module_dem_sar.py                   # DEM+SAR data module
├── data_module_alphaearth_only.py           # AlphaEarth-only data module ✨ NEW
├── run_lightning_train_dem_thermal.py       # DEM+Thermal CLI script ✨ NEW
├── run_lightning_train_dem_optical.py       # DEM+Optical CLI script
├── run_lightning_train_dem_sar.py           # DEM+SAR CLI script
├── run_lightning_train_alphaearth_only.py   # AlphaEarth-only CLI script ✨ NEW
├── submit_train_dem_thermal_array.sh        # DEM+Thermal array job ✨ NEW
├── submit_train_dem_thermal_single.sh       # DEM+Thermal single job ✨ NEW
├── submit_train_alphaearth_only_array.sh    # AlphaEarth-only array job ✨ NEW
├── submit_train_alphaearth_only_single.sh   # AlphaEarth-only single job ✨ NEW
├── submit_train_dem_optical_array.sh        # DEM+Optical array job
├── submit_train_dem_sar_array.sh            # DEM+SAR array job
├── submit_train_dem_optical_single.sh       # DEM+Optical single job
├── submit_train_dem_sar_single.sh           # DEM+SAR single job
├── train_mdmt_lightning.py                  # Original 4-modality module
├── train_dem_thermal.py                     # Legacy DEM+Thermal (non-Lightning)
├── data_module.py                           # Original data module
├── run_lightning_train.py                   # Original CLI script
├── submit_train_array.sh                    # Original array job
└── README_Training.md                       # This file
```