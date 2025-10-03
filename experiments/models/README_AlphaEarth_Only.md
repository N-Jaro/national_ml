# AlphaEarth-Only Model

This directory contains the AlphaEarth-only variant of the multimodal multitask model for water segmentation and D8 flow direction prediction.

## Overview

The AlphaEarth-only model uses Google's satellite embedding data (AlphaEarth) as the sole input modality. AlphaEarth provides rich, pre-trained 64-dimensional embeddings that capture complex Earth surface patterns from satellite imagery.

## Model Architecture

### Input
- **AlphaEarth Embeddings**: 64-channel embeddings (configurable: 32, 64, or 128 channels)
- **Resolution**: 10-meter (native AlphaEarth resolution)

### Architecture Details
- **Encoder**: Specialized AlphaEarth encoder with input channel reduction layer
- **Shared Processing**: Enhanced representation learning for embeddings
- **Decoders**: Dual task-specific decoders for water and flow prediction
- **Skip Connections**: U-Net style skip connections from AlphaEarth encoder

### Tasks
1. **Water Segmentation** (Task 1): Binary classification for water presence
2. **D8 Flow Direction** (Task 2): 8-class classification for flow direction

## Key Features

### Advantages
- ✅ **Rich Pre-trained Embeddings**: Leverages Google's state-of-the-art satellite foundation model
- ✅ **Simplified Data Pipeline**: Single modality reduces preprocessing complexity
- ✅ **Global Coverage**: Consistent AlphaEarth data availability worldwide
- ✅ **Foundation Model Power**: Benefits from large-scale pre-training

### Considerations
- 📊 **Interpretability**: Embeddings are less interpretable than raw sensor data
- 💾 **Memory Usage**: 64-channel inputs require more memory than single-band modalities
- 🔍 **Task-Specific Performance**: May miss modality-specific information (elevation, thermal, etc.)

## Files

```
models/
├── mdmt_alphaearth_only.py         # Main model architecture
data/
├── patchDataLoader_alphaearth_only.py  # Data loader for AlphaEarth data
training/
├── train_alphaearth_only_lightning.py  # Lightning training module
├── data_module_alphaearth_only.py      # Data module for training
├── run_lightning_train_alphaearth_only.py  # Training script
example_alphaearth_only.py              # Usage examples and demonstrations
README_AlphaEarth_Only.md               # This file
```

## Quick Start

### 1. Model Usage

```python
import torch
from models.mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only

# Create model
model = MultitaskModel_AlphaEarth_Only(
    n_classes_task1=1,      # Water segmentation
    n_classes_task2=8,      # D8 flow direction
    base_channels=64,
    alphaearth_channels=64  # Number of AlphaEarth embedding channels
)

# Example input (batch_size=2, channels=64, height=224, width=224)
alphaearth = torch.randn(2, 64, 224, 224)

# Forward pass
water_output, flow_output = model(alphaearth)
# water_output: (2, 1, 224, 224) - water segmentation logits
# flow_output: (2, 8, 224, 224) - flow direction logits
```

### 2. Data Loading

```python
from data.patchDataLoader_alphaearth_only import create_dataloader_alphaearth_only

dataloader = create_dataloader_alphaearth_only(
    base_path="/path/to/patch/data",
    huc_codes=["10020007"],
    batch_size=8,
    alphaearth_channels=64
)
```

### 3. Training

```bash
cd experiments/training

python run_lightning_train_alphaearth_only.py \
    --hucs 10020007,03030005 \
    --batch_size 8 \
    --epochs 50 \
    --lr 1e-3 \
    --alphaearth_channels 64 \
    --wandb_project my-alphaearth-experiments \
    --wandb_run alphaearth-only-v1
```

## Configuration Options

### Model Parameters
- `alphaearth_channels`: Number of embedding channels (32, 64, 128)
- `base_channels`: Base model capacity (32, 64, 128)
- `n_classes_task1`: Water segmentation classes (typically 1)
- `n_classes_task2`: Flow direction classes (typically 8)

### Training Parameters
- `--alphaearth_channels`: AlphaEarth embedding dimensions
- `--water_loss_scale`: Weight for water segmentation loss (default: 1.0)
- `--d8_loss_scale`: Weight for flow direction loss (default: 0.5)
- `--no_dynamic_weighter`: Disable uncertainty-based loss weighting

## Performance Considerations

### Memory Usage
- **64-channel input**: ~16x more memory than single-band inputs
- **Recommended**: Start with smaller batch sizes
- **GPU Memory**: Monitor CUDA memory usage during training

### Training Tips
1. **Learning Rate**: Start with 1e-3, may need adjustment for embeddings
2. **Batch Size**: Reduce if experiencing memory issues
3. **Loss Balancing**: Tune water vs. flow loss scales based on task priority
4. **Channel Configuration**: Try 32/64/128 channels based on available resources

## Data Requirements

### Input Data Format
AlphaEarth data should be stored in patch NPZ files with:
```python
{
    'alphaearth': numpy.ndarray,     # Shape: (H, W, 64) or (H, W, C)
    'hydro_mask': numpy.ndarray,     # Shape: (H, W) - water labels
    'flow_dir': numpy.ndarray,       # Shape: (H, W) - D8 flow codes
}
```

### Normalization
AlphaEarth embeddings are already pre-processed and **do not require normalization**. The data loader uses raw embedding values directly. No normalization statistics are needed in the `normalization_stats.json` file for AlphaEarth data.

## Comparison with Other Variants

| Aspect | AlphaEarth-Only | DEM-Only | DEM+Optical | DEM+SAR |
|--------|-----------------|----------|-------------|---------|
| **Input Channels** | 64 | 1 | 7 | 2 |
| **Memory Usage** | High | Low | Medium | Low |
| **Data Complexity** | Low | Low | Medium | Medium |
| **Weather Dependency** | None | None | Yes | No |
| **Interpretability** | Low | High | High | Medium |
| **Foundation Model** | Yes | No | No | No |

### When to Use AlphaEarth-Only
- ✅ Leveraging foundation model capabilities
- ✅ Simplified data pipeline requirements
- ✅ Research into embedding effectiveness for hydro tasks
- ✅ Consistent global data availability needed

### When to Consider Alternatives
- ❌ Limited computational resources (use DEM-only or DEM+SAR)
- ❌ Need for interpretable features (use DEM+Optical)
- ❌ Weather-independent monitoring required (use DEM+SAR)

## Visualization

The training script includes W&B visualizations:
- **AlphaEarth RGB**: First 3 channels displayed as pseudo-RGB
- **AlphaEarth Average**: Mean across all channels
- **Water Predictions**: Binary segmentation results
- **Flow Direction**: Colorized D8 flow predictions

## Advanced Usage

### Custom Channel Configurations

```python
# Reduced embedding (32 channels)
model_32 = MultitaskModel_AlphaEarth_Only(alphaearth_channels=32)

# Extended embedding (128 channels) 
model_128 = MultitaskModel_AlphaEarth_Only(alphaearth_channels=128)
```

### Loss Customization

```python
# Prioritize water segmentation
python run_lightning_train_alphaearth_only.py \
    --water_loss_scale 2.0 \
    --d8_loss_scale 0.1 \
    --no_dynamic_weighter
```

## Troubleshooting

### Common Issues

1. **CUDA Out of Memory**
   - Reduce `--batch_size`
   - Use `--precision 16` or `--precision bf16`
   - Reduce `--alphaearth_channels` to 32

2. **No Valid Patches Found**
   - Ensure AlphaEarth data exists in patch NPZ files
   - Check HUC codes are correct
   - Verify data path and file structure

3. **Poor Performance**
   - Verify AlphaEarth data quality (embeddings should be raw/unnormalized)
   - Tune loss scale parameters
   - Try different channel configurations

### Performance Monitoring

```bash
# Monitor GPU memory
nvidia-smi -l 1

# Check W&B logs for:
# - Loss convergence
# - Gradient norms  
# - Validation metrics
# - Embedding visualizations
```

**Note**: AlphaEarth embeddings are used as raw values without normalization, which is different from other modalities that typically require per-HUC normalization.

## Research Applications

This AlphaEarth-only variant is particularly valuable for:

1. **Foundation Model Evaluation**: Assessing Google's satellite embeddings for hydrological tasks
2. **Modality Comparison**: Comparing embedding performance vs. raw sensor data
3. **Transfer Learning**: Leveraging pre-trained representations for hydro applications
4. **Simplified Pipelines**: Reducing data preprocessing complexity in operational settings

## Citation

If you use this AlphaEarth-only model variant in your research, please cite:

```bibtex
@article{your_paper_2024,
    title={Your Paper Title},
    author={Your Name},
    journal={Your Journal},
    year={2024}
}
```