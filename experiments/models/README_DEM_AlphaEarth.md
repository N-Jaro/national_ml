# DEM + AlphaEarth Model

## Overview

The DEM + AlphaEarth model is a multimodal multitask deep learning architecture that combines:

- **DEM (Digital Elevation Model)**: Explicit topographic information as the primary modality
- **AlphaEarth**: Google's satellite embeddings providing rich Earth surface patterns

This fusion leverages both traditional hydrologic principles (elevation drives water flow) and modern foundation model capabilities (contextual Earth surface understanding).

## Architecture

### Key Components

1. **DEM Encoder**: Standard U-Net encoder processing 1-channel elevation data
2. **AlphaEarth Encoder**: Specialized encoder with input reducer for 64-channel embeddings  
3. **Hierarchical Attention Fusion**: DEM-guided attention for AlphaEarth features
4. **Shared Encoder**: Additional processing of fused multimodal features
5. **Task Decoders**: Separate decoders for water segmentation and D8 flow direction

### Model Features

- **Primary Modality**: DEM provides skip connections to decoders (topography is fundamental)
- **Secondary Modality**: AlphaEarth features guided by DEM attention weights
- **Configurable Channels**: Support for 32, 64, or 128 AlphaEarth embedding channels
- **Multitask Output**: Water segmentation (binary) + D8 flow direction (8-class)

## Data Requirements

### Input Data Format

The model expects patch data with these keys:
- `dem`: (H, W) elevation data - **normalized using HUC statistics**
- `alphaearth`: (H, W, C) embedding data - **raw values (pre-processed)**
- `hydro_mask`: (H, W) water segmentation ground truth
- `flow_dir`: (H, W) D8 flow direction ground truth

### Data Processing

- **DEM**: Z-score normalized using per-HUC statistics (elevation_mean, elevation_stdDev)
- **AlphaEarth**: Used as raw embedding values (already pre-processed by Google)
- **Outputs**: Standard binary masks and 8-class flow directions

## Model Variants

### Channel Configurations

| Configuration | AlphaEarth Channels | Memory Usage | Use Case |
|--------------|-------------------|--------------|----------|
| Reduced | 32 | Low | Resource-constrained environments |
| Standard | 64 | Medium | Default configuration |
| Extended | 128 | High | Maximum information retention |

## Usage

### Basic Model Creation

```python
from models.mdmt_dem_alphaearth import MultimodalMultitaskModel_DEM_AlphaEarth

model = MultimodalMultitaskModel_DEM_AlphaEarth(
    n_classes_task1=1,      # Water segmentation (binary)
    n_classes_task2=8,      # D8 flow direction (8 classes)
    base_channels=64,       # Model capacity
    alphaearth_channels=64  # AlphaEarth embedding channels
)

# Forward pass
water_pred, flow_pred = model(dem, alphaearth)
```

### Data Loading

```python
from data.patchDataLoader_dem_alphaearth import (
    MultimodalPatchDataset_DEM_AlphaEarth, 
    create_dataloader_dem_alphaearth
)

# Create dataset
dataset = MultimodalPatchDataset_DEM_AlphaEarth(
    base_path="/path/to/patch/data",
    huc_codes=["10020007", "03030005"],
    alphaearth_channels=64
)

# Create dataloader
dataloader = create_dataloader_dem_alphaearth(
    base_path="/path/to/patch/data",
    huc_codes=["10020007", "03030005"],
    batch_size=8,
    alphaearth_channels=64
)
```

### Example Script

Run the demonstration script to see the model in action:

```bash
cd /u/nathanj/national_ml/experiments
python example_dem_alphaearth.py
```

## Advantages

### Scientific Benefits

- **Interpretable Foundation**: DEM provides well-understood topographic basis
- **Rich Context**: AlphaEarth adds complex Earth surface patterns beyond elevation
- **Hydrologic Relevance**: Elevation is fundamental to water flow physics
- **Foundation Model Power**: Leverages Google's state-of-the-art embeddings

### Technical Benefits

- **Attention Fusion**: Smart combination of explicit and learned features
- **Flexible Channels**: Configurable AlphaEarth embedding dimensions
- **Skip Connections**: DEM-based skip connections preserve topographic detail
- **Multitask Learning**: Joint optimization of related hydrologic tasks

## Considerations

### Computational

- **Memory Usage**: AlphaEarth's 64 channels increase memory requirements
- **Training Time**: Two encoder branches require more computation
- **Data Requirements**: Need both DEM and AlphaEarth processed patches

### Model Behavior

- **Feature Redundancy**: Some topographic info may be duplicated between modalities
- **Embedding Interpretability**: AlphaEarth features are less interpretable than DEM
- **Normalization**: Different preprocessing needs (DEM normalized, AlphaEarth raw)

## Comparison with Other Variants

### vs. DEM-only
- **Advantage**: Rich contextual information beyond just elevation
- **Trade-off**: Higher complexity and computational cost

### vs. AlphaEarth-only  
- **Advantage**: Explicit topographic structure and interpretability
- **Trade-off**: Additional data requirements and processing

### vs. DEM + Optical
- **Advantage**: Foundation model embeddings vs. raw spectral bands
- **Trade-off**: Less control over feature learning process

### vs. DEM + SAR
- **Advantage**: Broader contextual information vs. weather-independence
- **Trade-off**: Weather/cloud dependency through foundation model training

### vs. DEM + Thermal
- **Advantage**: Comprehensive Earth patterns vs. specific thermal signatures
- **Trade-off**: Less direct hydrologic information

## Performance Optimization

### Hyperparameter Tuning

- **AlphaEarth Channels**: Start with 64, reduce to 32 if memory-constrained
- **Base Channels**: Adjust model capacity (32, 64, 128)
- **Learning Rate**: May need different rates for DEM vs. AlphaEarth branches
- **Loss Weighting**: Balance water segmentation vs. flow direction importance

### Training Tips

- **Batch Size**: Reduce if running into memory issues with 64-channel AlphaEarth
- **Gradient Monitoring**: Check both encoder branches for gradient flow
- **Attention Analysis**: Visualize which modality dominates in different regions
- **Ablation Studies**: Compare with single-modality versions

## Future Directions

### Research Opportunities

- **Attention Analysis**: Study when/where each modality contributes most
- **Channel Optimization**: Find optimal AlphaEarth embedding dimensionality
- **Fusion Strategies**: Explore alternatives to hierarchical attention
- **Multi-scale**: Incorporate multiple DEM resolutions
- **Temporal**: Add time-series AlphaEarth embeddings

### Extensions

- **Additional Tasks**: Extend to other hydrologic predictions
- **Transfer Learning**: Pre-train on larger datasets
- **Uncertainty**: Add uncertainty quantification
- **Real-time**: Optimize for deployment scenarios

## Related Files

- **Model**: `models/mdmt_dem_alphaearth.py`
- **Data Loader**: `data/patchDataLoader_dem_alphaearth.py`
- **Example**: `example_dem_alphaearth.py`
- **Training**: Create `training/run_lightning_train_dem_alphaearth.py`

## Citation

When using this model, please cite the broader National ML project and acknowledge the use of both DEM data sources and Google's AlphaEarth embeddings.