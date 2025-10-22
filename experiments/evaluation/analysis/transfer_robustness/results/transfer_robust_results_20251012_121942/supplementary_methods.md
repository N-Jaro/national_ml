# Supplementary Methods: Transfer and Robustness Analysis Framework

## S1. Detailed Experimental Setup

### S1.1 Model Architecture Specifications

**AlphaEarth-Only Model (AE-only)**
```python
class MultitaskModel_AlphaEarth_Only:
    - Input: AlphaEarth embeddings (64 channels)
    - Encoder: ResNet-based with skip connections
    - Decoder: U-Net architecture with multitask heads
    - Task 1: Binary segmentation (water/non-water)
    - Task 2: 8-class flow direction (D8 system)
    - Parameters: ~12.5M trainable parameters
```

**DEM+AlphaEarth Model (DEM+AE)**
```python
class MultimodalMultitaskModel_DEM_AlphaEarth:
    - Input: DEM (1 channel) + AlphaEarth (64 channels)
    - Fusion: Early fusion with channel concatenation
    - Architecture: Shared encoder with modality-specific preprocessing
    - Parameters: ~13.2M trainable parameters
```

**All-Modalities+AlphaEarth Model**
```python
class MultimodalMultitaskModel_All_Modalities_AlphaEarth:
    - Input: DEM (1) + Optical (6) + Thermal (1) + SAR (1) + AlphaEarth (64)
    - Total Input Channels: 73
    - Fusion Strategy: Channel-wise concatenation with learned weighting
    - Parameters: ~15.8M trainable parameters
```

### S1.2 Training Configuration

**Hyperparameters**
- Batch Size: 16 (distributed across available GPUs)
- Learning Rate: 1e-4 with cosine annealing
- Optimizer: Adam (β1=0.9, β2=0.999, ε=1e-8)
- Weight Decay: 1e-5
- Epochs: 50 with early stopping (patience=10)

**Loss Function**
```python
total_loss = α × BCE_loss(segmentation) + β × CrossEntropy_loss(flow_direction)
where α = 0.6, β = 0.4 (empirically optimized)
```

**Data Augmentation**
- Random horizontal/vertical flips (p=0.5)
- Random rotation (±15 degrees, p=0.3)
- Gaussian noise addition (σ=0.01, p=0.2)
- No geometric transformations (preserves spatial relationships)

### S1.3 Normalization Strategy

Critical for multimodal integration, all modalities use HUC-specific normalization:

**Per-HUC Z-score Normalization**
```python
# For each modality m in HUC h:
normalized_m = (m - mean_m_h) / std_m_h

# Prevents cross-HUC distribution shifts
# Computed from normalization_stats.json files
```

**Modality-Specific Ranges**
- DEM: Elevation values (meters above sea level)
- Optical: Landsat TOA reflectance (0-1 scaled)
- Thermal: Brightness temperature (Kelvin, normalized)
- SAR: VV polarization backscatter (dB, normalized)
- AlphaEarth: Pre-normalized embeddings (-1 to 1)

### S1.4 Geographic Stratification

**HUC Selection Rationale**
Selected HUCs provide geographic diversity while maintaining data quality:

```
HUC 03030005: South Atlantic-Gulf Region
- Terrain: Coastal plains, low relief
- Climate: Humid subtropical
- Land Cover: Mixed forest/agriculture
- Hydrologic Features: Coastal watersheds

HUC 04060102: Great Lakes Region  
- Terrain: Glacial landscape, moderate relief
- Climate: Continental humid
- Land Cover: Agriculture/urban
- Hydrologic Features: Lake tributaries

HUC 07040006: Upper Mississippi Region
- Terrain: Agricultural plains, minimal relief
- Climate: Continental humid
- Land Cover: Intensive agriculture
- Hydrologic Features: Prairie streams

HUC 08020301: Lower Mississippi Region
- Terrain: River delta, very low relief
- Climate: Humid subtropical
- Land Cover: Wetlands/agriculture
- Hydrologic Features: Delta channels

HUC 10270104: Missouri Region
- Terrain: Great Plains, variable relief
- Climate: Semi-arid continental
- Land Cover: Grassland/agriculture
- Hydrologic Features: Prairie streams
```

## S2. Analysis Framework Implementation

### S2.1 Transfer Analysis Pipeline

**Step 1: Model Loading and Validation**
```python
# Load pre-trained checkpoints
ae_model = load_checkpoint("mdmt-alphaearth-only-epoch=28-val_loss=0.8024.ckpt")
dem_ae_model = load_checkpoint("mdmt-dem-alphaearth-epoch=40-val_loss=0.4377.ckpt")

# Validate model compatibility
assert ae_model.input_channels == 64  # AlphaEarth only
assert dem_ae_model.input_channels == 65  # DEM + AlphaEarth
```

**Step 2: Terrain Classification**
```python
def classify_terrain(dem_patch):
    relief = np.max(dem_patch) - np.min(dem_patch)
    if relief < 35.0:
        return "low_relief"
    elif relief > 113.9:
        return "high_relief"
    else:
        return "medium_relief"
```

**Step 3: Comparative Evaluation**
For each patch in each HUC:
1. Load multimodal data (DEM, optical, thermal, SAR, AlphaEarth)
2. Classify terrain type based on DEM relief
3. Evaluate AE-only model with AlphaEarth input only
4. Evaluate DEM+AE model with DEM+AlphaEarth input
5. Record performance metrics by terrain category
6. Aggregate results across all patches

### S2.2 Robustness Analysis Pipeline

**Step 1: Cloud Simulation**
```python
def simulate_cloud_coverage(optical_data, coverage=0.3):
    """
    Simulate realistic cloud coverage through spatial masking
    """
    mask = generate_cloud_mask(optical_data.shape, coverage)
    masked_optical = optical_data.copy()
    masked_optical[mask] = 0.0  # Neutral fill value
    return masked_optical, mask

def generate_cloud_mask(shape, coverage):
    """
    Generate spatially clustered cloud mask using morphological operations
    """
    # Random seed points
    seeds = np.random.random(shape) < (coverage * 0.1)
    
    # Morphological dilation to create clusters
    kernel = np.ones((5, 5), np.uint8)
    clustered = morphological_dilation(seeds, kernel, iterations=2)
    
    # Adjust to target coverage
    current_coverage = np.mean(clustered)
    if current_coverage > 0:
        threshold = np.percentile(np.random.random(shape), 
                                 (1 - coverage/current_coverage) * 100)
        mask = (np.random.random(shape) > threshold) & clustered
    else:
        mask = np.random.random(shape) < coverage
        
    return mask
```

**Step 2: Paired Evaluation**
For each patch:
1. Load complete multimodal data
2. Generate clean baseline prediction
3. Apply cloud simulation to optical channels
4. Generate masked prediction with same model
5. Calculate performance degradation metrics
6. Aggregate across all patches

### S2.3 Statistical Analysis

**Performance Metrics**
```python
def calculate_metrics(predictions, targets):
    # Segmentation metrics
    iou = intersection_over_union(predictions['segmentation'], targets['hydro_mask'])
    f1 = f1_score(predictions['segmentation'], targets['hydro_mask'])
    
    # Flow direction metrics  
    flow_acc = accuracy_score(predictions['flow_direction'], targets['flow_dir'])
    
    return {'hydro_iou': iou, 'hydro_f1': f1, 'flow_accuracy': flow_acc}
```

**Aggregation Strategy**
- Patch-level metrics calculated individually
- Terrain-category aggregation using unweighted means
- Bootstrap confidence intervals (1000 iterations)
- Statistical significance testing via Welch's t-test

## S3. Reproducibility Information

### S3.1 Computational Environment
```yaml
Environment: pytorch_gpu_cu118
Python: 3.11.x
PyTorch: 2.0.x with CUDA 11.8
Key Dependencies:
  - pytorch-lightning: 2.0.x
  - numpy: 1.24.x  
  - matplotlib: 3.7.x
  - opencv-python: 4.8.x
  - rasterio: 1.3.x
```

### S3.2 Hardware Specifications
- GPU: NVIDIA A100 or equivalent (40GB+ VRAM recommended)
- CPU: 16+ cores for data loading
- RAM: 100GB+ for large HUC processing
- Storage: 1TB+ for processed datasets

### S3.3 Data Processing Pipeline
```bash
# Complete pipeline execution
cd data/script/
python rerun_pipeline.py --hucs "03030005,04060102,07040006,08020301,10270104"

# Analysis execution
cd experiments/evaluation/analysis/transfer_robustness/
python run_transfer_robustness.py --hucs "03030005,04060102,07040006,08020301,10270104"
```

### S3.4 Result Management System
- Datetime-based directory structure prevents result overwrites
- Complete provenance tracking via configuration files
- Automatic visualization generation with consistent styling
- JSON and Markdown output formats for downstream analysis

## S4. Validation and Quality Assurance

### S4.1 Model Validation
- Checkpoint validation via forward pass testing
- Input/output dimension verification
- Gradient flow verification during evaluation mode
- Memory usage monitoring for large-scale evaluation

### S4.2 Data Quality Checks
- Missing data detection and handling
- Normalization statistics validation
- Spatial alignment verification across modalities
- Outlier detection in performance metrics

### S4.3 Statistical Validation
- Distribution normality testing (Shapiro-Wilk)
- Homoscedasticity verification (Levene's test)
- Effect size calculation (Cohen's d)
- Multiple comparison correction (Bonferroni)

This supplementary methods section provides the technical depth necessary for full reproducibility while maintaining focus on the key methodological innovations in multimodal transfer and robustness analysis.