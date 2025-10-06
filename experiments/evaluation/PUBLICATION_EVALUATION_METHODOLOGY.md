# MDMT Model Evaluation Methodology for Multi-Domain Hydrological Prediction

## Abstract

This document presents the comprehensive evaluation methodology for Multi-Domain Multi-Task (MDMT) models applied to hydrological prediction tasks. Our evaluation framework assesses model performance across two primary tasks: water body segmentation and D8 flow direction prediction. The evaluation encompasses six distinct model variants incorporating different Earth observation data modalities, evaluated across multiple experimental runs to ensure statistical robustness.

## 1. Introduction

### 1.1 Problem Statement

Accurate hydrological modeling requires the integration of multiple Earth observation data sources and the simultaneous prediction of complementary hydrological features. Our MDMT framework addresses this challenge by jointly learning water body segmentation and D8 flow direction prediction across diverse input modalities.

### 1.2 Evaluation Objectives

1. **Multi-Task Performance Assessment**: Quantify model performance on both water segmentation and flow direction prediction tasks
2. **Cross-Modal Comparison**: Compare effectiveness of different Earth observation data combinations
3. **Statistical Robustness**: Evaluate model consistency across multiple independent training runs
4. **Spatial Generalization**: Assess model performance across diverse hydrological catchments

## 2. Model Variants and Data Modalities

### 2.1 MDMT Model Variants

Our evaluation encompasses six distinct model variants, each incorporating different combinations of Earth observation data to assess the contribution of different modalities and their interactions:

| Variant | Input Modalities | Spectral Bands | Native Resolution | Final Resolution | Temporal Coverage |
|---------|------------------|-----------------|-------------------|------------------|-------------------|
| **AlphaEarth** | AlphaEarth Foundation Model Features | 64 channels | 10m | 10m | Multi-temporal |
| **DEM+Thermal** | Digital Elevation Model + Landsat Thermal | DEM + Band 10,11 | 10m + 30m | 10m | Single temporal |
| **DEM+SAR** | Digital Elevation Model + Sentinel-1 SAR | DEM + VV,VH | 10m + 10m | 10m | Single temporal |
| **DEM+Optical** | Digital Elevation Model + Landsat 9 Optical | DEM + Bands 2-7 | 10m + 30m | 10m | Single temporal |
| **DEM+AlphaEarth** | Digital Elevation Model + AlphaEarth Features | DEM + 64 channels | 10m + 10m | 10m | Multi-temporal |
| **Landsat6B** | Multi-Modal (DEM+Optical+SAR+Thermal) | DEM + Bands 2-7 + VV,VH + Bands 10,11 | 10m + 30m + 10m + 30m | 10m | Single temporal |

### 2.2 Input Data Specifications

- **Spatial Extent**: 224×224 pixel patches at 10m resolution (2.24km × 2.24km coverage)
- **Digital Elevation Model**: USGS National Elevation Dataset, processed to 10m resolution
- **Landsat 9**: Collection 2, Level-2 surface reflectance bands 2-7 (30m native) and thermal products bands 10-11 (30m native), resampled to 10m using bilinear interpolation
- **Sentinel-1**: Ground Range Detected (GRD) products, dual-polarization (VV+VH), 10m native resolution
- **AlphaEarth**: Pre-trained foundation model features (64 channels) from multi-temporal Landsat composites, processed to 10m resolution

**Multi-Resolution Processing**: The integration of data from multiple native resolutions (10m DEM/SAR/AlphaEarth, 30m Landsat optical/thermal) required careful co-registration and resampling to the common 10m grid to preserve spatial relationships while maximizing information content from higher-resolution sources.

**Variant Design Strategy**: The model variants follow a systematic design to assess modality contributions:
- **Single Modality**: AlphaEarth (foundation model features only)
- **Dual Modality**: DEM+Thermal, DEM+SAR, DEM+Optical (topography + single remote sensing type)
- **Multi-Modal**: DEM+AlphaEarth (topography + foundation model), Landsat6B (comprehensive traditional sensors)

The **Landsat6B** variant serves as a comprehensive baseline combining all traditional Earth observation modalities (DEM + optical + SAR + thermal) without foundation model features, enabling direct comparison with the AlphaEarth-based approaches.

## 3. Ground Truth Data and Labels

### 3.1 Water Body Segmentation Labels

Water body ground truth derived from high-resolution hydrographic datasets:
- **Source**: National Hydrography Dataset Plus (NHDPlus HR)
- **Resolution**: Vector data rasterized to 10m grid
- **Classes**: Binary classification (water/non-water)
- **Validation**: Cross-referenced with Landsat water indices (NDWI, MNDWI)

### 3.2 D8 Flow Direction Labels

Flow direction ground truth computed using D8 algorithm on high-resolution DEMs:
- **Source**: USGS National Elevation Dataset, processed to 10m resolution
- **Algorithm**: Deterministic 8-direction (D8) flow routing
- **Classes**: 8 discrete flow directions plus undefined/flat areas
- **Encoding**: Powers of 2 (1,2,4,8,16,32,64,128) plus 255 for undefined areas
- **Preprocessing**: Pit filling and flow enforcement applied

### 3.3 Spatial Sampling Strategy

- **Coverage**: Continental United States (CONUS)
- **Sampling**: Hydrologic Unit Code (HUC) based stratified sampling
- **Test Set**: 15 geographically diverse HUCs representing different climatic and topographic conditions
- **Patch Generation**: Non-overlapping 224×224 pixel patches within each HUC boundary

## 4. Model Architecture and Training

### 4.1 MDMT Architecture

The MDMT model employs a shared encoder-decoder architecture with task-specific prediction heads:

- **Encoder**: ResNet-based feature extraction with multi-scale feature fusion
- **Decoder**: U-Net style upsampling with skip connections
- **Task Heads**: 
  - Water Segmentation: Single-channel sigmoid output
  - D8 Flow Direction: 9-channel softmax output (8 directions + undefined)

### 4.2 Training Configuration

- **Framework**: PyTorch Lightning
- **Optimizer**: AdamW with cosine annealing learning rate schedule
- **Loss Function**: Combined cross-entropy (D8) and binary cross-entropy (water) with task weighting
- **Batch Size**: 32 patches per GPU (224×224 patches enable larger batch sizes)
- **Training Duration**: 50 epochs with early stopping
- **Validation**: 20% holdout from training HUCs

### 4.3 Experimental Runs

Each model variant trained with 10 independent runs:
- **Seeds**: Different random seeds for initialization and data shuffling
- **Hardware**: NVIDIA A100 GPUs, 40GB memory
- **Training Time**: ~6-12 hours per run depending on data modality
- **Checkpointing**: Best model based on validation loss preservation

## 5. Evaluation Methodology

### 5.1 Test Dataset Configuration

- **Geographic Scope**: 15 independent HUC watersheds
- **Patch Count**: Variable per HUC (range: 45-2,847 patches)
- **Total Patches**: 14,847 evaluation patches across all HUCs
- **Data Loading**: PyTorch DataLoader with batch processing for computational efficiency

### 5.2 Evaluation Pipeline

#### 5.2.1 Individual Model Evaluation

Each trained model checkpoint evaluated independently:

```python
# Pseudocode for individual evaluation
for huc in test_hucs:
    patches = load_huc_patches(huc)
    for batch in batches(patches):
        predictions = model.forward(batch)
        water_pred = torch.sigmoid(predictions['water'])
        d8_pred = torch.softmax(predictions['d8'], dim=1)
        
        # Accumulate predictions and ground truth
        accumulate_results(water_pred, d8_pred, batch['labels'])
    
    # Compute HUC-level metrics
    huc_metrics = compute_metrics(accumulated_predictions, accumulated_labels)
```

#### 5.2.2 Batch Evaluation System

Automated evaluation across all model variants and runs:
- **Parallelization**: GPU-accelerated batch processing
- **Memory Management**: Efficient tensor operations with gradient disabling
- **Result Aggregation**: HUC-level and overall performance metrics
- **Quality Control**: Automated validation of prediction formats and ranges

### 5.3 Performance Metrics

#### 5.3.1 Water Segmentation Metrics (Focus on Water Class)

All water segmentation metrics focus specifically on water class detection (positive class = 1, water pixels):

**Intersection over Union (IoU)**:
```
IoU = TP / (TP + FP + FN)
```
where TP = correctly identified water pixels, FP = incorrectly predicted water pixels, FN = missed water pixels.

**Dice Coefficient**:
```
Dice = 2×TP / (2×TP + FP + FN)
```
Measures water pixel overlap between prediction and ground truth.

**Precision (Water Detection)**:
```
Precision = TP / (TP + FP)
```
Proportion of predicted water pixels that are actually water.

**Recall (Water Coverage)**:
```
Recall = TP / (TP + FN)
```
Proportion of actual water pixels correctly identified.

**F1-Score (Balanced Water Performance)**:
```
F1 = 2 × (Precision × Recall) / (Precision + Recall)
```
Harmonic mean of precision and recall, providing balanced assessment of water detection quality.

**Overall Accuracy**:
```
Accuracy = (TP + TN) / (TP + TN + FP + FN)
```
Pixel-level accuracy including both water and non-water classification.

All metrics computed at pixel level with sigmoid activation and binary thresholding at 0.5. The focus on water class (positive label) ensures metrics reflect practical water detection performance rather than overall pixel accuracy dominated by non-water areas.

#### 5.3.2 D8 Flow Direction Classification Metrics

**Overall Classification Accuracy**:
```
Accuracy = Correct_Predictions / Total_Predictions
```
Computed using corrected D8 value mapping to ensure proper class-to-value correspondence.

**Multi-Class Performance Metrics**:

**Macro-Averaged Metrics** (unweighted across classes):
- **Precision (Macro)**: Average precision across all 9 D8 flow directions
- **Recall (Macro)**: Average recall across all 9 D8 flow directions  
- **F1-Score (Macro)**: Average F1-score across all 9 D8 flow directions

**Weighted-Averaged Metrics** (weighted by class frequency):
- **Precision (Weighted)**: Class-frequency weighted precision
- **Recall (Weighted)**: Class-frequency weighted recall
- **F1-Score (Weighted)**: Class-frequency weighted F1-score

**Per-Direction Analysis**:
- Individual precision, recall, F1-score for each flow direction (1,2,4,8,16,32,64,128,255)
- Confusion matrix analysis for directional prediction bias assessment
- Spatial correlation analysis of prediction errors with topographic complexity

#### 5.3.3 Spatial Performance Analysis

- **HUC-Level Aggregation**: Mean performance across patches within each watershed
- **Geographic Distribution**: Performance variation across climatic and topographic gradients
- **Patch-Size Effects**: Performance consistency across different patch densities per HUC

## 6. Statistical Analysis Framework

### 6.1 Multi-Run Analysis

Each model variant evaluated across 10 independent training runs:
- **Central Tendency**: Mean and median performance across runs
- **Variability**: Standard deviation and interquartile range
- **Distribution**: Box plots and statistical significance testing

### 6.2 Cross-Variant Comparison

**Performance Ranking**:
- Individual metric rankings (IoU, Dice, D8 Accuracy)
- Composite ranking using average rank across metrics
- Statistical significance testing (paired t-tests, Wilcoxon signed-rank)

**Modality Effectiveness**:
- Information content analysis of different Earth observation inputs
- Correlation analysis between input complexity and performance
- Cost-benefit analysis considering data acquisition requirements

### 6.3 Robustness Assessment

- **Training Stability**: Coefficient of variation across multiple runs
- **Convergence Analysis**: Training curve consistency and final performance distribution
- **Outlier Detection**: Identification and analysis of poorly performing runs

## 7. Quality Assurance and Validation

### 7.1 Metric Validation

**D8 Encoding Verification**:
Critical validation of D8 class-to-value mapping to ensure correct accuracy computation:
```python
# Corrected D8 mapping
d8_class_to_value = {0:1, 1:2, 2:4, 3:8, 4:16, 5:32, 6:64, 7:128, 8:255}
```

**Cross-Validation with External Libraries**:
- Scikit-learn metric verification for consistency
- Synthetic data testing to validate metric implementations

### 7.2 Reproducibility Measures

- **Code Versioning**: Git-based version control for all evaluation scripts
- **Environment Documentation**: Conda environment specifications and dependency management
- **Seed Management**: Deterministic random number generation for reproducible results
- **Checkpoint Preservation**: All model weights and training configurations archived

### 7.3 Data Integrity Checks

- **Input Validation**: Automated verification of patch dimensions and value ranges
- **Label Consistency**: Cross-referencing between water and flow direction labels
- **Missing Data Handling**: Systematic treatment of undefined/masked regions

## 8. Computational Infrastructure

### 8.1 Hardware Specifications

- **GPU**: NVIDIA A100 with 40GB memory
- **CPU**: 32 cores, 128GB RAM per evaluation job
- **Storage**: High-performance filesystem for rapid data access
- **Network**: InfiniBand interconnect for distributed processing

### 8.2 Software Environment

- **Framework**: PyTorch 1.12+ with CUDA 11.8 support
- **Python**: 3.8+ with scientific computing stack (NumPy, SciPy, Pandas)
- **Visualization**: Matplotlib, Seaborn for result analysis
- **Job Management**: SLURM for high-performance computing cluster integration

### 8.3 Performance Optimization

- **Batch Processing**: Optimized tensor operations for GPU utilization
- **Memory Management**: Efficient data loading and caching strategies
- **Parallelization**: Multi-process data loading and GPU-accelerated inference

## 9. Results Reporting Framework

### 9.1 Summary Statistics

**Per-Variant Performance Tables**:
- Mean ± standard deviation across 10 runs
- Best and worst performing runs identification
- 95% confidence intervals for population estimates

**Cross-Variant Comparison Tables**:
- Pairwise significance testing results
- Effect size measurements (Cohen's d)
- Performance ranking with statistical confidence

### 9.2 Visualization Standards

**Performance Distribution Plots**:
- Box plots showing quartiles and outliers across runs
- Violin plots displaying full performance distributions
- Scatter plots for correlation analysis between metrics

**Geographic Performance Maps**:
- HUC-level performance visualization
- Spatial patterns in model effectiveness
- Topographic and climatic correlation analysis

### 9.3 Detailed Analysis Reports

**Per-HUC Performance Breakdown**:
- Individual watershed performance characteristics
- Challenging regions identification and analysis
- Correlation with hydrologic and topographic properties

**Error Analysis**:
- Confusion matrices for D8 flow direction prediction
- False positive/negative analysis for water segmentation
- Spatial patterns in prediction errors

## 10. Limitations and Considerations

### 10.1 Spatial Resolution Considerations

- 10m resolution enables detection of smaller hydrological features compared to traditional 30m approaches
- Edge effects at patch boundaries (2.24km × 2.24km coverage)
- Resampling of 30m Landsat data to 10m grid may introduce interpolation artifacts
- Computational overhead increased due to higher spatial resolution (9× more pixels than 30m analysis)

### 10.2 Temporal Representation

- Single temporal snapshot for most variants
- Seasonal variability not captured in evaluation
- Dynamic hydrological processes not represented

### 10.3 Label Uncertainty

- Ground truth derived from existing datasets with inherent uncertainties
- Potential temporal misalignment between imagery and reference data
- Subjective decisions in water body delineation for complex cases

## 11. Ethical Considerations and Data Use

### 11.1 Data Sources and Licensing

All input data derived from publicly available government datasets:
- USGS data products (public domain)
- NASA/USGS Landsat (open access)
- ESA Sentinel-1 (open access under Copernicus program)

### 11.2 Computational Resource Usage

- Efficient use of shared HPC resources
- Energy consumption considerations for large-scale model training
- Open-source code release for research community benefit

## 12. Conclusions

This evaluation methodology provides a comprehensive framework for assessing MDMT model performance in hydrological prediction tasks. The multi-variant, multi-run experimental design ensures statistical robustness while the diverse metrics capture both pixel-level accuracy and practical utility for hydrological applications.

The systematic evaluation across different Earth observation modalities provides insights into optimal data combinations for hydrological modeling, supporting evidence-based decisions for operational hydrologic prediction systems.

## References and Data Sources

1. **National Hydrography Dataset Plus (NHDPlus HR)**: USGS, EPA
2. **USGS National Elevation Dataset**: U.S. Geological Survey
3. **Landsat Collection 2**: NASA/USGS Landsat Program
4. **Sentinel-1**: European Space Agency Copernicus Program
5. **PyTorch Lightning**: Falcon, W. et al. (2019)
6. **Scikit-learn**: Pedregosa, F. et al. (2011)

---

**Document Version**: 1.0  
**Date**: October 5, 2025  
**Authors**: [To be filled with publication authors]  
**Corresponding Author**: [To be filled]  
**Institution**: [To be filled]  

---

*This document serves as the technical specification for the MDMT evaluation methodology used in our hydrological prediction research. All evaluation code and data processing scripts are available in the accompanying repository for full reproducibility.*