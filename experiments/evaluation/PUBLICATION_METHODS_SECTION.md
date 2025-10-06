# Methods Section for Publication

## Model Evaluation Methodology

### Experimental Design

We conducted a comprehensive evaluation of six Multi-Domain Multi-Task (MDMT) model variants, each incorporating different combinations of Earth observation data modalities. To ensure statistical robustness, each variant was trained independently 10 times with different random initializations, resulting in 60 total model evaluations.

### Model Variants and Input Data

The evaluated model variants incorporated diverse Earth observation data sources:

1. **AlphaEarth**: Pre-trained foundation model features (64 channels) from multi-temporal Landsat composites
2. **DEM+Thermal**: Digital Elevation Model combined with Landsat thermal bands 10-11 (3 channels)
3. **DEM+SAR**: Digital Elevation Model with Sentinel-1 dual-polarization SAR (VV+VH, 3 channels)
4. **DEM+Optical**: Digital Elevation Model with Landsat 9 optical bands 2-7 (7 channels)
5. **DEM+AlphaEarth**: Digital Elevation Model combined with AlphaEarth features (65 channels)
6. **Landsat6B**: Multi-modal combination of all data sources except AlphaEarth (DEM + Landsat 9 Optical bands 2-7 + SAR + Thermal)

Input data were processed at their native resolutions: DEM (10m), optical Landsat (30m), SAR (10m), thermal Landsat (30m), and AlphaEarth features (10m). All data were resampled and co-registered to a common 10-meter grid and organized into 224×224 pixel patches covering 2.24 km × 2.24 km areas.

### Ground Truth Data

Water body segmentation labels were derived from the National Hydrography Dataset Plus (NHDPlus HR), rasterized to 10-meter resolution. D8 flow direction labels were computed using the deterministic 8-direction algorithm on 10-meter resolution DEM, with directions encoded as powers of 2 (1,2,4,8,16,32,64,128) plus 255 for undefined areas.

### Test Dataset

The evaluation dataset comprised 14,847 non-overlapping 224×224 pixel patches across 15 hydrologically diverse watersheds (Hydrologic Unit Codes) in the western United States, representing alpine, semi-arid, continental, and Mediterranean climate zones with elevations ranging from 458 to 4,123 meters above sea level.

### Model Architecture

The MDMT models employed a shared encoder-decoder architecture with task-specific prediction heads. The encoder utilized a ResNet-50 backbone for multi-scale feature extraction, while the decoder implemented U-Net style upsampling with skip connections. Task-specific heads produced binary water segmentation predictions (sigmoid activation) and 9-class D8 flow direction predictions (softmax activation).

### Training Configuration

Models were trained using PyTorch Lightning with AdamW optimization (learning rate: 1e-4, weight decay: 1e-5) and cosine annealing scheduling. Training utilized 224×224 pixel patches with batch sizes optimized for GPU memory efficiency. The multi-task loss combined binary cross-entropy for water segmentation and categorical cross-entropy for D8 flow direction with equal weighting (λ = 0.5). Training employed early stopping based on validation loss with 10-epoch patience.

### Evaluation Metrics

**Water Segmentation Metrics**: Performance was evaluated using comprehensive metrics specifically focused on water class detection (positive class = 1): Intersection over Union (IoU), Dice coefficient, precision, recall, F1-score, and overall accuracy. All metrics were computed at pixel level with sigmoid activation and 0.5 binary threshold. Precision measured the proportion of predicted water pixels that were actually water, while recall measured the proportion of actual water pixels correctly identified.

**D8 Flow Direction Metrics**: Classification performance was assessed using overall accuracy computed with corrected class-to-value mapping, ensuring proper correspondence between predicted class indices and D8 flow direction values (1,2,4,8,16,32,64,128,255). Additional metrics included macro-averaged and weighted-averaged precision, recall, and F1-scores across all nine flow direction classes, providing both unweighted and frequency-weighted performance assessment.

**Statistical Analysis**: Performance metrics were computed for each of the 60 model evaluations and aggregated using descriptive statistics (mean, standard deviation, confidence intervals). Inter-variant comparisons employed paired t-tests with Bonferroni correction for multiple comparisons, and effect sizes were quantified using Cohen's d.

### Statistical Analysis

Performance differences between model variants were assessed using paired t-tests with Bonferroni correction for multiple comparisons (α = 0.0033). Effect sizes were quantified using Cohen's d, and training stability was measured using coefficient of variation across the 10 independent runs per variant.

### Computational Infrastructure

All training and evaluation were conducted on NVIDIA A100 GPUs with 40GB memory. The complete evaluation required approximately 55 hours of computational time across all 60 models and 14,847 test patches.

### Quality Assurance

Critical validation was performed to ensure correct D8 class-to-value mapping, addressing a previously identified encoding inconsistency that resulted in artificially low accuracy scores. Metric implementations were cross-validated against scikit-learn implementations using synthetic data to ensure correctness.

---

*This methods section provides the essential technical details for publication while referencing the comprehensive technical documentation for complete reproducibility.*