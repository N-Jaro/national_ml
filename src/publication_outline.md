# Publication Outline: National ML Study

## 1. Study Area

### Overview
- **Geographic Scope**: Continental United States coverage through HUC8 watershed boundaries
- **Scale**: National-scale hydrologic modeling across 2303 watersheds
- **Purpose**: Development and evaluation of multi-modal deep learning approaches for hydrologic feature extraction
- **Selection Criteria**: Comprehensive coverage of diverse hydrologic conditions, climate zones, and terrain types
- **Research Objectives**: Advance automated hydrologic mapping using satellite remote sensing and machine learning

### Specific Areas
- **Dataset Scale**: 2303 HUC8 (Hydrologic Unit Code) watersheds covering the continental United States
- **Training/Validation Set**: 50 HUC8 watersheds selected for geographic and hydrologic diversity
  - Examples: HUC 04030105 (Peshtigo, WI), HUC 10020007, HUC 19090102, etc.
  - Strategic selection to represent various climate zones, terrain types, and land cover conditions
- **Test Set**: 2253 HUC8 watersheds held-out for final model evaluation
  - Comprehensive geographic coverage across all major US hydrologic regions
  - Ensures robust generalization testing across diverse environmental conditions
- **Total Coverage**: Nearly complete HUC8 coverage of the continental US
- **Spatial Distribution**: From coastal regions to mountainous areas, representing diverse hydrologic regimes

### Data Availability and Constraints
- **Temporal Coverage**: Summer 2023 (July 1-31) for consistent phenological conditions
- **Spatial Resolution**: 10m native resolution with multi-scale processing
- **Geographic Coverage**: Near-complete continental US HUC8 watershed coverage (2303/2318 total HUC8s)
- **Data Constraints**: Cloud cover limitations, DEM availability gaps, temporal consistency requirements
- **Quality Considerations**: Automated quality control for satellite data artifacts and geometric accuracy

## 2. Input Data

### Satellite Imagery Sources
- **Landsat Surface Reflectance**: 
  - Bands: SR_B2 (Blue), SR_B3 (Green), SR_B4 (Red), SR_B5 (NIR), SR_B6 (SWIR1), SR_B7 (SWIR2)
  - Temporal range: 2023-07-01 to 2023-07-31 (summer growing season)
  - Spatial resolution: 30m
  - Cloud masking and atmospheric correction details

- **Sentinel-1 SAR**:
  - Polarization: VV
  - Instrument mode: IW (Interferometric Wide Swath)
  - Temporal range: Matching Landsat period
  - Spatial resolution: 10m
  - Preprocessing: Radiometric calibration, speckle filtering

- **Thermal Data**:
  - Source: Landsat ST_B10 band
  - Spatial resolution: 30m
  - Units: Kelvin (converted from digital numbers)

### Elevation Data
- **Digital Elevation Model (DEM)**:
  - Source: Google Earth Engine DEM collection
  - Spatial resolution: 10m
  - Coordinate reference system: EPSG:5070 (Albers Equal Area)
  - Vertical datum and accuracy specifications

### Advanced Embeddings
- **AlphaEarth Satellite Embeddings**:
  - Source: Google Satellite Embedding V1 Annual collection
  - Year: 2024
  - Feature dimensions: 64-band embeddings
  - Spatial resolution: 10m
  - Pre-trained model details and embedding methodology

### Data Acquisition Pipeline
- Google Earth Engine integration
- Automated download and preprocessing workflows
- Quality control and data validation steps
- Storage and organization in local cache

## 3. Reference Data

### Hydrographic Data Sources
- **National Hydrography Dataset (NHD)**:
  - Version: NHDPlus High Resolution
  - Components: Flowlines, waterbodies, non-network features
  - Geographic coverage: National, clipped to study HUCs
  - Attribute completeness and accuracy

- **NHD GeoDatabase**:
  - File: NHDPlus_H_National_Release_2.gdb
  - Layers processed: NetworkNHDFlowline, NHDWaterbody, NonNetworkNHDFlowline
  - Coordinate systems and projection details

### Derived Reference Layers
- **Flow Direction**:
  - Methodology: D8 flow direction algorithm using PySheds
  - Input: DEM data
  - Output: 8-directional flow routing
  - Resolution matching: Aligned with DEM (10m)

- **Hydrographic Mask**:
  - Creation process: Rasterization of NHD features
  - Buffering: 15m buffer on flowlines for connectivity
  - Binary classification: Water (1) vs non-water (0)
  - Spatial alignment with satellite data

### Reference Data Processing
- Automated extraction and processing pipeline
- Quality assurance and validation
- Integration with satellite-derived features
- Temporal consistency considerations

## 4. Model Data Preparation

### Patch Generation Strategy
- **Center Point Sampling**:
  - Method: Systematic grid-based sampling within HUC boundaries
  - Density: 626 center points for HUC 04030105 (example)
  - Spatial distribution: Uniform coverage with boundary constraints

- **Patch Extraction**:
  - Patch size: 224×224 pixels (configurable)
  - Multi-resolution handling: Consistent 10m output resolution
  - Coordinate reference system standardization
  - Overlap and edge handling

### Data Sources Integration
- **Multi-modal Data Fusion**:
  - DEM: Elevation values
  - Optical: 6-band surface reflectance
  - Thermal: Single-band temperature
  - SAR: Single-band backscatter
  - AlphaEarth: 64-band embeddings

- **Normalization and Scaling**:
  - Per-HUC statistics calculation using Google Earth Engine
  - Z-score normalization formula: $z = \frac{x - \mu}{\sigma}$
    - Where $x$ is the raw pixel value, $\mu$ is the mean, and $\sigma$ is the standard deviation
    - Statistics computed per modality and band within each HUC boundary
    - Memory-efficient sampling approach for large watersheds
  - Handling of missing data across modalities
  - Robust fallback strategies for statistical computation failures

### Quality Control and Filtering
- **Data Completeness Checks**:
  - Minimum data requirements per patch
  - Cloud cover thresholds
  - Geometric accuracy validation
  - Outlier detection and removal

- **Patch Filtering**:
  - DEM availability requirement for reference data generation
  - Result: 410 valid patches from 626 center points (example for HUC 04030105)
  - Quality metrics for patch acceptance

### Methodology Validation
- **Reference Data Accuracy**: Assessment of NHD data quality
- **Ground Truth Verification**: If available, comparison with field measurements
- **Cross-Validation with Established Methods**: Comparison with traditional hydrologic models
- **Sensitivity Analysis**: Parameter robustness testing

### Data Organization
- **Storage Format**: Compressed NumPy (.npz) files
- **Directory Structure**: Organized by HUC and patch index
- **Metadata**: Georeference templates and coordinate information
- **Version Control**: Dataset versioning and reproducibility
- **Data Splitting**: 
  - Training and validation: 50 HUC8 watersheds
  - Testing: 2253 HUC8 watersheds
  - Total dataset: 2303 HUC8 regions
  - Spatial stratification to ensure geographic diversity

### Computational Requirements
- **Hardware Specifications**: GPU models, CPU cores, memory requirements
- **Training Time**: Per-epoch timing and total training duration
- **Inference Time**: Prediction speed for real-time applications
- **Storage Requirements**: Dataset size and model checkpoint sizes

### Software Environment
- **Python Version**: 3.x specification
- **Key Libraries**: PyTorch, Lightning, GeoPandas, Rasterio versions
- **Google Earth Engine API**: Version and authentication requirements
- **System Dependencies**: GDAL, PROJ, and other geospatial libraries

## 5. Model Creation

### Model Architecture Overview
- **Multi-Modal Deep Learning Framework**:
  - Base architecture: U-Net with encoder-decoder structure
  - Multi-head attention mechanisms for cross-modal fusion
  - Hierarchical attention fusion for modality integration
  - Skip connections for multi-scale feature preservation

- **Architecture Choices**:
  - **U-Net Selection**: Chosen for pixel-level segmentation tasks requiring precise spatial localization
    - Encoder-decoder structure captures both local and global context
    - Skip connections preserve fine-grained spatial details lost in downsampling
    - Proven effectiveness in remote sensing and medical image segmentation
  - **Multi-Modal Fusion Strategy**: Hierarchical attention mechanism prioritizes elevation data
    - DEM designated as primary modality due to its fundamental role in hydrologic processes
    - Attention weights computed between primary (DEM) and secondary modalities
    - Element-wise multiplication applies learned attention masks
  - **Modality-Specific Encoders**: Separate convolutional encoders for each input modality
    - Allows learning specialized feature representations per data source
    - Maintains modality-specific characteristics before fusion
  - **Shared Representation**: Fused features processed through shared shallow encoder
    - Enables cross-modal knowledge transfer
    - Reduces parameter count while maintaining representational capacity

### Model Variants
- **DEM-Only Model (mdmt_dem_only.py)**:
  - Input: Single elevation channel (1 × 224 × 224)
  - Architecture: Simplified U-Net encoder-decoder
  - Output: Hydrologic predictions from topographic features alone
  - Purpose: Baseline topographic-only performance evaluation

- **DEM + Optical Model (mdmt_dem_optical.py)**:
  - Inputs: DEM (1 channel) + Landsat optical (6 channels) = 7 total channels
  - Fusion strategy: Early concatenation with hierarchical attention
  - Enhanced feature representation combining topography and land cover
  - Applications: Improved surface water and vegetation mapping

- **DEM + SAR Model (mdmt_dem_sar.py)**:
  - Inputs: DEM (1 channel) + Sentinel-1 SAR (1 channel) = 2 total channels
  - Fusion: Multi-modal attention for radar-topography integration
  - All-weather capability emphasis for cloud-prone regions
  - Applications: Enhanced flood mapping and surface roughness characterization

- **DEM + Thermal Model (mdmt_dem_thermal.py)**:
  - Inputs: DEM (1 channel) + Thermal (1 channel) = 2 total channels
  - Fusion: Temperature-elevation feature combination
  - Focus: Hydrologic process modeling (evaporation, groundwater discharge)
  - Applications: Thermal anomaly detection and water temperature mapping

- **Full Multi-Modal Model (mdmt_v1.py)**:
  - Inputs: All available modalities (DEM:1 + optical:6 + thermal:1 + SAR:1 + AlphaEarth:64) = 73 total channels
  - Architecture: Hierarchical fusion with modality-specific encoders
  - Fusion Process:
    - Separate U-Net encoders for each modality (4-layer downsampling)
    - Hierarchical attention fusion with DEM as primary modality
    - Attention computation: $\alpha_i = \sigma(W \cdot [f_{dem}, f_i])$
    - Attended features: $f_i^{att} = f_i \odot \alpha_i$
    - Final fusion: $[f_{dem}, f_{opt}^{att}, f_{therm}^{att}, f_{sar}^{att}, f_{embed}^{att}]$
  - Comprehensive feature integration across all available data sources

### Technical Implementation Details
- **Framework**: PyTorch Lightning for scalable training
- **Backbone Components**:
  - DoubleConv blocks: Two convolutional layers with BatchNorm and ReLU
  - Down blocks: MaxPool2d + DoubleConv for encoder path
  - Up blocks: Bilinear upsampling + DoubleConv for decoder path
  - Channel progression: 64 → 128 → 256 → 512 → 1024 (encoder)
- **Attention Mechanism**:
  - Context features: Concatenation of primary and secondary modality features
  - Attention network: 1×1 convolutions with sigmoid activation
  - Output: Attention weights matching secondary modality channels
- **Loss Functions**: Custom hydrologic loss functions combining:
  - Binary cross-entropy for hydrographic mask prediction
  - Weighted loss for flow direction classification
  - Spatial consistency regularization terms
- **Regularization**: 
  - Dropout (0.1-0.2) in decoder blocks
  - Batch normalization after each convolution
  - Weight decay (1e-4) in AdamW optimizer

## 6. Training Strategy and Evaluation

### Data Loading and Augmentation
- **DataLoader Classes**:
  - patchDataLoader.py: Base multi-modal loader
  - Specialized loaders for different modality combinations
  - Memory-efficient batch processing

- **Data Modules**:
  - LightningDataModule implementations
  - Train/validation/test splits: 50 HUC8 (train/val) / 2253 HUC8 (test)
  - Spatial cross-validation within training regions
  - Patch-level batching with HUC stratification

### Training Configuration
- **Hyperparameters**:
  - **Batch Size**: 32 samples per batch (optimized for GPU memory and training stability)
  - **Learning Rate**: 1e-4 for single-modality models, 1e-5 for multi-modal models (with cosine annealing scheduling)
  - **Epochs**: Maximum 100-500 epochs with early stopping (patience: 20 epochs based on validation loss)
  - **Precision**: Mixed precision training (16-bit floating point) for computational efficiency
  - **Optimizer**: AdamW with weight decay (1e-4) for regularization and generalization
  - **Loss Weighting**: Dynamic uncertainty weighting for multi-task learning (automatically learned)
  - **Data Loading**: 16 CPU workers for efficient data preprocessing and augmentation

- **Loss Functions**:
  - **Multi-Task Loss**: Dynamic uncertainty weighting (Kendall et al., 2018)
    - Formula: $L_{total} = e^{-s_1}L_1 + e^{-s_2}L_2 + s_1 + s_2$
    - Where $s_i$ are learnable log-variances representing task uncertainty
    - Automatically balances water segmentation and flow direction losses
  - **Water Segmentation Loss**: Combined Focal + Dice Loss
    - Focal Loss: $FL(p_t) = -\alpha(1-p_t)^\gamma \log(p_t)$
    - Dice Loss: $DL = 1 - \frac{2\sum yp}{(\sum y) + (\sum p) + \epsilon}$
    - Topology-preserving ClDice component for connected structures
  - **Flow Direction Loss**: Cross-entropy for 8-class D8 classification
    - Classes: 8 flow directions (N, NE, E, SE, S, SW, W, NW)
    - D8 encoding: Powers of 2 (1, 2, 4, 8, 16, 32, 64, 128)

- **Hardware Utilization**:
  - **GPU**: Single NVIDIA GPU (CUDA-enabled) for training acceleration
  - **Memory**: 50-100GB RAM allocation depending on model complexity
  - **CPU**: 16 CPU cores for data loading and preprocessing
  - **Precision**: Mixed precision (FP16) training for 2x speedup and memory efficiency
  - **Job Scheduling**: SLURM workload manager for HPC resource allocation

### Training Scripts
- **Lightning Training**:
  - train_mdmt_lightning.py: Main training script
  - Modality-specific training variants
  - SLURM job submission scripts for HPC

- **Evaluation Metrics**:
  - **Water Segmentation**:
    - Pixel-wise accuracy and IoU (Intersection over Union)
    - Precision, Recall, F1-score for water class
    - Topology metrics: ClDice, Skeleton Dice for connectivity preservation
  - **Flow Direction**:
    - Overall accuracy and class-wise F1 scores
    - Directional consistency metrics
    - Spatial agreement indices with reference DEM-derived flow
  - **Multi-modal Contribution Analysis**:
    - Ablation studies measuring performance drop without each modality
    - Attention weight visualization and interpretation
    - Feature importance analysis using SHAP or similar methods

### Model Evaluation Strategy
- **Validation Approach**:
  - Single train/validation split within the 50 training HUC8s (PyTorch Lightning automatic split)
  - Cross-HUC generalization testing on 2253 test watersheds
  - Early stopping based on validation loss to prevent overfitting
  - Robust evaluation across diverse hydrologic conditions

- **Performance Metrics**:
  - Pixel-wise accuracy for hydrographic masks
  - Flow direction prediction accuracy
  - Multi-class classification metrics
  - Spatial agreement indices

### Experimental Design
- **Ablation Studies**:
  - Modality importance analysis
  - Architecture component evaluation
  - Data resolution sensitivity
  - Loss function component analysis

- **Comparative Analysis**:
  - Baseline methods: Traditional hydrologic models, single-modality DL approaches
  - State-of-the-art remote sensing methods
  - Commercial software comparisons (e.g., ArcGIS, ENVI)
  - Computational efficiency assessment

- **Hyperparameter Optimization**:
  - **Search Strategy**: Manual tuning based on literature review and empirical testing
  - **Batch Size Selection (32)**: 
    - Balances GPU memory efficiency with gradient stability
    - Large enough for reliable batch normalization statistics
    - Small enough to fit multi-modal inputs (73 channels) within GPU memory constraints
    - Common practice in remote sensing segmentation tasks
  - **Learning Rate Selection**: 
    - 1e-4 for single-modality models: Provides stable convergence for simpler architectures
    - 1e-5 for multi-modal models: Lower rate needed for complex parameter spaces (73 input channels)
    - Cosine annealing scheduling: Gradual learning rate decay for better generalization
    - Prevents overshooting in high-dimensional feature spaces
  - **Early Stopping Patience (20 epochs)**: 
    - Allows sufficient training time for convergence on large datasets
    - Prevents overfitting while permitting model adaptation to diverse hydrologic patterns
    - Balances training thoroughness with computational efficiency
  - **Mixed Precision (FP16)**: 
    - Reduces memory footprint by 50% for larger batch processing
    - 2x training speedup on modern GPUs with minimal accuracy loss
    - Essential for handling high-dimensional multi-modal inputs
  - **AdamW Optimizer with Weight Decay (1e-4)**: 
    - Superior generalization compared to Adam for regularization
    - Weight decay prevents overfitting in high-capacity models
    - Well-suited for transformer-like architectures with attention mechanisms
  - **CPU Workers (16)**: 
    - Matches SLURM job allocation for efficient data preprocessing
    - Prevents GPU starvation during I/O intensive patch loading
    - Optimized for the 224×224 pixel patch size and multi-modal data access
  - **Validation**: Hyperparameter choices validated through train/validation split on training HUC8s

### Cross-Validation Strategy
- **Spatial Cross-Validation**: Avoiding spatial autocorrelation bias
- **HUC-based Splitting**: 
  - Training set: 50 HUC8 watersheds (geographically diverse selection)
  - Validation set: 10% of patches randomly split from training HUC8s (41,541 training + 4,615 validation patches)
  - Test set: 2253 HUC8 watersheds (held-out regions for final evaluation)
  - Total coverage: 2303 HUC8 regions across continental US
- **Temporal Validation**: If applicable for time-series components
- **K-fold Cross-Validation**: Not implemented; single train/validation split used with early stopping

### Results Analysis
- **Visualization**:
  - Prediction maps and overlays
  - Error analysis and spatial patterns
  - Feature importance heatmaps
  - Attention mechanism visualizations

- **Performance Comparison Tables**:
  - Method vs Metric matrices
  - Statistical significance indicators
  - Computational complexity comparisons
  - Scalability assessments

- **Feature Importance Analysis**:
  - SHAP (SHapley Additive exPlanations) values
  - Permutation importance
  - Attention weight analysis
  - Modality contribution breakdown

- **Spatial Analysis**:
  - Geographic performance variation
  - Error spatial autocorrelation
  - Terrain complexity correlations
  - Land cover type performance differences

- **Statistical Validation**:
  - Confidence intervals and significance testing
  - Performance across different hydrologic regimes (2253 test HUC8s)
  - Generalization to unseen HUCs with comprehensive geographic coverage
  - Robust evaluation on large-scale held-out test set

### Uncertainty Quantification
- **Model Confidence**: Prediction probability analysis
- **Spatial Uncertainty**: Pixel-wise uncertainty maps
- **Modality Reliability**: Confidence scores per input modality
- **Ensemble Methods**: If applicable for uncertainty estimation

### Limitations and Future Work
- **Data Limitations**: Coverage gaps, temporal constraints, resolution trade-offs
- **Model Limitations**: Computational complexity, interpretability challenges
- **Geographic Limitations**: Applicability to different climates/ecosystems
- **Future Directions**: Multi-temporal modeling, higher resolution data, real-time applications

### Data Availability and Reproducibility
- **Code Repository**: GitHub link and version information
- **Data Access**: Instructions for accessing NHD and satellite data
- **Preprocessing Scripts**: Availability of data preparation pipelines
- **Model Checkpoints**: Pre-trained model availability
- **Documentation**: Comprehensive usage instructions

### Ethical Considerations
- **Data Privacy**: Use of publicly available geospatial data
- **Environmental Impact**: Computational resource usage considerations
- **Bias Assessment**: Geographic and temporal bias analysis
- **Responsible AI**: Transparency in hydrologic modeling applications

## 7. Discussion and Conclusions

### Key Findings
- **Performance Achievements**: Quantitative results summary
- **Modality Contributions**: Which data sources provided most value
- **Geographic Generalizability**: Robust evaluation across 2253 held-out HUC8 test regions

### Scientific Contributions
- **Methodological Advances**: Novel aspects of the approach
- **Hydrologic Modeling Implications**: Impact on water resource management
- **Remote Sensing Applications**: Broader applicability to earth observation

### Practical Implications
- **Operational Feasibility**: Deployment considerations for water management agencies
- **Cost-Benefit Analysis**: Computational vs traditional method trade-offs
- **Scalability Assessment**: Application to continental-scale analysis (2303 HUC8 regions)

### Limitations and Future Research
- **Current Constraints**: Data, computational, and methodological limitations
- **Research Directions**: Multi-temporal analysis, higher resolution modeling
- **Technology Integration**: Combining with existing hydrologic workflows