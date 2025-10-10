# Hardware and Software Environment - Section 3.3

## 3.3 Hardware and Software Environment

### 3.3.1 Computing Infrastructure

All experiments were conducted on the National Center for Supercomputing Applications (NCSA) Rails cluster, utilizing GPU-accelerated compute nodes with the following specifications:

**Compute Nodes:**
- **Architecture:** Intel Xeon Gold 6426Y processors (2 sockets, 16 cores per socket, 64 total cores)
- **Memory:** 2.06 TB RAM per node (206 GB available per GPU)
- **GPU:** NVIDIA H100 Tensor Core GPUs (8 GPUs per node)
- **Operating System:** Red Hat Enterprise Linux 8.8 (Ootpa)
- **SLURM Workload Manager:** Batch job scheduling and resource management

**Resource Allocation:**
- **Training Jobs:** Single GPU, 16 CPU cores, 100-200 GB RAM, 12-48 hour wall time
- **Evaluation Jobs:** Array jobs with 2 concurrent tasks, single GPU per task
- **Storage:** High-performance parallel file system for dataset access

### 3.3.2 Software Environment

**Python Environment:**
- **Python:** 3.10.18
- **CUDA:** 11.8.89 with NVIDIA CUDA Toolkit
- **Environment Management:** Conda (Miniconda3)

**Core Deep Learning Framework:**
- **PyTorch:** 2.5.1 with CUDA 11.8 support
- **TorchVision:** 0.20.1
- **PyTorch Lightning:** 2.5.5 (high-level training framework)
- **Lightning Utilities:** 0.15.2

**Foundation Model Framework:**
- **TerraTorch:** 1.0 (Microsoft AI for Good foundation model framework)
- **TorchGeo:** 0.6.2 (geospatial datasets and transforms)

**Scientific Computing Libraries:**
- **NumPy:** 2.2.6 (numerical computing)
- **Pandas:** 2.3.3 (data manipulation and analysis)  
- **Scikit-learn:** 1.7.2 (machine learning utilities)
- **Scikit-image:** 0.25.2 (image processing)

**Geospatial Libraries:**
- **Rasterio:** 1.4.3 (raster data I/O)
- **GDAL:** 3.8+ (geospatial data abstraction library)
- **GeoPandas:** 1.1.1 (geospatial data manipulation)

**Model Evaluation and Monitoring:**
- **TorchMetrics:** 1.8.2 (metric computation)
- **Weights & Biases (wandb):** 0.22.1 (experiment tracking)
- **Segmentation Models PyTorch:** 0.5.0 (segmentation architectures)

**Additional Libraries:**
- **Affine:** 2.4.0 (affine transformations for geospatial data)
- **Click:** 8.3.0 (command-line interface creation)
- **Matplotlib/Seaborn:** Visualization libraries
- **BLAS:** Intel MKL 2.116 (optimized linear algebra)

### 3.3.3 Development Environment

**Version Control and Reproducibility:**
- **Git:** Version control with branch-based development
- **Conda Environment Files:** Pinned dependencies for reproducibility
- **SLURM Scripts:** Standardized job submission and resource management
- **Configuration Management:** Centralized parameter and path management

**Data Processing Pipeline:**
- **Google Earth Engine:** Cloud-based satellite imagery access
- **HUC-based Processing:** Watershed-level data organization
- **Patch-based Training:** 224×224 pixel patches with stride-based sampling
- **Multi-modal Normalization:** Per-HUC statistics for consistent preprocessing

**Parallel Processing:**
- **SLURM Array Jobs:** Concurrent model evaluation across multiple checkpoints
- **Multi-GPU Support:** Distributed training capabilities
- **Batch Processing:** Efficient data loading with configurable batch sizes (16-32)

**Model Checkpointing and Deployment:**
- **Automatic Checkpointing:** Best model preservation based on validation metrics
- **Cross-Platform Compatibility:** CPU/GPU inference support
- **Incremental Evaluation:** Progressive result aggregation during long-running evaluations

This computational infrastructure enabled efficient training of multimodal deep learning models on large-scale geospatial datasets, with typical training times of 12-48 hours per model variant and evaluation completing within 12 hours for 67 watershed test regions.