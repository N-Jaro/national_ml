# DEM Sanity Analysis - Updated SLURM Scripts

## ✅ **All SLURM Scripts Updated with Standard Pattern**

All DEM sanity analysis SLURM scripts have been updated to use your standard configuration pattern:

```bash
#!/bin/bash
# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for each array job
#SBATCH --gpus=1

# Request a single task (process) per array job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job (increased for 64-channel AlphaEarth inputs)
#SBATCH --mem=200G

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118
```

## 📋 **Updated Scripts**

### 1. **Full Analysis** (`run_dem_sanity_analysis.sh`)
- **Time**: 4:00:00
- **Job Name**: `dem_sanity_full`
- **HUCs**: 67 (complete test set)
- **Purpose**: Maximum statistical confidence

### 2. **Quick Representative** (`run_dem_sanity_quick.sh`) ⭐ **Recommended**
- **Time**: 1:00:00
- **Job Name**: `dem_sanity_quick`
- **HUCs**: 10 (representative geographic sample)
- **Purpose**: Publication-ready results

### 3. **Development** (`run_dem_sanity_dev.sh`)
- **Time**: 0:30:00
- **Job Name**: `dem_sanity_dev`
- **HUCs**: 3 (minimal set)
- **Purpose**: Quick validation and debugging

## 🚀 **Ready to Submit**

All scripts are now standardized and ready for submission:

```bash
# Recommended for publication
sbatch run_dem_sanity_quick.sh

# OR for quick testing
sbatch run_dem_sanity_dev.sh

# OR for maximum confidence
sbatch run_dem_sanity_analysis.sh
```

## 📊 **Expected Outputs**

Each job will generate:
- **Overall results CSV**: Summary across variants
- **Per-HUC results CSV**: Individual HUC performance for each variant
- **HUC summary statistics CSV**: Statistical analysis per HUC
- **Comparison visualization**: Publication-ready plot showing performance drops

## 🎯 **Key Benefits of Standardization**

✅ **Consistent resource allocation** (200G memory, 16 cores, 1 GPU)
✅ **Proper account and partition** (`bcrm-tgirails`, `gpu`)
✅ **Standardized conda environment loading**
✅ **Compatible with your existing SLURM infrastructure**

The preview already showed perfect results - now you can run the full analysis with confidence! 🎉