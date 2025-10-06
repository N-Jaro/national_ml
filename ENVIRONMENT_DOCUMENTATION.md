# PyTorch GPU CUDA 11.8 Environment Documentation

**Environment Name:** `pytorch_gpu_cu118`  
**Created:** October 3, 2025  
**Purpose:** Fixed environment for national_ml data processing pipeline  
**Status:** ✅ Working and tested

## Problem Description

The original `pytorch_gpu_cu118` environment became corrupted due to:

1. **GLIBC version mismatch** - Packages built against newer GLIBC than system supports
2. **PyTorch version conflict** - Upgraded to 2.6.0+cu124 causing compatibility issues
3. **Rasterio library errors** - Shared library dependencies incompatible with system

### Error Symptoms
```
ImportError: /sw/spack/v1/apps/gcc/11.4.0-gcc-8.5.0-d6zlqss/lib64/libstdc++.so.6: 
version `GLIBCXX_3.4.30' not found (required by libgdal.so.36)
```

## Solution Implementation

### Core Strategy
1. Complete environment rebuild with compatible versions
2. Use Python 3.10 (more stable than 3.11)  
3. Install geospatial packages from conda-forge
4. Pin critical package versions

### Key Package Versions

| Package | Version | Channel | Notes |
|---------|---------|---------|-------|
| Python | 3.10.18 | conda-forge | Stable base |
| PyTorch | 2.5.1 | pytorch | CUDA 11.8 compatible |
| torchvision | 0.20.1 | pytorch | Matches PyTorch |
| torchaudio | 2.5.1 | pytorch | Matches PyTorch |
| rasterio | 1.4.3 | conda-forge | Geospatial I/O |
| geopandas | 1.1.1 | conda-forge | Spatial data |
| pandas | 2.3.3 | conda-forge | Data analysis |
| numpy | 2.2.6 | conda-forge | Numerical computing |
| earthengine-api | 1.6.10 | pip | Google Earth Engine |

## Environment Files

### 1. Complete Conda Environment Export
**File:** `pytorch_gpu_cu118_environment.yml`
```bash
# Recreate exact environment
conda env create -f pytorch_gpu_cu118_environment.yml
```

### 2. Pip Requirements
**File:** `pytorch_gpu_cu118_pip_requirements.txt`  
```bash
# Install pip packages only
pip install -r pytorch_gpu_cu118_pip_requirements.txt
```

### 3. Detailed Package List
**File:** `pytorch_gpu_cu118_conda_list.txt`
- Complete package manifest with build numbers and channels
- Useful for debugging and verification

### 4. Automated Setup Script  
**File:** `setup_pytorch_gpu_cu118_environment.sh`
```bash
# Automated recreation of environment
./setup_pytorch_gpu_cu118_environment.sh
```

## Verification Tests

### Basic Import Test
```python
import torch
import rasterio 
import geopandas
import pandas
import numpy
import earthengine
print("✅ All packages working!")
```

### Pipeline Test
```bash
cd /u/nathanj/national_ml/data/script
python rerun_pipeline.py --help
python rerun_pipeline.py --hucs 19010207 --dry-run
```

## Hardware Compatibility

### System Requirements
- **OS:** Linux (tested on CentOS/RHEL)
- **GLIBC:** 2.28+ (system has 2.28)
- **CUDA:** 11.8+ (for GPU nodes)
- **Memory:** 8GB+ recommended

### SLURM Cluster Usage
- Environment works with existing SLURM job scripts
- CUDA available on GPU nodes (not head node)
- Compatible with `sbatch` submission system

## Troubleshooting

### Common Issues

#### CUDA Not Available on Head Node
- **Expected behavior** - CUDA only available on GPU compute nodes
- Test CUDA availability in SLURM jobs, not interactive sessions

#### Import Errors
```bash
# Check environment activation
conda activate pytorch_gpu_cu118
which python  # Should show environment path

# Verify package installation  
conda list | grep rasterio
python -c "import rasterio; print('OK')"
```

#### Version Conflicts
```bash
# Check for package conflicts
conda list --explicit
pip check
```

## Maintenance

### Environment Updates
- **Avoid automatic updates** - pins maintain stability
- Test updates in separate environment first
- Update documentation when changes made

### Backup Strategy
1. Regular exports: `conda env export > backup_YYYY-MM-DD.yml`
2. Version control environment files
3. Test restoration procedures periodically

## Usage Instructions

### Daily Usage
```bash
# Activate environment
conda activate pytorch_gpu_cu118

# Verify activation
which python
python --version  # Should show 3.10.18

# Run national_ml pipeline
cd /u/nathanj/national_ml/data/script
python rerun_pipeline.py --hucs [HUC_LIST]
```

### SLURM Job Submission
```bash
# Environment should work with existing job scripts
sbatch submit_jobs_1_1.sh
sbatch submit_jobs_2_1.sh
# etc.
```

## Environment Recreation

### Method 1: From YAML (Recommended)
```bash
conda env create -f pytorch_gpu_cu118_environment.yml
```

### Method 2: Automated Script
```bash
./setup_pytorch_gpu_cu118_environment.sh
```

### Method 3: Manual (for debugging)
```bash
conda create -n pytorch_gpu_cu118 python=3.10 -y
conda activate pytorch_gpu_cu118
# Follow setup_pytorch_gpu_cu118_environment.sh steps
```

## Contact & Support

**Environment Fixed By:** GitHub Copilot  
**Date Fixed:** October 3, 2025  
**Tested With:** national_ml rerun_pipeline.py  
**Documentation:** This file + environment exports

For issues:
1. Check this documentation first
2. Verify environment files are present
3. Try recreation from scratch using setup script
4. Compare `conda list` output with working state