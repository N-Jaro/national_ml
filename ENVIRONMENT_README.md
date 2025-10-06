# Environment Reproducibility Files

This directory contains all files necessary to reproduce the working `pytorch_gpu_cu118` environment for the national_ml project.

## 📁 Files Overview

| File | Purpose | Usage |
|------|---------|-------|
| `pytorch_gpu_cu118_environment.yml` | Complete conda environment export | `conda env create -f pytorch_gpu_cu118_environment.yml` |
| `pytorch_gpu_cu118_pip_requirements.txt` | Pip package list | `pip install -r pytorch_gpu_cu118_pip_requirements.txt` |
| `pytorch_gpu_cu118_conda_list.txt` | Detailed package manifest | Reference for debugging |
| `setup_pytorch_gpu_cu118_environment.sh` | Automated setup script | `./setup_pytorch_gpu_cu118_environment.sh` |
| `validate_environment.py` | Environment validation | `python validate_environment.py` |
| `ENVIRONMENT_DOCUMENTATION.md` | Complete documentation | Reference guide |

## 🚀 Quick Start

### Option 1: Automated Setup (Recommended)
```bash
./setup_pytorch_gpu_cu118_environment.sh
```

### Option 2: From YAML Export
```bash
conda env create -f pytorch_gpu_cu118_environment.yml
conda activate pytorch_gpu_cu118
```

### Option 3: Manual Recreation
1. Follow steps in `ENVIRONMENT_DOCUMENTATION.md`
2. Validate with `python validate_environment.py`

## ✅ Validation

After setup, always run:
```bash
conda activate pytorch_gpu_cu118
python validate_environment.py
```

Expected output: "🎉 ALL TESTS PASSED!"

## 📋 Environment Summary

- **Python:** 3.10.18
- **PyTorch:** 2.5.1 (CUDA 11.8)
- **Key packages:** rasterio 1.4.3, geopandas 1.1.1, earthengine-api 1.6.10
- **Status:** ✅ Working and tested (Oct 3, 2025)
- **Compatible with:** national_ml data processing pipeline, SLURM cluster

## 🔄 Backup Strategy

Environment files are version controlled. To create new backups:
```bash
conda env export -n pytorch_gpu_cu118 > pytorch_gpu_cu118_backup_$(date +%Y%m%d).yml
pip freeze > pytorch_gpu_cu118_pip_backup_$(date +%Y%m%d).txt
```

## 📞 Support

For issues with environment recreation:
1. Check `ENVIRONMENT_DOCUMENTATION.md` 
2. Run `validate_environment.py` for diagnostics
3. Compare package versions with working state
4. Use automated setup script for clean recreation