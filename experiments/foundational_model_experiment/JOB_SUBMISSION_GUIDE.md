# Foundation Model Job Submission Guide

This document explains how to use the SLURM job submission scripts for all foundation models (Prithvi, Clay, DOFA, and SatLas).

## 📋 Overview

Each foundation model has two scripts following the same pattern:
- `submit_[model]_jobs.sh` - Helper script with argument parsing
- `submit_train_[model]_array.sh` - SLURM array job script

## 🚀 Quick Start

### Submit All Models (Recommended)
```bash
cd /u/nathanj/national_ml/experiments/foundational_model_experiment
./submit_all_foundation_models.sh
```

### Submit Individual Models
```bash
# Navigate to specific model directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/[model]

# Submit array jobs
./submit_[model]_jobs.sh
```

## 📊 Script Options

### Individual Model Scripts
Each model script supports the same options:

```bash
./submit_[model]_jobs.sh [OPTIONS]

Options:
  --runs NUM        Number of array jobs to submit (default: 5)
  --concurrent NUM  Number of concurrent jobs (default: 3)
  --help, -h        Show help message

Examples:
  ./submit_clay_jobs.sh                    # 5 runs, 3 concurrent
  ./submit_dofa_jobs.sh --runs 10          # 10 runs, 3 concurrent
  ./submit_satlas_jobs.sh --runs 3 --concurrent 1  # 3 runs, 1 at a time
```

### Master Submission Script
The master script can submit multiple models at once:

```bash
./submit_all_foundation_models.sh [OPTIONS]

Options:
  --runs NUM        Number of runs per model (default: 5)
  --concurrent NUM  Concurrent jobs per model (default: 3)
  --models LIST     Comma-separated models (default: prithvi,clay,dofa,satlas)
  --help, -h        Show help message

Examples:
  ./submit_all_foundation_models.sh                      # All models, 5 runs each
  ./submit_all_foundation_models.sh --runs 10            # All models, 10 runs each
  ./submit_all_foundation_models.sh --models prithvi,clay # Only Prithvi and Clay
```

## 🔧 SLURM Configuration

All models use the same SLURM resource configuration:
- **Account**: `bcrm-tgirails`
- **Partition**: `gpu`
- **GPUs**: 1 per job
- **CPUs**: 16 cores per job
- **Memory**: 128GB per job
- **Time Limit**: 72 hours
- **Environment**: `terratorch_env` conda environment

## 📁 Directory Structure

```
foundational_model_experiment/
├── submit_all_foundation_models.sh  # Master submission script
├── prithvi/
│   ├── submit_prithvi_jobs.sh
│   ├── submit_train_prithvi_array.sh
│   └── slurm_logs/
├── clay/
│   ├── submit_clay_jobs.sh
│   ├── submit_train_clay_array.sh
│   └── slurm_logs/
├── dofa/
│   ├── submit_dofa_jobs.sh
│   ├── submit_train_dofa_array.sh
│   └── slurm_logs/
└── satlas/
    ├── submit_satlas_jobs.sh
    ├── submit_train_satlas_array.sh
    └── slurm_logs/
```

## 📋 Monitoring Jobs

### Check Job Status
```bash
# All array jobs
squeue -u $USER --array

# Specific model jobs
squeue -u $USER | grep -E "(prithvi|clay|dofa|satlas)"

# Specific job ID
squeue -j JOB_ID
```

### Check Logs
```bash
# Navigate to model directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/[model]

# View logs
ls slurm_logs/
tail -f slurm_logs/[model]-9ch-array_JOBID_TASKID.out
tail -f slurm_logs/[model]-9ch-array_JOBID_TASKID.err
```

### Monitor on WandB
Each model logs to its own WandB project:
- **Prithvi**: `prithvi_water_segmentation`
- **Clay**: `clay_water_segmentation` 
- **DOFA**: `dofa_water_segmentation`
- **SatLas**: `satlas_water_segmentation`

## 🎯 Training Configuration

Each model uses 9-channel multimodal input:
- **Channel 0**: DEM (Digital Elevation Model)
- **Channels 1-6**: Optical bands (6×Landsat/Sentinel-2)
- **Channel 7**: Thermal band
- **Channel 8**: SAR backscatter

Training parameters:
- **Batch Size**: 4 (optimized for GPU memory)
- **Precision**: Mixed precision (bf16)
- **Input Channels**: 9
- **Foundation Model**: Pre-trained weights with intelligent adaptation

## 🔍 Troubleshooting

### Common Issues

1. **Missing config file**
   ```bash
   ❌ Error: Please run this script from the [model] experiment directory
   ```
   Solution: Navigate to the correct model directory before running

2. **SLURM not available**
   ```bash
   ❌ Error: SLURM (sbatch) not found
   ```
   Solution: Run on a SLURM cluster node

3. **Failed job submission**
   - Check SLURM account/partition access
   - Verify resource availability
   - Check conda environment activation

### Debug Mode
To test without submitting jobs, check the scripts manually:
```bash
# Dry run - check script syntax
bash -n submit_clay_jobs.sh

# View generated command
cat submit_train_clay_array.sh
```

## 📊 Expected Outputs

### Successful Submission
```
🚀 Clay Foundation Model Array Submission
==========================================
📊 Submission Configuration:
   Number of runs: 5
   Concurrent jobs: 3
   Array specification: 1-5%3

🎯 Submitting Clay array job...
✅ Successfully submitted job array!
   Job ID: 12345678
   Array: 1-5%3

📋 Monitor your jobs:
   squeue -u $USER --array
   squeue -j 12345678
```

### Job Execution
Each array task will output detailed information:
```
======================================================
Starting Clay Foundation Model 9-Channel Training
======================================================
Array Job ID: 12345678
Array Task ID: 3
Input Channels: 9 (DEM + 6×Optical + Thermal + SAR)
Foundation Model: clay_foundation_model
Pre-trained Weights: Enabled with intelligent adaptation
WandB Run Name: clay_9ch_20251005_143022_run3
======================================================
```

## 🎉 Ready to Submit!

All scripts are ready for production use. Choose your preferred submission method:

1. **Individual models**: Use model-specific scripts for focused experiments
2. **All models**: Use master script for comprehensive comparison
3. **Custom combinations**: Use master script with `--models` option

The scripts handle resource management, logging, and monitoring automatically, following the established pattern from Prithvi.