# MDMT Batch Evaluation System

A comprehensive system for evaluating multiple MDMT (Multi-Domain Multi-Task) model variants across multiple experiment runs with automated result aggregation and analysis.

## 🚀 Quick Start

### 1. Populate Run Configurations

First, discover and configure runs for all variants:

```bash
cd /u/nathanj/national_ml/experiments/evaluation

# Discover runs from lightning_logs (automatic)
python discover_runs.py --lightning-logs /path/to/lightning_logs --output-dir run_configs

# Or manually edit configuration files in run_configs/
```

### 2. Run Batch Evaluation

Execute evaluation for all variants and runs:

```bash
# Submit SLURM job for full batch evaluation
sbatch submit_batch_evaluation.sh

# Or run directly (for smaller batches)
python batch_mdmt_evaluator.py --variants alphaearth dem_thermal --max-runs 5
```

### 3. Analyze Results

Generate comprehensive analysis reports:

```bash
python analyze_batch_results.py --results-dir batch_results
```

## 📋 System Components

### Core Scripts

- **`batch_mdmt_evaluator.py`** - Main batch evaluation orchestrator
- **`discover_runs.py`** - Auto-discover experiment runs from lightning_logs
- **`analyze_batch_results.py`** - Generate analysis reports and rankings
- **`submit_batch_evaluation.sh`** - SLURM job submission script
- **`test_batch_system.py`** - System validation and testing

### Individual Evaluators

- `simple_alphaearth_evaluator.py` - AlphaEarth variant evaluation
- `simple_dem_thermal_evaluator.py` - DEM+Thermal variant evaluation  
- `simple_dem_sar_evaluator.py` - DEM+SAR variant evaluation
- `simple_dem_optical_evaluator.py` - DEM+Optical variant evaluation
- `simple_dem_alphaearth_evaluator.py` - DEM+AlphaEarth variant evaluation
- `simple_landsat6b_evaluator.py` - Landsat 6-band variant evaluation

### Configuration Files

Located in `run_configs/`:
- `alphaearth_runs.txt` - AlphaEarth experiment runs
- `dem_thermal_runs.txt` - DEM+Thermal experiment runs
- `dem_sar_runs.txt` - DEM+SAR experiment runs
- `dem_optical_runs.txt` - DEM+Optical experiment runs
- `dem_alphaearth_runs.txt` - DEM+AlphaEarth experiment runs  
- `landsat6b_runs.txt` - Landsat 6-band experiment runs

## 📖 Usage Guide

### Discovering Experiment Runs

The discovery system automatically scans `lightning_logs` directories and creates run configuration files:

```bash
python discover_runs.py \
    --lightning-logs /path/to/lightning_logs \
    --output-dir run_configs \
    --experiments alphaearth_experiment dem_thermal_experiment \
    --max-runs 10
```

Features:
- Automatic experiment detection by pattern matching
- Best checkpoint selection based on lowest validation loss
- Generates properly formatted configuration files
- Handles missing or corrupted experiment directories

### Manual Configuration

Edit configuration files manually using the format:
```
# Comments start with #
run_name|checkpoint_path

# Example:
run_01|/path/to/experiment/version_1/checkpoints/best_model.ckpt
run_02|/path/to/experiment/version_2/checkpoints/best_model.ckpt
```

### Batch Evaluation Options

```bash
python batch_mdmt_evaluator.py [OPTIONS]

Options:
  --variants LIST           Variants to evaluate (default: all 6 variants)
  --config-dir DIR         Directory with run configuration files  
  --output-dir DIR         Output directory for results
  --limit-batches N        Limit batches per HUC (for testing)
  --max-runs N             Maximum runs per variant (default: 10)
  --dry-run                Validate configs without running evaluations

Examples:
  # Evaluate specific variants
  python batch_mdmt_evaluator.py --variants alphaearth dem_thermal
  
  # Test with limited data
  python batch_mdmt_evaluator.py --limit-batches 5 --max-runs 2
  
  # Validate configuration
  python batch_mdmt_evaluator.py --dry-run
```

### SLURM Job Submission

The system includes optimized SLURM job submission:

```bash
sbatch submit_batch_evaluation.sh
```

SLURM Configuration:
- 24-hour time limit
- GPU allocation (A100 preferred)
- 32 CPU cores, 128GB RAM
- Comprehensive logging
- Automatic environment setup

### Results Analysis

The analysis system generates multiple report types:

```bash
python analyze_batch_results.py [OPTIONS]

Options:
  --results-dir DIR        Directory with evaluation results
  --output-dir DIR         Output directory for analysis reports

Generated Reports:
  - Performance comparison plots (box plots, correlations)
  - Overall ranking report (multi-metric ranking)
  - Per-variant summary statistics
  - Best performing runs identification
```

## 📊 Output Structure

### Evaluation Results

```
batch_results/
├── mdmt_batch_evaluation_summary_YYYYMMDD_HHMMSS.csv    # Summary results
├── mdmt_batch_evaluation_detailed_YYYYMMDD_HHMMSS.csv   # Detailed results  
├── alphaearth/
│   ├── run_01_evaluation_results.csv                    # Individual run results
│   ├── run_02_evaluation_results.csv
│   └── ...
├── dem_thermal/
│   ├── run_01_evaluation_results.csv
│   └── ...
└── logs/
    ├── batch_evaluation_YYYYMMDD_HHMMSS.log            # Execution logs
    └── slurm-JOBID.out                                   # SLURM output
```

### Analysis Reports

```
batch_results/
├── mdmt_performance_comparison_YYYYMMDD_HHMMSS.png      # Performance plots
├── mdmt_ranking_report_YYYYMMDD_HHMMSS.csv              # Overall rankings
├── mdmt_variant_summary_YYYYMMDD_HHMMSS.csv             # Variant statistics
└── mdmt_best_runs_YYYYMMDD_HHMMSS.csv                   # Best runs per variant
```

## 🎯 Metrics Evaluated

### Water Segmentation
- **IoU (Intersection over Union)** - Overlap between predicted and ground truth water pixels
- **Dice Score** - Harmonic mean of precision and recall for water segmentation

### D8 Flow Direction  
- **D8 Accuracy** - Exact match accuracy for 8-directional flow prediction
- **D8 Classification Report** - Per-class precision, recall, F1-score

### HUC-Level Analysis
- Performance aggregated by Hydrologic Unit Code (HUC)
- Spatial distribution of model performance
- Error analysis by geographic region

## 🔧 System Requirements

### Environment
- Python 3.8+
- PyTorch 1.12+ with CUDA support
- Lightning framework
- Pandas, NumPy, Matplotlib, Seaborn
- Scikit-learn for metric validation

### Hardware  
- GPU recommended (A100 preferred for large batches)
- 32+ CPU cores for parallel processing
- 128+ GB RAM for large datasets
- Sufficient storage for results (~10GB per full evaluation)

### Data Dependencies
- Test dataset with water and D8 labels
- Model checkpoints for all experiment runs
- Properly structured lightning_logs directories

## 🧪 Testing and Validation

### System Readiness Check
```bash
python test_batch_system.py
```

Validates:
- All core scripts present
- Individual evaluators available  
- Configuration templates ready
- Output directories prepared

### Dry Run Testing
```bash
python batch_mdmt_evaluator.py --dry-run --variants alphaearth dem_thermal
```

### Metric Validation
The system includes comprehensive metric validation:
- Corrected D8 class-to-value mapping
- Scikit-learn metric cross-validation
- Synthetic data testing for accuracy verification

## 📈 Performance Improvements

Recent optimizations include:

### D8 Accuracy Correction
- **Issue**: D8 accuracy was extremely low (0.004-0.023) due to comparing class indices to D8 values
- **Solution**: Implemented proper class-to-value mapping: `{0:1, 1:2, 2:4, 3:8, 4:16, 5:32, 6:64, 7:128, 8:255}`
- **Results**: 18x-200x accuracy improvements across all variants

### Batch Processing Efficiency
- Parallel HUC processing within evaluations
- Efficient checkpoint loading and GPU memory management
- Comprehensive logging and error handling
- Automatic result aggregation and summary generation

## 🚨 Troubleshooting

### Common Issues

1. **Missing Checkpoints**
   ```
   ERROR: Checkpoint not found: /path/to/checkpoint.ckpt
   ```
   - Verify checkpoint paths in configuration files
   - Use `discover_runs.py` to regenerate configurations
   - Check storage permissions and file integrity

2. **GPU Memory Issues**
   ```
   RuntimeError: CUDA out of memory
   ```
   - Reduce batch size in individual evaluators
   - Use `--limit-batches` for testing
   - Ensure no other processes using GPU

3. **Data Loading Errors**
   ```
   FileNotFoundError: Test data not found
   ```
   - Verify test dataset paths in evaluator scripts
   - Check data preprocessing pipeline
   - Ensure proper file permissions

### Debug Mode

Enable detailed logging:
```bash
python batch_mdmt_evaluator.py --variants alphaearth --limit-batches 1 --max-runs 1
```

### Validation Commands

```bash
# Test individual evaluator
python simple_alphaearth_evaluator.py --checkpoint /path/to/checkpoint.ckpt --limit-batches 5

# Validate configuration parsing
python batch_mdmt_evaluator.py --dry-run

# Check system readiness
python test_batch_system.py
```

## 📞 Support

For issues or questions:
1. Check logs in `batch_results/logs/`
2. Run system validation: `python test_batch_system.py`
3. Test with dry run: `--dry-run` flag
4. Verify individual evaluators work independently

## 🎉 Success Stories

After implementing the corrected D8 mapping:

- **AlphaEarth**: D8 accuracy improved from 0.023 to 0.418 (18x improvement)
- **DEM+Thermal**: D8 accuracy improved from 0.004 to 0.831 (200x improvement)  
- **DEM+SAR**: D8 accuracy improved from 0.007 to 0.798 (114x improvement)
- **DEM+Optical**: D8 accuracy improved from 0.006 to 0.849 (142x improvement)
- **DEM+AlphaEarth**: D8 accuracy improved from 0.005 to 0.847 (169x improvement)
- **Landsat 6-band**: D8 accuracy improved from 0.004 to 0.851 (213x improvement)

The batch evaluation system now provides reliable, consistent metrics across all model variants for comprehensive performance analysis.