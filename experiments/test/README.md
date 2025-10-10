# MDMT Model Testing Suite

This directory contains test scripts for validating the MultiModal MultiTask (MDMT) model variants in the National ML project.

## Test Scripts Overview

### Model Architecture Tests
These scripts test the forward pass, parameter counts, and basic functionality of each MDMT variant:

- **`test_all_modalities_alphaearth.py`** - All Modalities + AlphaEarth (73 channels)
- **`test_dem_optical.py`** - DEM + Optical (7 channels) 
- **`test_dem_thermal.py`** - DEM + Thermal (2 channels)
- **`test_dem_sar.py`** - DEM + SAR (2 channels)
- **`test_dem_alphaearth.py`** - DEM + AlphaEarth (65 channels)
- **`test_alphaearth_only.py`** - AlphaEarth Only (64 channels)

## Usage

### Quick Architecture Test (Demo Mode)
Test model architecture with synthetic data only:

```bash
cd /u/nathanj/national_ml/experiments/test
python test_all_modalities_alphaearth.py --demo_only
```

### Full Test Suite
Test architecture + data loading + end-to-end with real data (if available):

```bash
python test_all_modalities_alphaearth.py
```

### Run All Tests
```bash
# Test all model variants
for test_script in test_*.py; do
    echo "Running $test_script..."
    python "$test_script" --demo_only
done
```

## Test Categories

### 1. Architecture Demo (`--demo_only`)
- ✅ Model instantiation
- ✅ Forward pass with synthetic data
- ✅ Output shape validation
- ✅ Parameter count and memory usage
- ⚡ Fast execution (no real data required)

### 2. Data Loading Test
- ✅ Dataset creation from patch files
- ✅ Sample loading and validation
- ✅ DataLoader functionality
- ⚠️ Requires processed patch data

### 3. End-to-End Test
- ✅ Real data forward pass
- ✅ Prediction validation
- ✅ Model-data compatibility
- ⚠️ Requires processed patch data

## Expected Output

### Success ✅
```
🚀 Testing [Model Variant] MDMT Model
================================================================================
[VARIANT] MDMT MODEL ARCHITECTURE DEMO
================================================================================
✅ Architecture test PASSED
🎉 [Model] model is ready for training!
```

### Failure ❌
```
❌ Architecture test FAILED: [error message]
❌ Data loading test failed: [error message]
❌ End-to-end test failed: [error message]
```

## Model Specifications

| Model Variant | Input Channels | Memory (MB) | Parameters | Use Case |
|---------------|----------------|-------------|------------|----------|
| All Modalities + AlphaEarth | 73 | ~763 | ~200M | Research/Benchmarking |
| DEM + AlphaEarth | 65 | ~763 | ~200M | Foundation + Topography |
| AlphaEarth Only | 64 | ~763 | ~200M | Foundation Model Only |
| DEM + Optical | 7 | ~176 | ~41M | General Purpose |
| DEM + Thermal | 2 | ~176 | ~41M | Temperature Analysis |
| DEM + SAR | 2 | ~176 | ~41M | Weather Independent |

## Integration with Training

After successful tests, use corresponding training scripts:

```bash
# Example: Train All Modalities + AlphaEarth
cd /u/nathanj/national_ml/experiments/training
sbatch submit_train_all_modalities_alphaearth_single.sh
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're in the correct conda environment
   ```bash
   conda activate pytorch_gpu_cu118
   ```

2. **CUDA Not Available**: Tests will run on CPU automatically
   ```bash
   # Check CUDA availability
   python -c "import torch; print(torch.cuda.is_available())"
   ```

3. **Missing Patch Data**: Use `--demo_only` for architecture-only testing
   ```bash
   python test_[variant].py --demo_only
   ```

4. **Memory Issues**: Tests use small batch sizes by default
   - Architecture tests: batch_size=2
   - Real data tests: batch_size=2, num_workers=0

## Development Workflow

1. **New Model Variant**: Create corresponding test script
2. **Architecture Changes**: Run architecture tests first
3. **Data Pipeline Changes**: Run data loading tests
4. **Training Issues**: Use end-to-end tests for debugging

The test suite provides a comprehensive validation framework for all MDMT model variants, ensuring reliability before expensive training runs.