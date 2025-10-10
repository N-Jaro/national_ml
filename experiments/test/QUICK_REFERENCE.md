# MDMT Test Infrastructure Quick Reference

## Test Directory Structure
```
experiments/test/
├── README.md                           # Comprehensive documentation
├── run_all_tests.py                    # Python test runner
├── quick_test.sh                       # Bash convenience script
├── test_all_modalities_alphaearth.py   # 73-channel comprehensive model
├── test_dem_alphaearth.py              # 65-channel DEM + AlphaEarth
├── test_alphaearth_only.py             # 64-channel AlphaEarth only
├── test_dem_optical.py                 # 7-channel DEM + Optical
├── test_dem_thermal.py                 # 2-channel DEM + Thermal
└── test_dem_sar.py                     # 2-channel DEM + SAR
```

## Quick Commands

### Individual Model Tests
```bash
cd experiments/test

# Test the comprehensive All Modalities + AlphaEarth model
python test_all_modalities_alphaearth.py --demo_only

# Test any specific variant
python test_dem_optical.py --demo_only
python test_alphaearth_only.py --demo_only
```

### Comprehensive Test Suite
```bash
cd experiments/test

# Run all tests (demo mode)
python run_all_tests.py --demo_only

# Run all tests (full validation)
python run_all_tests.py --full

# Run specific variant
python run_all_tests.py --variant all_modalities_alphaearth --demo_only
```

### Bash Quick Tests
```bash
cd experiments/test

# Test all variants (demo mode)
./quick_test.sh

# Full validation mode
./quick_test.sh full

# Test specific variant
./quick_test.sh all_modalities_alphaearth demo
```

## Model Variants Overview

| Variant | Channels | Modalities | Parameters | Test Script |
|---------|----------|------------|------------|-------------|
| **All Modalities + AlphaEarth** | 73 | DEM + Optical + Thermal + SAR + AlphaEarth | ~200M | `test_all_modalities_alphaearth.py` |
| DEM + AlphaEarth | 65 | DEM + AlphaEarth | ~150M | `test_dem_alphaearth.py` |
| AlphaEarth Only | 64 | AlphaEarth | ~140M | `test_alphaearth_only.py` |
| DEM + Optical | 7 | DEM + Optical | ~80M | `test_dem_optical.py` |
| DEM + Thermal | 2 | DEM + Thermal | ~50M | `test_dem_thermal.py` |
| DEM + SAR | 2 | DEM + SAR | ~50M | `test_dem_sar.py` |

## Test Modes

### Demo Mode (`--demo_only`)
- Architecture validation only
- Synthetic data generation
- Parameter counting
- Memory estimation
- Fast execution (~1-6 seconds per model)

### Full Mode (`--full`)
- Architecture validation
- Data loading tests
- Training step simulation
- Loss computation validation
- Comprehensive error checking
- Longer execution (~30-60 seconds per model)

## Expected Output

### Successful Test
```
✅ PASSED in 3.48s
🎉 Architecture test PASSED!
The comprehensive All Modalities + AlphaEarth model (64-channel) is ready for training!
```

### Test Suite Summary
```
================================================================================
📊 TEST RESULTS SUMMARY
================================================================================
Tests completed: 6
Passed: 6 ✅
Failed: 0 ❌
Success rate: 100.0%
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```
   Solution: Ensure you're running from experiments/test/ directory
   cd /u/nathanj/national_ml/experiments/test
   ```

2. **CUDA Not Available**
   ```
   Expected: Tests run on CPU in demo mode
   Note: GPU will be used automatically if available
   ```

3. **Memory Issues**
   ```
   Solution: Reduce batch size in full mode tests
   Use --demo_only for memory-constrained environments
   ```

## Integration with Training

After successful tests, proceed to training:

```bash
# Move to training directory
cd ../training

# Single run
python run_lightning_train_all_modalities_alphaearth.py --hucs "03030005" --epochs 50

# SLURM submission
sbatch submit_train_all_modalities_alphaearth_single.sh
```

## Development Workflow

1. **Test Architecture**: `./quick_test.sh`
2. **Validate Changes**: `python run_all_tests.py --full`
3. **Train Model**: `cd ../training && sbatch submit_train_[variant]_single.sh`
4. **Monitor Progress**: `tail -f slurm_logs_[variant]/slurm_*.out`

This test infrastructure ensures all MDMT variants are thoroughly validated before training, reducing debugging time and improving development efficiency.