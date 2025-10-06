# Landsat6B Ultra-Fast Evaluator - Incremental Output & wandb Integration

## Summary of Changes

The Landsat6B ultra-fast evaluator has been enhanced with incremental CSV output and comprehensive wandb logging as requested. Here are the key modifications:

## ✅ Completed Features

### 1. Incremental CSV Output
- **New Function**: `append_to_csv(csv_path, huc_results)`
  - Writes individual HUC results immediately after evaluation
  - Creates CSV header if file doesn't exist
  - Prevents data loss during long-running evaluations
  - Thread-safe writing with proper file handling

### 2. wandb Integration
- **New Function**: `setup_wandb(project_name, run_name)`
  - Initializes wandb with project-specific naming
  - Sets up run with descriptive metadata
  - Returns wandb table for metric logging
  - Handles wandb configuration and authentication

### 3. Enhanced Evaluation Loop
- **Modified Function**: `fast_evaluate_all_hucs()`
  - Added `run_name` and `use_wandb` parameters
  - Creates timestamped output directories
  - Initializes wandb with project "mdmt_landsat6b_evaluation"
  - Creates wandb table with columns: ["huc_id", "water_f1", "water_precision", "water_recall", "d8_f1_macro", "d8_f1_weighted", "d8_accuracy", "total_samples"]
  - Logs individual HUC metrics to wandb in real-time
  - Appends results to CSV after each HUC evaluation
  - Calculates and logs final summary statistics
  - Properly closes wandb run with `wandb.finish()`

### 4. Run Name Extraction
- **Modified Function**: `main()`
  - Extracts run name from checkpoint filename
  - Removes file extensions (.ckpt, .pth)
  - Passes run_name to evaluation function
  - Enables wandb logging by default

## 🎯 User Requirements Met

✅ **wandb project per variant**: "mdmt_landsat6b_evaluation"  
✅ **wandb run per model run**: Uses extracted run_name from checkpoint  
✅ **run_name matches CSV filename**: Both use same extracted name  
✅ **wandb table with one row per HUC**: All metrics logged per HUC_ID  
✅ **Incremental CSV output**: Real-time writing prevents data loss  

## 📊 Monitoring Capabilities

### Real-time Metrics
- Per-HUC water detection F1, precision, recall
- Per-HUC D8 flow direction F1-macro, F1-weighted, accuracy
- Total sample counts per HUC
- Processing progress and timing

### wandb Dashboard Features
- Project-level organization by variant
- Run-level organization by model checkpoint
- Interactive tables with sortable metrics
- Progress tracking with percentage completion
- Summary statistics and averages

## 🚀 Usage Example

```bash
python ultra_fast_landsat6b_evaluator.py \
    --checkpoint /path/to/landsat6B_run1_epoch_10.ckpt \
    --huc-list test_huc_list.txt \
    --device auto
```

This will:
1. Extract run_name as "landsat6B_run1_epoch_10"
2. Create CSV: `results/landsat6B_run1_epoch_10_YYYYMMDD_HHMMSS.csv`
3. Initialize wandb run: "landsat6B_run1_epoch_10"
4. Log metrics incrementally to both CSV and wandb
5. Provide real-time progress monitoring

## 🔧 Technical Implementation

### Dependencies Added
- `wandb`: For experiment tracking and monitoring
- Enhanced error handling for file operations
- Timestamped output directories for organization

### Performance Considerations
- Minimal overhead from CSV appending
- Efficient wandb logging with batch operations
- GPU synchronization maintained for accurate timing
- Memory-efficient processing maintained

## 🧪 Testing

A test script `test_landsat6b_incremental.py` has been created to validate:
- Incremental CSV output functionality
- wandb logging integration
- Run name extraction
- Error handling and cleanup

## 📁 Output Structure

```
experiments/evaluation/results/
├── landsat6B_run1_epoch_10_20241220_143022.csv
├── landsat6B_run2_epoch_15_20241220_144512.csv
└── landsat6B_run3_final_20241220_150203.csv
```

Each CSV contains:
- huc_id, water_f1, water_precision, water_recall
- d8_f1_macro, d8_f1_weighted, d8_accuracy, total_samples

## 🔄 Next Steps

1. **Apply to Other Variants**: Extend incremental output and wandb logging to remaining 6 MDMT variants
2. **Batch Testing**: Test with full HUC list (207 HUCs)
3. **SLURM Integration**: Update batch scripts to support wandb logging
4. **Dashboard Setup**: Configure wandb dashboards for comprehensive monitoring

The Landsat6B evaluator is now ready for production use with comprehensive monitoring and data safety features!