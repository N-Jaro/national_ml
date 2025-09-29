# Pipeline Rerun Tool

This tool provides intelligent rerun capabilities for the GEE data processing pipeline. Instead of simply checking for folder existence, it verifies that all required output files exist and are non-empty.

## Problem Solved

The original `run_pipeline.py` had an issue where:
- Stage 2 would create the patch folder when generating `normalization_stats.json`
- Stage 3 would then be skipped because the folder existed, even though patch files didn't exist
- This resulted in incomplete processing

## Solution

The new `rerun_pipeline.py` checks for specific output files rather than just folders:

### Stage 1 (HUC Processing) - Complete when ALL exist:
- `huc8_{huc_id}_boundary.geojson`
- `huc8_{huc_id}_center_points.csv`
- `huc8_{huc_id}_center_points_features.geojson`
- `huc8_{huc_id}_dem.tif`

### Stage 2 (Stats Processing) - Complete when exists:
- `normalization_stats.json`

### Stage 3 (Patch Processing) - Complete when ALL exist:
- At least 5 `patch_*.npz` files
- At least 1 `patch_*_georef_template.tif` file

### Stage 4 (Local Reference Processing) - Complete when ALL exist:
- `flow_direction.tif`
- `hydro_mask.tif`

## Usage

### Basic Usage
```bash
# Analyze and run only incomplete stages
python rerun_pipeline.py --hucs 19010207

# Process multiple HUCs
python rerun_pipeline.py --hucs 19010207 10020007 03160113
```

### Dry Run (Analysis Only)
```bash
# See what would be executed without running anything
python rerun_pipeline.py --hucs 19010207 --dry-run
```

### Force Specific Stages
```bash
# Force rerun stage 3 (patches) even if it appears complete
python rerun_pipeline.py --hucs 19010207 --force 3

# Force rerun multiple stages
python rerun_pipeline.py --hucs 19010207 --force 1 3 4

# Force rerun all stages
python rerun_pipeline.py --hucs 19010207 --force 1 2 3 4
```

### Using the Shell Script
```bash
# Make it executable (only needed once)
chmod +x run_rerun_pipeline.sh

# Run with the shell script
./run_rerun_pipeline.sh --hucs 19010207 --dry-run
./run_rerun_pipeline.sh --hucs 19010207 --force 3
```

## Command Line Arguments

- `--hucs`: **(Required)** List of HUC8 IDs to process
- `--force`: **(Optional)** Force rerun specific stages (1, 2, 3, 4)
- `--dry-run`: **(Optional)** Analyze what would be run without executing

## Stage Dependencies

The tool respects stage dependencies:
- **Stage 2** requires Stage 1 to be complete
- **Stage 3** requires Stage 1 to be complete
- **Stage 4** requires Stage 3 to be complete

If a prerequisite stage is incomplete, the dependent stage will be skipped with a warning message.

## Output

### Analysis Phase
The tool first analyzes each HUC and reports:
```
--- Analyzing HUC 19010207 ---
  Stage 1 (HUC processing): ✓ Complete
  Stage 2 (Stats): ✗ Incomplete
    Missing: normalization_stats.json
  Stage 3 (Patches): ✓ Complete
  Stage 4 (Local ref): ✓ Complete
```

### Execution Plan
Before running, it shows what will be executed:
```
=== EXECUTION PLAN ===
Stage 1 (HUC processing): 0 HUCs - []
Stage 2 (Stats): 1 HUCs - ['19010207']
Stage 3 (Patches): 0 HUCs - []
Stage 4 (Local ref): 0 HUCs - []
```

### Final Status Report
After completion, it provides a final status:
```
=== FINAL STATUS REPORT ===

--- HUC 19010207 ---
  Stage 1: ✓ Complete
  Stage 2: ✓ Complete
  Stage 3: ✓ Complete
  Stage 4: ✓ Complete
  Overall: ✓ ALL STAGES COMPLETE
```

## Updated Original Pipeline

The original `run_pipeline.py` has also been updated to use the same file-based checking logic, fixing the Stage 3 skipping issue.

## Examples

### Common Scenarios

1. **Process a new HUC that failed partway through:**
   ```bash
   python rerun_pipeline.py --hucs 19010207
   ```

2. **Reprocess patches for a HUC where Stage 3 was skipped:**
   ```bash
   python rerun_pipeline.py --hucs 19010207 --force 3
   ```

3. **Check status of multiple HUCs without running:**
   ```bash
   python rerun_pipeline.py --hucs 19010207 10020007 03160113 --dry-run
   ```

4. **Rerun everything for a HUC (complete reprocessing):**
   ```bash
   python rerun_pipeline.py --hucs 19010207 --force 1 2 3 4
   ```

## Error Handling

The tool includes comprehensive error handling:
- Skips HUCs with missing prerequisite files
- Reports specific missing files
- Continues processing other HUCs even if one fails
- Provides detailed error messages for debugging

## File Location

- Main script: `/u/nathanj/national_ml/data/script/rerun_pipeline.py`
- Shell wrapper: `/u/nathanj/national_ml/data/script/run_rerun_pipeline.sh`
- Updated original: `/u/nathanj/national_ml/data/script/run_pipeline.py`