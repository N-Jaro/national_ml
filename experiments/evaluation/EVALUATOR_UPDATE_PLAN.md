# Ultra-Fast Evaluators Update Plan

## Completed Updates

### ✅ Landsat6B Evaluator (`ultra_fast_landsat6b_evaluator.py`)
- **Status**: FULLY UPDATED and TESTED
- **wandb Project**: "mdmt_landsat6b_evaluation"
- **Features**: Incremental CSV, wandb logging, performance optimizations, run name extraction

### ✅ AlphaEarth-only Evaluator (`ultra_fast_alphaearth_evaluator.py`)
- **Status**: FULLY UPDATED
- **wandb Project**: "mdmt_alphaearth_evaluation"
- **Features**: All new features implemented

### 🔄 DEM-only Evaluator (`ultra_fast_dem_only_evaluator.py`)
- **Status**: PARTIALLY UPDATED (wandb import and helper functions added)
- **wandb Project**: "mdmt_dem_evaluation"
- **Remaining**: Function signature, evaluation loop, main() function

## Pending Updates

### ⏳ DEM+AlphaEarth Evaluator (`ultra_fast_dem_alphaearth_evaluator.py`)
- **wandb Project**: "mdmt_dem_alphaearth_evaluation"
- **Status**: NOT STARTED

### ⏳ DEM+Optical Evaluator (`ultra_fast_dem_optical_evaluator.py`)
- **wandb Project**: "mdmt_dem_optical_evaluation"
- **Status**: NOT STARTED

### ⏳ DEM+SAR Evaluator (`ultra_fast_dem_sar_evaluator.py`)
- **wandb Project**: "mdmt_dem_sar_evaluation"
- **Status**: NOT STARTED

### ⏳ DEM+Thermal Evaluator (`ultra_fast_dem_thermal_evaluator.py`)
- **wandb Project**: "mdmt_dem_thermal_evaluation"
- **Status**: NOT STARTED

### ⏳ Unified MDMT Evaluator (`ultra_fast_mdmt_evaluator.py`)
- **wandb Project**: "mdmt_unified_evaluation"
- **Status**: NOT STARTED

## Update Pattern for Each Evaluator

### 1. Add wandb Import
```python
import wandb
```

### 2. Add Helper Functions (after logging setup)
```python
def append_to_csv(result_dict: Dict[str, Any], csv_path: str):
    # ... incremental CSV writing logic

def setup_wandb(run_name: str, checkpoint_path: str, csv_path: str = None):
    # ... wandb initialization with variant-specific project name
```

### 3. Remove tqdm from Batch Loop
```python
# OLD: for batch_idx, batch in enumerate(tqdm(dataloader, desc=f"Evaluating HUC {huc_id}")):
# NEW: for batch_idx, batch in enumerate(dataloader):
```

### 4. Update Function Signature
```python
def fast_evaluate_all_hucs(checkpoint_path: str, huc_codes: List[str], 
                          test_data_path: str = "/projects/bcrm/nathanj/data/processed/test/patch_dataset",
                          device: str = "cpu", limit_batches: int = None, 
                          run_name: str = "unknown", use_wandb: bool = True) -> List[Dict[str, Any]]:
```

### 5. Replace Evaluation Loop
- Add output directory setup
- Add wandb initialization
- Add incremental CSV writing
- Add wandb table logging
- Add summary statistics

### 6. Update main() Function
- Add run name extraction logic
- Pass new parameters to fast_evaluate_all_hucs()

## Variant-Specific wandb Project Names

| Evaluator | wandb Project Name |
|-----------|-------------------|
| Landsat6B | mdmt_landsat6b_evaluation |
| AlphaEarth-only | mdmt_alphaearth_evaluation |
| DEM-only | mdmt_dem_evaluation |
| DEM+AlphaEarth | mdmt_dem_alphaearth_evaluation |
| DEM+Optical | mdmt_dem_optical_evaluation |
| DEM+SAR | mdmt_dem_sar_evaluation |
| DEM+Thermal | mdmt_dem_thermal_evaluation |
| Unified MDMT | mdmt_unified_evaluation |

## CSV Filename Patterns

- AlphaEarth-only: `alphaearth_{run_name}_{timestamp}.csv`
- DEM-only: `dem_{run_name}_{timestamp}.csv`
- DEM+AlphaEarth: `dem_alphaearth_{run_name}_{timestamp}.csv`
- DEM+Optical: `dem_optical_{run_name}_{timestamp}.csv`
- DEM+SAR: `dem_sar_{run_name}_{timestamp}.csv`
- DEM+Thermal: `dem_thermal_{run_name}_{timestamp}.csv`
- Landsat6B: `landsat6b_{run_name}_{timestamp}.csv`

## Benefits After All Updates Complete

1. **Unified Monitoring**: All variants logged to wandb with consistent structure
2. **Data Safety**: Incremental CSV output prevents data loss
3. **Performance**: Optimized for SLURM batch processing
4. **Consistency**: Matching wandb run names and CSV filenames
5. **Traceability**: Complete metadata tracking across all variants

## Estimated Completion Time

- **Per Evaluator**: ~10-15 minutes of careful editing
- **Total Remaining**: ~60-90 minutes for 5 remaining evaluators
- **Testing**: Additional time needed to verify each variant works

## Testing Strategy

After each update:
1. Test with small HUC list and limited batches
2. Verify CSV file creation and content
3. Check wandb logging and dashboard
4. Confirm run name extraction works correctly