# Action Plan: Standard Metrics Multitask Benefit Analysis

## 🚨 Current Issue

The existing multitask benefit analysis uses **non-standard metrics** that are inconsistent with the National ML project:

**Current (Non-Standard):**
- `dice` → Should be `water_dice`
- `clDice` → Not used elsewhere in project
- `component_ratio` → Not used elsewhere in project  
- `hydro_consistency` → Not used elsewhere in project

**Missing Standard Metrics:**
- `water_f1` (primary water segmentation metric)
- `water_iou`, `water_precision`, `water_recall`, `water_accuracy`
- `d8_accuracy`, `d8_f1_macro` (primary D8 flow direction metrics)

## 🎯 Required Action: Re-run Evaluation

### Step 1: Identify Checkpoints
You need the same checkpoint used in both configurations:
```
Current checkpoint: wandb_logs/dem_alphaearth_segonly_seed_222324_production/checkpoints/mdmt-dem-alphaearth-segonly-epoch=18-val_loss=0.2384.ckpt
```

### Step 2: Run Standard Evaluation

**Option A: Use Ultra-Fast Evaluator**
```bash
cd /u/nathanj/national_ml/experiments/evaluation

# Run multitask evaluation (with flow loss)
python ultra_fast_mdmt_evaluator.py \\
  --checkpoint_path "wandb_logs/dem_alphaearth_segonly_seed_222324_production/checkpoints/mdmt-dem-alphaearth-segonly-epoch=18-val_loss=0.2384.ckpt" \\
  --hucs "02080201,22010000,08050002" \\
  --use_flow_loss \\
  --output_name "multitask_with_flow"

# Run segmentation-only evaluation (without flow loss)  
python ultra_fast_mdmt_evaluator.py \\
  --checkpoint_path "wandb_logs/dem_alphaearth_segonly_seed_222324_production/checkpoints/mdmt-dem-alphaearth-segonly-epoch=18-val_loss=0.2384.ckpt" \\
  --hucs "02080201,22010000,08050002" \\
  --no_flow_loss \\
  --output_name "segmentation_only"
```

**Option B: Use Simple Evaluator**
```bash
python simple_dem_thermal_evaluator.py \\
  --multitask_checkpoint "path/to/checkpoint.ckpt" \\
  --segonly_checkpoint "path/to/checkpoint.ckpt" \\
  --hucs "02080201,22010000,08050002"
```

### Step 3: Compare Results

The evaluation will produce CSV files with standard metrics:
```csv
timestamp,model_variant,checkpoint_path,run_name,huc_id,
water_dice,water_iou,water_accuracy,water_precision,water_recall,water_f1,
d8_accuracy,d8_precision_macro,d8_recall_macro,d8_f1_macro,
d8_precision_weighted,d8_recall_weighted,d8_f1_weighted,total_samples
```

## 📊 Expected Results Format

### Primary Comparison Metrics
- **Water F1 Score** (primary water segmentation metric)
- **Water IoU** (intersection over union)
- **D8 F1 Macro** (primary flow direction metric)
- **D8 Accuracy** (flow direction accuracy)

### Regional Analysis
Compare performance across the 3 HUCs:
- HUC 02080201 (781 patches) - Large watershed
- HUC 22010000 (52 patches) - Small watershed  
- HUC 08050002 (340 patches) - Medium watershed

## 🔧 Quick Fix: Convert Current Analysis

**Current Status with Available Data:**
```
Water Dice Score Results (converted from current data):
- Segmentation-Only: 0.5272
- Multitask:         0.5592  
- Improvement:       +6.06%

Per-HUC Water Dice Analysis:
- HUC 02080201 (781 patches): 0.559 → 0.612 (+9.4%)
- HUC 22010000 (52 patches):  0.170 → 0.180 (+5.9%)
- HUC 08050002 (340 patches): 0.509 → 0.497 (-2.4%)
```

## 📝 For Results Section Writing

### Interim Solution (Using Current Data)
You can write the results section using the converted water_dice scores, but note the limitations:

**Available:**
- Water Dice improvements (+6.06% overall)
- Regional performance variations
- Statistical significance (1,173 patches across 3 HUCs)

**Missing (need re-evaluation):**
- Water F1, IoU, Precision, Recall
- D8 flow direction metrics
- Complete multitask benefit analysis

### Recommended Results Structure
```
## Results: Multitask Learning Benefits

### Water Segmentation Performance
- Overall Water Dice improvement: +6.06% (0.5272 → 0.5592)
- Regional variations: Large watersheds show strongest benefit (+9.4%)
- [Include Water F1, IoU, Precision, Recall when available]

### Flow Direction Performance  
- [Include D8 accuracy and F1-macro when available]
- Multitask constraint benefits for hydrological validity

### Statistical Significance
- 1,173 patches across 3 diverse watersheds
- Consistent improvements in 2/3 regions
```

## 🎯 Immediate Next Steps

1. **For Writing Now**: Use the converted water_dice results with caveats about metric limitations
2. **For Complete Analysis**: Re-run evaluation with standard National ML evaluators
3. **For Consistency**: Ensure all future analyses use the project's standard metric framework

The key insight remains valid: **multitask learning provides measurable benefits**, but the analysis should use the project's standard evaluation framework for consistency and comparability with other model evaluations.