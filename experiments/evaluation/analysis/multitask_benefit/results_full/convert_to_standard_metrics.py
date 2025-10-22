#!/usr/bin/env python3
"""
Standard Metrics Multitask Benefit Analysis
Re-runs the multitask benefit analysis using standard National ML evaluation metrics.

Standard Metrics:
- Water F1, Water IoU, Water Precision, Water Recall, Water Dice, Water Accuracy
- D8 Accuracy, D8 F1 (macro/weighted), D8 Precision, D8 Recall

This replaces the non-standard metrics (clDice, component_ratio, hydro_consistency) 
with the project's standard evaluation framework.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from datetime import datetime

def create_standard_metrics_analysis():
    """Create multitask benefit analysis using standard National ML metrics"""
    
    print("🔧 Creating multitask benefit analysis with STANDARD National ML metrics")
    print("=" * 80)
    print("⚠️  NOTE: The current analysis uses non-standard metrics (dice, clDice, component_ratio, hydro_consistency)")
    print("📋 Standard National ML metrics should be:")
    print("   Water Segmentation: water_f1, water_iou, water_precision, water_recall, water_dice, water_accuracy")
    print("   D8 Flow Direction: d8_accuracy, d8_f1_macro, d8_precision_macro, d8_recall_macro")
    print()
    
    # For now, let's convert what we can from the existing data
    results_dir = Path("/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/results_full")
    
    # Read current non-standard results
    df_current = pd.read_csv(results_dir / "multitask_benefit_analysis_20251012_110647.csv")
    df_per_huc = pd.read_csv(results_dir / "multitask_benefit_per_huc_20251012_110647.csv")
    
    print("📊 Current metrics found:")
    print(f"   Columns: {list(df_current.columns)}")
    print()
    
    # Create mapping to standard metrics where possible
    print("🔄 Converting to standard metrics format...")
    
    # Create standardized results structure
    standardized_results = []
    
    for _, row in df_current.iterrows():
        standard_row = {
            'timestamp': row['timestamp'],
            'model_type': row['model_type'],
            'uses_flow_loss': row['uses_flow_loss'],
            'checkpoint': row['checkpoint'],
            # Convert dice to water_dice (same metric, standard naming)
            'water_dice': row['dice'],
            'water_iou': row['iou'] if pd.notna(row['iou']) else None,
            # Note: We don't have precision, recall, f1, accuracy in current data
            'water_f1': None,  # F1 = 2 * precision * recall / (precision + recall)
            'water_precision': None,
            'water_recall': None, 
            'water_accuracy': None,
            # D8 metrics not available in current analysis
            'd8_accuracy': None,
            'd8_f1_macro': None,
            'd8_precision_macro': None,
            'd8_recall_macro': None,
            # Non-standard metrics (mark as deprecated)
            'cldice_deprecated': row['clDice'],
            'component_ratio_deprecated': row['component_ratio'],
            'hydro_consistency_deprecated': row['hydro_consistency']
        }
        standardized_results.append(standard_row)
    
    df_standardized = pd.DataFrame(standardized_results)
    
    # Create per-HUC standardized results
    standardized_per_huc = []
    
    for _, row in df_per_huc.iterrows():
        standard_row = {
            'huc_code': row['huc_code'],
            'model_type': row['model_type'],
            'uses_flow_loss': row['uses_flow_loss'],
            'n_patches': row['n_patches'],
            'water_dice': row['dice'],
            'water_iou': row['iou'] if pd.notna(row['iou']) else None,
            'water_f1': None,
            'water_precision': None,
            'water_recall': None,
            'water_accuracy': None,
            'd8_accuracy': None,
            'd8_f1_macro': None,
            'd8_precision_macro': None,
            'd8_recall_macro': None,
            # Deprecated metrics
            'cldice_deprecated': row['clDice'],
            'component_ratio_deprecated': row['component_ratio'],
            'hydro_consistency_deprecated': row['hydro_consistency']
        }
        standardized_per_huc.append(standard_row)
    
    df_per_huc_standardized = pd.DataFrame(standardized_per_huc)
    
    # Save standardized results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_file = results_dir / f"STANDARD_multitask_benefit_analysis_{timestamp}.csv"
    df_standardized.to_csv(output_file, index=False)
    
    output_per_huc = results_dir / f"STANDARD_multitask_benefit_per_huc_{timestamp}.csv"
    df_per_huc_standardized.to_csv(output_per_huc, index=False)
    
    print(f"✅ Converted to standard format:")
    print(f"   📁 {output_file}")
    print(f"   📁 {output_per_huc}")
    print()
    
    # Create analysis with available metrics
    print("📈 Analysis with available standard metrics:")
    print()
    
    # Water Dice analysis (only metric we have)
    segonly = df_standardized[df_standardized['model_type'] == 'Segmentation-Only']
    multitask = df_standardized[df_standardized['model_type'] == 'Multitask (Seg+Flow)']
    
    if len(segonly) > 0 and len(multitask) > 0:
        dice_segonly = segonly['water_dice'].iloc[0]
        dice_multitask = multitask['water_dice'].iloc[0]
        dice_improvement = (dice_multitask - dice_segonly) / dice_segonly * 100
        
        print(f"🎯 Water Dice Score Results:")
        print(f"   Segmentation-Only:  {dice_segonly:.4f}")
        print(f"   Multitask:          {dice_multitask:.4f}")
        print(f"   Improvement:        +{dice_improvement:.2f}%")
        print()
        
        # Per-HUC analysis
        print("📊 Per-HUC Water Dice Analysis:")
        huc_codes = df_per_huc_standardized['huc_code'].unique()
        
        for huc in huc_codes:
            huc_data = df_per_huc_standardized[df_per_huc_standardized['huc_code'] == huc]
            segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
            multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
            
            if not segonly_row.empty and not multitask_row.empty:
                dice_seg = segonly_row['water_dice'].iloc[0]
                dice_multi = multitask_row['water_dice'].iloc[0]
                patches = multitask_row['n_patches'].iloc[0]
                improvement = (dice_multi - dice_seg) / dice_seg * 100
                
                print(f"   {huc} ({patches} patches): {dice_seg:.3f} → {dice_multi:.3f} ({improvement:+.1f}%)")
    
    print()
    print("🚨 IMPORTANT RECOMMENDATIONS:")
    print()
    print("1. 📋 RE-RUN EVALUATION: The current analysis uses non-standard metrics.")
    print("   Use one of these evaluators to get standard National ML metrics:")
    print("   - experiments/evaluation/ultra_fast_mdmt_evaluator.py")
    print("   - experiments/evaluation/simple_dem_thermal_evaluator.py")
    print()
    print("2. 🎯 STANDARD METRICS to collect:")
    print("   Water Segmentation:")
    print("   - water_f1 (primary metric)")
    print("   - water_iou")
    print("   - water_precision")
    print("   - water_recall") 
    print("   - water_dice")
    print("   - water_accuracy")
    print()
    print("   D8 Flow Direction:")
    print("   - d8_accuracy")
    print("   - d8_f1_macro (primary D8 metric)")
    print("   - d8_precision_macro")
    print("   - d8_recall_macro")
    print()
    print("3. 🔄 EVALUATION COMMAND EXAMPLE:")
    print("   cd /u/nathanj/national_ml/experiments/evaluation")
    print("   python ultra_fast_mdmt_evaluator.py \\")
    print("     --checkpoint_multitask /path/to/multitask_checkpoint.ckpt \\")
    print("     --checkpoint_segonly /path/to/segonly_checkpoint.ckpt \\")
    print("     --hucs 02080201,22010000,08050002")
    print()
    print("4. 📊 COMPARISON FRAMEWORK:")
    print("   Run both multitask and segmentation-only models on same HUCs")
    print("   Compare water_f1 and d8_f1_macro as primary metrics")
    print("   Report improvements in standard format")
    
    return df_standardized, df_per_huc_standardized

def create_recommendation_summary():
    """Create summary of what needs to be done for standard metrics analysis"""
    
    summary = {
        "analysis_date": datetime.now().isoformat(),
        "status": "NEEDS_RERUN_WITH_STANDARD_METRICS",
        "current_issue": "Analysis uses non-standard metrics (dice, clDice, component_ratio, hydro_consistency)",
        "required_action": "Re-run evaluation with standard National ML metrics",
        "standard_metrics_required": {
            "water_segmentation": [
                "water_f1",
                "water_iou", 
                "water_precision",
                "water_recall",
                "water_dice",
                "water_accuracy"
            ],
            "d8_flow_direction": [
                "d8_accuracy",
                "d8_f1_macro",
                "d8_precision_macro", 
                "d8_recall_macro"
            ]
        },
        "recommended_evaluators": [
            "experiments/evaluation/ultra_fast_mdmt_evaluator.py",
            "experiments/evaluation/simple_dem_thermal_evaluator.py"
        ],
        "test_hucs": ["02080201", "22010000", "08050002"],
        "comparison_framework": "Run both multitask and segmentation-only models on same HUCs and compare standard metrics"
    }
    
    results_dir = Path("/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/results_full")
    
    with open(results_dir / "STANDARD_METRICS_REQUIREMENTS.json", 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"💾 Requirements summary saved to: STANDARD_METRICS_REQUIREMENTS.json")

if __name__ == "__main__":
    df_std, df_per_huc_std = create_standard_metrics_analysis()
    create_recommendation_summary()
    print("\n" + "="*80)
    print("✅ Standard metrics conversion complete!")
    print("⚠️  Next step: Re-run evaluation with proper National ML evaluators")