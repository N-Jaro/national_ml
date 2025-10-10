#!/usr/bin/env python3
"""
Foundation Model Results Summary - Compare Prithvi vs Clay performance
"""

import pandas as pd
import glob
import os
from pathlib import Path

def load_all_results(results_dir):
    """Load all CSV results from foundation model evaluations"""
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(results_dir, "*_run*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {results_dir}")
        return None
    
    # Load and combine all results
    all_results = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        all_results.append(df)
    
    return pd.concat(all_results, ignore_index=True)

def main():
    results_dir = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results"
    
    print("=== FOUNDATION MODEL EVALUATION RESULTS SUMMARY ===\n")
    
    # Load results
    results_df = load_all_results(results_dir)
    if results_df is None:
        return
    
    print(f"Total evaluations loaded: {len(results_df)}")
    print(f"Models: {sorted(results_df['model_variant'].unique())}")
    print(f"HUCs evaluated: {sorted(results_df['huc_id'].unique())}")
    print()
    
    # Group by model variant
    for model in sorted(results_df['model_variant'].unique()):
        model_results = results_df[results_df['model_variant'] == model]
        
        print(f"=== {model.upper()} RESULTS ({len(model_results)} runs) ===")
        
        # Water segmentation metrics
        water_metrics = ['water_dice', 'water_iou', 'water_accuracy', 'water_precision', 'water_recall', 'water_f1']
        
        print("Water Segmentation Performance:")
        for metric in water_metrics:
            values = model_results[metric]
            print(f"  {metric:15s}: {values.mean():.4f} ± {values.std():.4f} (range: {values.min():.4f} - {values.max():.4f})")
        
        print(f"  {'total_samples':15s}: {model_results['total_samples'].iloc[0]}")
        print()
    
    # Compare models directly
    if len(results_df['model_variant'].unique()) > 1:
        print("=== MODEL COMPARISON ===")
        comparison_df = results_df.groupby('model_variant')[['water_dice', 'water_iou', 'water_accuracy']].agg(['mean', 'std'])
        
        print("Mean Performance Comparison:")
        print(comparison_df.round(4))
        print()
        
        # Statistical significance test (if enough samples)
        if len(results_df['model_variant'].unique()) == 2:
            from scipy import stats
            
            models = sorted(results_df['model_variant'].unique())
            model1_data = results_df[results_df['model_variant'] == models[0]]
            model2_data = results_df[results_df['model_variant'] == models[1]]
            
            print(f"Statistical Comparison ({models[0]} vs {models[1]}):")
            for metric in ['water_dice', 'water_iou', 'water_accuracy']:
                try:
                    t_stat, p_value = stats.ttest_ind(model1_data[metric], model2_data[metric])
                    significance = "***" if p_value < 0.001 else "**" if p_value < 0.01 else "*" if p_value < 0.05 else "ns"
                    print(f"  {metric:15s}: t={t_stat:6.3f}, p={p_value:.4f} {significance}")
                except:
                    print(f"  {metric:15s}: Statistical test failed")
            print()
    
    # Detailed results table
    print("=== DETAILED RESULTS ===")
    display_cols = ['model_variant', 'run_name', 'water_dice', 'water_iou', 'water_accuracy']
    detailed = results_df[display_cols].round(4)
    print(detailed.to_string(index=False))
    
    print(f"\nResults saved in: {results_dir}")
    print("Foundation model evaluation completed successfully! ✅")

if __name__ == '__main__':
    main()