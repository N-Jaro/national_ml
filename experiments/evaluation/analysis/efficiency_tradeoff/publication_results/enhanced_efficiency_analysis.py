#!/usr/bin/env python3
"""
Enhanced Efficiency Analysis with Corrected F1 Scores
Updates the efficiency tradeoff analysis with accurate F1 metrics from comprehensive evaluation.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from datetime import datetime

def load_and_enhance_efficiency_data():
    """Load existing efficiency data and enhance with correct F1 scores"""
    
    # Load existing efficiency results
    results_dir = Path("/u/nathanj/national_ml/experiments/evaluation/analysis/efficiency_tradeoff/publication_results")
    df_efficiency = pd.read_csv(results_dir / "publication_efficiency_results_20251012_125954.csv")
    
    # Corrected performance metrics from comprehensive evaluation
    corrected_metrics = {
        'AlphaEarth-only': {
            'water_f1': 0.466973516,
            'water_dice': 0.466973516,  # Dice = F1 for binary segmentation
            'water_iou': 0.326563067,
            'water_precision': 0.526478727,
            'water_recall': 0.482595674,
            'water_accuracy': 0.918539744,
        },
        'DEM+AlphaEarth': {
            'water_f1': 0.473100852,
            'water_dice': 0.473100853,
            'water_iou': 0.331710592,
            'water_precision': 0.507860853,
            'water_recall': 0.55598649,
            'water_accuracy': 0.878744484,
        },
        'All+AlphaEarth': {  # This corresponds to DEM+SAR+Thermal+Optical+AlphaEarth
            'water_f1': 0.484945637,
            'water_dice': 0.484945638,
            'water_iou': 0.342354764,
            'water_precision': 0.568362874,
            'water_recall': 0.487398596,
            'water_accuracy': 0.916022566,
        }
    }
    
    # Update the dataframe with corrected metrics
    for idx, row in df_efficiency.iterrows():
        model_name = row['model_name']
        if model_name in corrected_metrics:
            for metric, value in corrected_metrics[model_name].items():
                df_efficiency.at[idx, metric] = value
    
    # Recalculate efficiency metrics with corrected performance
    for idx, row in df_efficiency.iterrows():
        model_name = row['model_name']
        water_f1 = df_efficiency.at[idx, 'water_f1']
        total_params = row['total_parameters']
        flops = row['flops']
        total_channels = row['total_input_channels']
        
        # Update efficiency calculations
        df_efficiency.at[idx, 'f1_per_param'] = water_f1 / (total_params / 1e6)  # F1 per million params
        df_efficiency.at[idx, 'f1_per_gflop'] = water_f1 / (flops / 1e9)  # F1 per GFLOP
        df_efficiency.at[idx, 'f1_per_channel'] = water_f1 / total_channels  # F1 per input channel
        
        # Update IoU-based efficiency metrics
        water_iou = df_efficiency.at[idx, 'water_iou']
        df_efficiency.at[idx, 'iou_per_param'] = water_iou / (total_params / 1e6)
        df_efficiency.at[idx, 'iou_per_gflop'] = water_iou / (flops / 1e9)
        df_efficiency.at[idx, 'iou_per_channel'] = water_iou / total_channels
    
    # Recalculate composite efficiency scores using F1 as primary metric
    f1_values = df_efficiency['water_f1'].values
    param_values = df_efficiency['total_parameters'].values
    flop_values = df_efficiency['flops'].values
    
    # Normalize metrics (0-1 scale)
    f1_norm = (f1_values - f1_values.min()) / (f1_values.max() - f1_values.min())
    param_efficiency_norm = 1 - (param_values - param_values.min()) / (param_values.max() - param_values.min())
    flop_efficiency_norm = 1 - (flop_values - flop_values.min()) / (flop_values.max() - flop_values.min())
    
    # Composite efficiency score
    df_efficiency['efficiency_score_f1'] = (f1_norm + param_efficiency_norm + flop_efficiency_norm) / 3
    
    return df_efficiency

def create_enhanced_efficiency_visualization(df_efficiency):
    """Create enhanced visualization with F1 scores"""
    
    plt.style.use('seaborn-v0_8')
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Enhanced Computational Efficiency Analysis: F1 Score Integration\nMultimodal Water Segmentation Models', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Extract data for plotting
    models = df_efficiency['model_name'].values
    f1_scores = df_efficiency['water_f1'].values
    iou_scores = df_efficiency['water_iou'].values
    params = df_efficiency['total_parameters'].values / 1e6  # Convert to millions
    flops = df_efficiency['flops'].values / 1e9  # Convert to billions
    colors = df_efficiency['color'].values
    markers = df_efficiency['marker'].values
    efficiency_f1 = df_efficiency['efficiency_score_f1'].values
    
    # 1. F1 Score vs Parameters (Top Left)
    for i, (model, f1, param, color, marker, eff) in enumerate(zip(models, f1_scores, params, colors, markers, efficiency_f1)):
        ax1.scatter(param, f1, c=color, marker=marker, s=200*eff, alpha=0.8, edgecolors='black', linewidth=1)
        ax1.annotate(model.replace('+', '+\\n'), (param, f1), xytext=(5, 5), 
                    textcoords='offset points', fontsize=9, ha='left')
    
    ax1.set_xlabel('Parameters (Millions)', fontweight='bold')
    ax1.set_ylabel('Water F1 Score', fontweight='bold')
    ax1.set_title('F1 Performance vs Model Complexity', fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0.46, 0.49)
    
    # 2. F1 Score vs FLOPs (Top Right)
    for i, (model, f1, flop, color, marker, eff) in enumerate(zip(models, f1_scores, flops, colors, markers, efficiency_f1)):
        ax2.scatter(flop, f1, c=color, marker=marker, s=200*eff, alpha=0.8, edgecolors='black', linewidth=1)
        ax2.annotate(model.replace('+', '+\\n'), (flop, f1), xytext=(5, 5), 
                    textcoords='offset points', fontsize=9, ha='left')
    
    ax2.set_xlabel('FLOPs (Billions)', fontweight='bold')
    ax2.set_ylabel('Water F1 Score', fontweight='bold')
    ax2.set_title('F1 Performance vs Computational Cost', fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0.46, 0.49)
    
    # 3. Efficiency Comparison - F1 vs IoU (Bottom Left)
    metrics = ['F1 Score', 'IoU Score']
    x_pos = np.arange(len(models))
    width = 0.35
    
    bars1 = ax3.bar(x_pos - width/2, f1_scores, width, label='F1 Score', alpha=0.8, color='steelblue')
    bars2 = ax3.bar(x_pos + width/2, iou_scores, width, label='IoU Score', alpha=0.8, color='lightcoral')
    
    ax3.set_xlabel('Model Architecture', fontweight='bold')
    ax3.set_ylabel('Performance Score', fontweight='bold')
    ax3.set_title('F1 vs IoU Performance Comparison', fontweight='bold')
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels([model.replace('+', '+\\n') for model in models], rotation=0, ha='center')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar1, bar2, f1, iou in zip(bars1, bars2, f1_scores, iou_scores):
        ax3.text(bar1.get_x() + bar1.get_width()/2, bar1.get_height() + 0.005, 
                f'{f1:.3f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
        ax3.text(bar2.get_x() + bar2.get_width()/2, bar2.get_height() + 0.005, 
                f'{iou:.3f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    # 4. Composite Efficiency Scores (Bottom Right)
    efficiency_iou = df_efficiency['efficiency_score'].values  # Original IoU-based
    
    bars1 = ax4.bar(x_pos - width/2, efficiency_f1, width, label='F1-Based Efficiency', alpha=0.8, color='darkgreen')
    bars2 = ax4.bar(x_pos + width/2, efficiency_iou, width, label='IoU-Based Efficiency', alpha=0.8, color='orange')
    
    ax4.set_xlabel('Model Architecture', fontweight='bold')
    ax4.set_ylabel('Efficiency Score', fontweight='bold')
    ax4.set_title('Composite Efficiency Score Comparison', fontweight='bold')
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels([model.replace('+', '+\\n') for model in models], rotation=0, ha='center')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.set_ylim(0, 1)
    
    # Add value labels on bars
    for bar1, bar2, eff_f1, eff_iou in zip(bars1, bars2, efficiency_f1, efficiency_iou):
        ax4.text(bar1.get_x() + bar1.get_width()/2, bar1.get_height() + 0.02, 
                f'{eff_f1:.3f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
        ax4.text(bar2.get_x() + bar2.get_width()/2, bar2.get_height() + 0.02, 
                f'{eff_iou:.3f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    
    # Save the enhanced visualization
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/u/nathanj/national_ml/experiments/evaluation/analysis/efficiency_tradeoff/publication_results/enhanced_efficiency_f1_analysis_{timestamp}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return output_path

def generate_enhanced_efficiency_report(df_efficiency):
    """Generate enhanced report with F1 score analysis"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Prepare analysis results
    results = {
        'analysis_date': datetime.now().isoformat(),
        'enhancement_type': 'F1_Score_Integration',
        'models_analyzed': len(df_efficiency),
        'primary_findings': {},
        'efficiency_rankings': {},
        'performance_gaps': {},
        'recommendations': {}
    }
    
    # Calculate performance gaps and improvements
    alphaearth_f1 = df_efficiency[df_efficiency['model_name'] == 'AlphaEarth-only']['water_f1'].iloc[0]
    dem_alphaearth_f1 = df_efficiency[df_efficiency['model_name'] == 'DEM+AlphaEarth']['water_f1'].iloc[0]
    all_alphaearth_f1 = df_efficiency[df_efficiency['model_name'] == 'All+AlphaEarth']['water_f1'].iloc[0]
    
    # Performance improvements
    dem_improvement = ((dem_alphaearth_f1 - alphaearth_f1) / alphaearth_f1) * 100
    all_improvement = ((all_alphaearth_f1 - dem_alphaearth_f1) / dem_alphaearth_f1) * 100
    
    results['performance_gaps'] = {
        'alphaearth_only_f1': float(alphaearth_f1),
        'dem_alphaearth_f1': float(dem_alphaearth_f1),
        'all_alphaearth_f1': float(all_alphaearth_f1),
        'dem_improvement_percent': float(dem_improvement),
        'all_improvement_percent': float(all_improvement)
    }
    
    # Efficiency rankings based on F1
    df_sorted = df_efficiency.sort_values('efficiency_score_f1', ascending=False)
    for idx, row in df_sorted.iterrows():
        results['efficiency_rankings'][row['model_name']] = {
            'rank': int(idx + 1),
            'f1_efficiency_score': float(row['efficiency_score_f1']),
            'water_f1': float(row['water_f1']),
            'parameters_millions': float(row['total_parameters'] / 1e6),
            'flops_billions': float(row['flops'] / 1e9)
        }
    
    # Key findings
    results['primary_findings'] = {
        'optimal_model': df_sorted.iloc[0]['model_name'],
        'optimal_efficiency_score': float(df_sorted.iloc[0]['efficiency_score_f1']),
        'f1_performance_leader': df_efficiency.loc[df_efficiency['water_f1'].idxmax(), 'model_name'],
        'best_f1_score': float(df_efficiency['water_f1'].max()),
        'dem_addition_benefit': f"DEM addition provides {dem_improvement:.1f}% F1 improvement",
        'multimodal_benefit': f"Full multimodal adds {all_improvement:.1f}% F1 improvement over DEM+AlphaEarth"
    }
    
    # Save enhanced results
    results_path = f"/u/nathanj/national_ml/experiments/evaluation/analysis/efficiency_tradeoff/publication_results/enhanced_efficiency_f1_results_{timestamp}.json"
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Save enhanced CSV
    csv_path = f"/u/nathanj/national_ml/experiments/evaluation/analysis/efficiency_tradeoff/publication_results/enhanced_efficiency_f1_data_{timestamp}.csv"
    df_efficiency.to_csv(csv_path, index=False)
    
    return results_path, csv_path

def create_enhanced_efficiency_summary():
    """Create comprehensive efficiency analysis with F1 scores"""
    
    print("🔄 Loading and enhancing efficiency data with corrected F1 scores...")
    df_efficiency = load_and_enhance_efficiency_data()
    
    print("📊 Creating enhanced visualization...")
    viz_path = create_enhanced_efficiency_visualization(df_efficiency)
    
    print("📋 Generating enhanced analysis report...")
    results_path, csv_path = generate_enhanced_efficiency_report(df_efficiency)
    
    print("\\n✅ Enhanced Efficiency Analysis Complete!")
    print(f"📈 Visualization: {viz_path}")
    print(f"📊 Results: {results_path}")
    print(f"📁 Data: {csv_path}")
    
    # Print key findings
    print("\\n🎯 Key Findings with F1 Scores:")
    for idx, row in df_efficiency.iterrows():
        model = row['model_name']
        f1 = row['water_f1']
        iou = row['water_iou']
        eff_f1 = row['efficiency_score_f1']
        params = row['total_parameters'] / 1e6
        
        print(f"  {model:20} | F1: {f1:.3f} | IoU: {iou:.3f} | Efficiency: {eff_f1:.3f} | Params: {params:.1f}M")
    
    print(f"\\n🏆 Optimal Model: {df_efficiency.loc[df_efficiency['efficiency_score_f1'].idxmax(), 'model_name']}")
    print(f"🎯 Best F1 Score: {df_efficiency['water_f1'].max():.3f}")
    
    return df_efficiency

if __name__ == "__main__":
    df_results = create_enhanced_efficiency_summary()