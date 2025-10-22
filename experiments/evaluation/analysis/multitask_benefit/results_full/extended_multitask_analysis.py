#!/usr/bin/env python3
"""
Extended Multitask Benefit Analysis
Generates additional insights and visualizations from multitask learning comparison results.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from datetime import datetime

def load_and_analyze_results(results_dir):
    """Load multitask benefit analysis results and perform extended analysis"""
    
    results_dir = Path(results_dir)
    
    # Load data
    csv_file = results_dir / "multitask_benefit_analysis_20251012_110647.csv"
    per_huc_file = results_dir / "multitask_benefit_per_huc_20251012_110647.csv"
    
    df_overall = pd.read_csv(csv_file)
    df_per_huc = pd.read_csv(per_huc_file)
    
    return df_overall, df_per_huc

def calculate_improvement_metrics(df_per_huc):
    """Calculate improvement metrics by HUC"""
    
    metrics = ['dice', 'iou', 'clDice', 'component_ratio', 'hydro_consistency']
    huc_codes = df_per_huc['huc_code'].unique()
    
    improvements = []
    
    for huc in huc_codes:
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        
        if len(huc_data) == 2:  # Should have both multitask and segonly
            multitask = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)'].iloc[0]
            segonly = huc_data[huc_data['model_type'] == 'Segmentation-Only'].iloc[0]
            
            huc_improvement = {
                'huc_code': huc,
                'n_patches': multitask['n_patches']
            }
            
            for metric in metrics:
                if pd.notna(multitask[metric]) and pd.notna(segonly[metric]):
                    if metric == 'component_ratio':
                        # For component ratio, closer to 1.0 is better
                        # Calculate improvement as reduction in distance from 1.0
                        seg_distance = abs(segonly[metric] - 1.0)
                        mult_distance = abs(multitask[metric] - 1.0)
                        improvement = (seg_distance - mult_distance) / seg_distance * 100
                    else:
                        # For other metrics, higher is better
                        improvement = (multitask[metric] - segonly[metric]) / segonly[metric] * 100
                    
                    huc_improvement[f'{metric}_improvement_pct'] = improvement
                    huc_improvement[f'{metric}_multitask'] = multitask[metric]
                    huc_improvement[f'{metric}_segonly'] = segonly[metric]
            
            improvements.append(huc_improvement)
    
    return pd.DataFrame(improvements)

def create_extended_visualizations(df_improvements, output_dir):
    """Create additional visualizations for the analysis"""
    
    output_dir = Path(output_dir)
    
    # Set style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("husl")
    
    # 1. Improvement by metric and HUC
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Multitask Learning Benefits by Metric and HUC', fontsize=16, fontweight='bold')
    
    metrics_to_plot = ['dice_improvement_pct', 'clDice_improvement_pct', 
                      'component_ratio_improvement_pct', 'hydro_consistency_improvement_pct']
    metric_labels = ['Dice Score', 'clDice', 'Component Ratio', 'Hydro Consistency']
    
    for i, (metric, label) in enumerate(zip(metrics_to_plot, metric_labels)):
        ax = axes[i//2, i%2]
        
        if metric in df_improvements.columns:
            bars = ax.bar(df_improvements['huc_code'], df_improvements[metric])
            
            # Color bars based on improvement (green positive, red negative)
            for bar, value in zip(bars, df_improvements[metric]):
                if value > 0:
                    bar.set_color('green')
                    bar.set_alpha(0.7)
                else:
                    bar.set_color('red')
                    bar.set_alpha(0.7)
            
            ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
            ax.set_title(f'{label} Improvement (%)')
            ax.set_xlabel('HUC Code')
            ax.set_ylabel('Improvement (%)')
            ax.tick_params(axis='x', rotation=45)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + (0.5 if height > 0 else -1.5),
                       f'{height:.1f}%', ha='center', va='bottom' if height > 0 else 'top')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'multitask_improvement_by_huc_extended.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Performance vs. Dataset Size
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    # Calculate overall improvement score
    improvement_metrics = ['dice_improvement_pct', 'clDice_improvement_pct', 
                          'hydro_consistency_improvement_pct']
    df_improvements['overall_improvement'] = df_improvements[improvement_metrics].mean(axis=1)
    
    scatter = ax.scatter(df_improvements['n_patches'], df_improvements['overall_improvement'], 
                        s=100, alpha=0.7, c=df_improvements['overall_improvement'], 
                        cmap='RdYlGn', edgecolors='black')
    
    # Add HUC labels
    for _, row in df_improvements.iterrows():
        ax.annotate(row['huc_code'], (row['n_patches'], row['overall_improvement']),
                   xytext=(5, 5), textcoords='offset points', fontsize=9)
    
    ax.set_xlabel('Number of Patches')
    ax.set_ylabel('Overall Improvement (%)')
    ax.set_title('Multitask Learning Benefit vs. Dataset Size')
    ax.grid(True, alpha=0.3)
    plt.colorbar(scatter, label='Overall Improvement (%)')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'multitask_benefit_vs_dataset_size.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. Metric correlation heatmap
    correlation_metrics = [col for col in df_improvements.columns if '_improvement_pct' in col]
    if len(correlation_metrics) > 1:
        fig, ax = plt.subplots(1, 1, figsize=(8, 6))
        
        corr_matrix = df_improvements[correlation_metrics].corr()
        
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0,
                   square=True, ax=ax, cbar_kws={'label': 'Correlation'})
        
        ax.set_title('Correlation Between Metric Improvements')
        plt.tight_layout()
        plt.savefig(output_dir / 'metric_improvement_correlations.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    return True

def generate_statistical_summary(df_improvements):
    """Generate statistical summary of improvements"""
    
    summary = {
        'analysis_date': datetime.now().isoformat(),
        'total_hucs_analyzed': len(df_improvements),
        'total_patches': int(df_improvements['n_patches'].sum()),
        'metric_improvements': {}
    }
    
    improvement_cols = [col for col in df_improvements.columns if '_improvement_pct' in col]
    
    for col in improvement_cols:
        metric_name = col.replace('_improvement_pct', '')
        if col in df_improvements.columns:
            values = df_improvements[col].dropna()
            summary['metric_improvements'][metric_name] = {
                'mean_improvement_pct': float(values.mean()),
                'std_improvement_pct': float(values.std()),
                'min_improvement_pct': float(values.min()),
                'max_improvement_pct': float(values.max()),
                'positive_improvements': int((values > 0).sum()),
                'negative_improvements': int((values < 0).sum()),
                'consistent_improvement': bool((values > 0).all())
            }
    
    # Overall assessment
    all_improvements = []
    for col in improvement_cols:
        if col in df_improvements.columns:
            all_improvements.extend(df_improvements[col].dropna().tolist())
    
    if all_improvements:
        summary['overall_assessment'] = {
            'mean_improvement_across_all_metrics': float(np.mean(all_improvements)),
            'percent_positive_improvements': float(np.mean([x > 0 for x in all_improvements]) * 100),
            'strong_improvements_count': int(np.sum([x > 10 for x in all_improvements])),  # >10% improvement
            'total_metric_comparisons': len(all_improvements)
        }
    
    return summary

def main():
    """Main analysis function"""
    
    results_dir = "/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/results_full"
    
    print("🔍 Loading multitask benefit analysis results...")
    df_overall, df_per_huc = load_and_analyze_results(results_dir)
    
    print("📊 Calculating improvement metrics...")
    df_improvements = calculate_improvement_metrics(df_per_huc)
    
    print("📈 Creating extended visualizations...")
    create_extended_visualizations(df_improvements, results_dir)
    
    print("📋 Generating statistical summary...")
    statistical_summary = generate_statistical_summary(df_improvements)
    
    # Save detailed results
    output_file = Path(results_dir) / "extended_analysis_results.json"
    with open(output_file, 'w') as f:
        json.dump(statistical_summary, f, indent=2)
    
    # Save improvement metrics
    improvements_file = Path(results_dir) / "improvement_metrics_by_huc.csv"
    df_improvements.to_csv(improvements_file, index=False)
    
    print(f"\\n✅ Extended analysis complete!")
    print(f"📁 Results saved to: {results_dir}")
    print(f"📊 Generated files:")
    print(f"   - extended_analysis_results.json")
    print(f"   - improvement_metrics_by_huc.csv")
    print(f"   - multitask_improvement_by_huc_extended.png")
    print(f"   - multitask_benefit_vs_dataset_size.png")
    print(f"   - metric_improvement_correlations.png")
    
    # Print key insights
    print(f"\\n🔑 Key Insights:")
    for metric, stats in statistical_summary['metric_improvements'].items():
        print(f"   {metric.upper()}: {stats['mean_improvement_pct']:.1f}% avg improvement " +
              f"({stats['positive_improvements']}/{len(df_improvements)} HUCs improved)")
    
    if 'overall_assessment' in statistical_summary:
        overall = statistical_summary['overall_assessment']
        print(f"\\n🎯 Overall Assessment:")
        print(f"   - Average improvement across all metrics: {overall['mean_improvement_across_all_metrics']:.1f}%")
        print(f"   - Positive improvements: {overall['percent_positive_improvements']:.1f}% of comparisons")
        print(f"   - Strong improvements (>10%): {overall['strong_improvements_count']} instances")

if __name__ == "__main__":
    main()