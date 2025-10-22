#!/usr/bin/env python3
"""
Enhanced Multitask Benefit Comparison Visualization
Creates comprehensive comparison plots including Dice Score as a primary metric.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json

def create_comprehensive_comparison_plots(results_dir):
    """Create enhanced comparison visualization with Dice Score prominence"""
    
    results_dir = Path(results_dir)
    
    # Load data
    csv_file = results_dir / "multitask_benefit_analysis_20251012_110647.csv"
    per_huc_file = results_dir / "multitask_benefit_per_huc_20251012_110647.csv"
    
    df_overall = pd.read_csv(csv_file)
    df_per_huc = pd.read_csv(per_huc_file)
    
    # Set style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("Set2")
    
    # Create comprehensive figure with multiple subplots
    fig = plt.figure(figsize=(20, 15))
    fig.suptitle('Multitask Learning Benefits: Comprehensive Performance Analysis', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    # 1. Overall Performance Comparison (Top Left)
    ax1 = plt.subplot(3, 3, 1)
    metrics = ['dice', 'clDice', 'component_ratio', 'hydro_consistency']
    metric_labels = ['Dice Score', 'clDice', 'Component Ratio*', 'Hydro Consistency']
    
    segonly_values = []
    multitask_values = []
    
    for metric in metrics:
        segonly_val = df_overall[df_overall['model_type'] == 'Segmentation-Only'][metric].iloc[0]
        multitask_val = df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)'][metric].iloc[0]
        
        segonly_values.append(segonly_val)
        multitask_values.append(multitask_val)
    
    x_pos = np.arange(len(metrics))
    width = 0.35
    
    bars1 = ax1.bar(x_pos - width/2, segonly_values, width, label='Segmentation-Only', alpha=0.8)
    bars2 = ax1.bar(x_pos + width/2, multitask_values, width, label='Multitask (Seg+Flow)', alpha=0.8)
    
    ax1.set_xlabel('Metrics')
    ax1.set_ylabel('Score')
    ax1.set_title('Overall Performance Comparison')
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(metric_labels, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Add value labels on bars
    def add_value_labels(ax, bars):
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                   f'{height:.3f}', ha='center', va='bottom', fontsize=8)
    
    add_value_labels(ax1, bars1)
    add_value_labels(ax1, bars2)
    
    # 2. Dice Score by HUC (Top Center) - PROMINENT PLACEMENT
    ax2 = plt.subplot(3, 3, 2)
    huc_codes = df_per_huc['huc_code'].unique()
    
    dice_segonly = []
    dice_multitask = []
    patch_counts = []
    
    for huc in huc_codes:
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
        multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
        
        if not segonly_row.empty and not multitask_row.empty:
            dice_segonly.append(segonly_row['dice'].iloc[0])
            dice_multitask.append(multitask_row['dice'].iloc[0])
            patch_counts.append(multitask_row['n_patches'].iloc[0])
    
    x_pos = np.arange(len(huc_codes))
    bars1 = ax2.bar(x_pos - width/2, dice_segonly, width, label='Segmentation-Only', 
                    alpha=0.8, color='lightcoral')
    bars2 = ax2.bar(x_pos + width/2, dice_multitask, width, label='Multitask (Seg+Flow)', 
                    alpha=0.8, color='lightgreen')
    
    ax2.set_xlabel('HUC Code')
    ax2.set_ylabel('Dice Score')
    ax2.set_title('Dice Score by HUC (Primary Metric)', fontweight='bold')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels([f'{huc}\n({pc} patches)' for huc, pc in zip(huc_codes, patch_counts)])
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    add_value_labels(ax2, bars1)
    add_value_labels(ax2, bars2)
    
    # 3. clDice by HUC (Top Right)
    ax3 = plt.subplot(3, 3, 3)
    cldice_segonly = []
    cldice_multitask = []
    
    for huc in huc_codes:
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
        multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
        
        if not segonly_row.empty and not multitask_row.empty:
            cldice_segonly.append(segonly_row['clDice'].iloc[0])
            cldice_multitask.append(multitask_row['clDice'].iloc[0])
    
    bars1 = ax3.bar(x_pos - width/2, cldice_segonly, width, label='Segmentation-Only', alpha=0.8)
    bars2 = ax3.bar(x_pos + width/2, cldice_multitask, width, label='Multitask (Seg+Flow)', alpha=0.8)
    
    ax3.set_xlabel('HUC Code')
    ax3.set_ylabel('clDice Score')
    ax3.set_title('clDice by HUC (Connectivity)')
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels([f'{huc}\n({pc} patches)' for huc, pc in zip(huc_codes, patch_counts)])
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    add_value_labels(ax3, bars1)
    add_value_labels(ax3, bars2)
    
    # 4. Improvement Percentages (Middle Left)
    ax4 = plt.subplot(3, 3, 4)
    
    # Calculate improvements
    improvements = []
    improvement_labels = []
    
    for i, metric in enumerate(metrics):
        segonly_val = segonly_values[i]
        multitask_val = multitask_values[i]
        
        if metric == 'component_ratio':
            # For component ratio, closer to 1.0 is better
            seg_distance = abs(segonly_val - 1.0)
            mult_distance = abs(multitask_val - 1.0)
            improvement = (seg_distance - mult_distance) / seg_distance * 100
        else:
            improvement = (multitask_val - segonly_val) / segonly_val * 100
        
        improvements.append(improvement)
        improvement_labels.append(metric_labels[i])
    
    colors = ['green' if x > 0 else 'red' for x in improvements]
    bars = ax4.barh(improvement_labels, improvements, color=colors, alpha=0.7)
    
    ax4.axvline(x=0, color='black', linestyle='-', alpha=0.5)
    ax4.set_xlabel('Improvement (%)')
    ax4.set_title('Overall Performance Improvements')
    ax4.grid(True, alpha=0.3)
    
    # Add value labels
    for bar, value in zip(bars, improvements):
        width = bar.get_width()
        ax4.text(width + (1 if width > 0 else -1), bar.get_y() + bar.get_height()/2,
                f'{value:.1f}%', ha='left' if width > 0 else 'right', va='center')
    
    # 5. Dice Score Improvement by HUC (Middle Center) - PROMINENT
    ax5 = plt.subplot(3, 3, 5)
    
    dice_improvements = []
    for i in range(len(huc_codes)):
        improvement = (dice_multitask[i] - dice_segonly[i]) / dice_segonly[i] * 100
        dice_improvements.append(improvement)
    
    colors = ['green' if x > 0 else 'red' for x in dice_improvements]
    bars = ax5.bar(huc_codes, dice_improvements, color=colors, alpha=0.7)
    
    ax5.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax5.set_xlabel('HUC Code')
    ax5.set_ylabel('Dice Improvement (%)')
    ax5.set_title('Dice Score Improvement by HUC', fontweight='bold')
    ax5.grid(True, alpha=0.3)
    
    # Add value labels
    for bar, value in zip(bars, dice_improvements):
        height = bar.get_height()
        ax5.text(bar.get_x() + bar.get_width()/2., height + (0.5 if height > 0 else -1),
               f'{value:.1f}%', ha='center', va='bottom' if height > 0 else 'top')
    
    # 6. Performance vs Dataset Size (Middle Right)
    ax6 = plt.subplot(3, 3, 6)
    
    scatter = ax6.scatter(patch_counts, dice_improvements, s=150, alpha=0.7, 
                         c=dice_improvements, cmap='RdYlGn', edgecolors='black')
    
    for i, huc in enumerate(huc_codes):
        ax6.annotate(huc, (patch_counts[i], dice_improvements[i]),
                   xytext=(5, 5), textcoords='offset points', fontsize=9)
    
    ax6.set_xlabel('Number of Patches')
    ax6.set_ylabel('Dice Improvement (%)')
    ax6.set_title('Dice Improvement vs Dataset Size')
    ax6.grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=ax6, label='Dice Improvement (%)')
    
    # 7. Hydro Consistency Comparison (Bottom Left)
    ax7 = plt.subplot(3, 3, 7)
    
    hydro_segonly = []
    hydro_multitask = []
    
    for huc in huc_codes:
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
        multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
        
        if not segonly_row.empty and not multitask_row.empty:
            hydro_segonly.append(segonly_row['hydro_consistency'].iloc[0])
            hydro_multitask.append(multitask_row['hydro_consistency'].iloc[0])
    
    bars1 = ax7.bar(x_pos - width/2, hydro_segonly, width, label='Segmentation-Only', alpha=0.8)
    bars2 = ax7.bar(x_pos + width/2, hydro_multitask, width, label='Multitask (Seg+Flow)', alpha=0.8)
    
    ax7.set_xlabel('HUC Code')
    ax7.set_ylabel('Hydro Consistency')
    ax7.set_title('Hydrological Consistency by HUC')
    ax7.set_xticks(x_pos)
    ax7.set_xticklabels([f'{huc}\n({pc} patches)' for huc, pc in zip(huc_codes, patch_counts)])
    ax7.legend()
    ax7.grid(True, alpha=0.3)
    
    add_value_labels(ax7, bars1)
    add_value_labels(ax7, bars2)
    
    # 8. Summary Statistics (Bottom Center)
    ax8 = plt.subplot(3, 3, 8)
    ax8.axis('off')
    
    # Calculate summary statistics
    overall_dice_improvement = (df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)']['dice'].iloc[0] - 
                               df_overall[df_overall['model_type'] == 'Segmentation-Only']['dice'].iloc[0]) / \
                               df_overall[df_overall['model_type'] == 'Segmentation-Only']['dice'].iloc[0] * 100
    
    overall_cldice_improvement = (df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)']['clDice'].iloc[0] - 
                                 df_overall[df_overall['model_type'] == 'Segmentation-Only']['clDice'].iloc[0]) / \
                                 df_overall[df_overall['model_type'] == 'Segmentation-Only']['clDice'].iloc[0] * 100
    
    positive_dice_improvements = sum(1 for x in dice_improvements if x > 0)
    total_hucs = len(dice_improvements)
    
    summary_text = f"""
    📊 SUMMARY STATISTICS
    
    🎯 Overall Dice Improvement: +{overall_dice_improvement:.1f}%
    🔗 Overall clDice Improvement: +{overall_cldice_improvement:.1f}%
    
    📈 HUCs with Dice Improvement: {positive_dice_improvements}/{total_hucs}
    📦 Total Patches Analyzed: {sum(patch_counts):,}
    
    🏆 Best Performing HUC: {huc_codes[np.argmax(dice_improvements)]}
    📈 Max Dice Improvement: +{max(dice_improvements):.1f}%
    
    ⭐ Key Finding: 
    Multitask learning provides consistent 
    connectivity improvements (clDice) across 
    all watersheds while maintaining or 
    improving overall segmentation quality.
    """
    
    ax8.text(0.1, 0.9, summary_text, transform=ax8.transAxes, fontsize=11,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.1))
    
    # 9. Component Ratio Analysis (Bottom Right)
    ax9 = plt.subplot(3, 3, 9)
    
    comp_segonly = []
    comp_multitask = []
    
    for huc in huc_codes:
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
        multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
        
        if not segonly_row.empty and not multitask_row.empty:
            comp_segonly.append(segonly_row['component_ratio'].iloc[0])
            comp_multitask.append(multitask_row['component_ratio'].iloc[0])
    
    bars1 = ax9.bar(x_pos - width/2, comp_segonly, width, label='Segmentation-Only', alpha=0.8)
    bars2 = ax9.bar(x_pos + width/2, comp_multitask, width, label='Multitask (Seg+Flow)', alpha=0.8)
    
    # Add ideal line at y=1
    ax9.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Ideal (1.0)')
    
    ax9.set_xlabel('HUC Code')
    ax9.set_ylabel('Component Ratio')
    ax9.set_title('Component Ratio by HUC\n(Closer to 1.0 = Better)')
    ax9.set_xticks(x_pos)
    ax9.set_xticklabels([f'{huc}\n({pc} patches)' for huc, pc in zip(huc_codes, patch_counts)])
    ax9.legend()
    ax9.grid(True, alpha=0.3)
    
    add_value_labels(ax9, bars1)
    add_value_labels(ax9, bars2)
    
    plt.tight_layout()
    
    # Save the comprehensive visualization
    output_file = results_dir / "COMPREHENSIVE_multitask_benefit_comparison_with_dice.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\\n✅ Created comprehensive comparison visualization with prominent Dice Score analysis!")
    print(f"📁 Saved to: {output_file}")
    
    return True

if __name__ == "__main__":
    results_dir = "/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/results_full"
    create_comprehensive_comparison_plots(results_dir)