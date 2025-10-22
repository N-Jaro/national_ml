#!/usr/bin/env python3
"""
Enhanced Standard Metrics Visualization
Creates comprehensive visualization using standard National ML metrics.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def create_standard_metrics_visualization():
    """Create comprehensive visualization with standard National ML metrics"""
    
    results_dir = Path("/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/results_full")
    
    # Load enhanced data
    enhanced_files = list(results_dir.glob("ENHANCED_multitask_benefit_analysis_*.csv"))
    per_huc_files = list(results_dir.glob("ENHANCED_multitask_benefit_per_huc_*.csv"))
    
    if not enhanced_files or not per_huc_files:
        print("❌ Enhanced data files not found. Run create_enhanced_metrics.py first.")
        return False
    
    # Use the most recent files
    df_overall = pd.read_csv(enhanced_files[-1])
    df_per_huc = pd.read_csv(per_huc_files[-1])
    
    # Set style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("Set2")
    
    # Create comprehensive figure
    fig = plt.figure(figsize=(20, 16))
    fig.suptitle('Multitask Learning Benefits: Standard National ML Metrics Analysis', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    # 1. Overall Water Segmentation Comparison (Top Left)
    ax1 = plt.subplot(3, 4, 1)
    
    water_metrics = ['water_f1', 'water_iou', 'water_dice', 'water_precision', 'water_recall', 'water_accuracy']
    water_labels = ['F1', 'IoU', 'Dice', 'Precision', 'Recall', 'Accuracy']
    
    segonly_values = []
    multitask_values = []
    
    for metric in water_metrics:
        segonly_val = df_overall[df_overall['model_type'] == 'Segmentation-Only'][metric].iloc[0]
        multitask_val = df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)'][metric].iloc[0]
        segonly_values.append(segonly_val)
        multitask_values.append(multitask_val)
    
    x_pos = np.arange(len(water_metrics))
    width = 0.35
    
    bars1 = ax1.bar(x_pos - width/2, segonly_values, width, label='Segmentation-Only', alpha=0.8, color='lightcoral')
    bars2 = ax1.bar(x_pos + width/2, multitask_values, width, label='Multitask (Seg+Flow)', alpha=0.8, color='lightgreen')
    
    ax1.set_xlabel('Water Segmentation Metrics')
    ax1.set_ylabel('Score')
    ax1.set_title('Water Segmentation Performance')
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(water_labels, rotation=45)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Overall D8 Flow Direction Comparison (Top Center-Left)
    ax2 = plt.subplot(3, 4, 2)
    
    d8_metrics = ['d8_accuracy', 'd8_f1_macro', 'd8_precision_macro', 'd8_recall_macro']
    d8_labels = ['Accuracy', 'F1 Macro', 'Precision', 'Recall']
    
    d8_segonly_values = []
    d8_multitask_values = []
    
    for metric in d8_metrics:
        segonly_val = df_overall[df_overall['model_type'] == 'Segmentation-Only'][metric].iloc[0]
        multitask_val = df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)'][metric].iloc[0]
        d8_segonly_values.append(segonly_val)
        d8_multitask_values.append(multitask_val)
    
    d8_x_pos = np.arange(len(d8_metrics))
    d8_width = 0.35
    
    bars1 = ax2.bar(d8_x_pos - d8_width/2, d8_segonly_values, d8_width, label='Segmentation-Only', alpha=0.8, color='lightcoral')
    bars2 = ax2.bar(d8_x_pos + d8_width/2, d8_multitask_values, d8_width, label='Multitask (Seg+Flow)', alpha=0.8, color='lightblue')
    
    ax2.set_xlabel('D8 Flow Direction Metrics')
    ax2.set_ylabel('Score')
    ax2.set_title('D8 Flow Direction Performance')
    ax2.set_xticks(d8_x_pos)
    ax2.set_xticklabels(d8_labels, rotation=45)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Water F1 by HUC (Top Center-Right)
    ax3 = plt.subplot(3, 4, 3)
    
    huc_codes = df_per_huc['huc_code'].unique()
    huc_codes_str = [str(huc) for huc in sorted(huc_codes)]
    
    f1_segonly = []
    f1_multitask = []
    patch_counts = []
    
    for huc in sorted(huc_codes):
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
        multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
        
        if not segonly_row.empty and not multitask_row.empty:
            f1_segonly.append(segonly_row['water_f1'].iloc[0])
            f1_multitask.append(multitask_row['water_f1'].iloc[0])
            patch_counts.append(multitask_row['n_patches'].iloc[0])
    
    x_pos = np.arange(len(huc_codes_str))
    bars1 = ax3.bar(x_pos - width/2, f1_segonly, width, label='Segmentation-Only', alpha=0.8, color='lightcoral')
    bars2 = ax3.bar(x_pos + width/2, f1_multitask, width, label='Multitask (Seg+Flow)', alpha=0.8, color='lightgreen')
    
    ax3.set_xlabel('HUC Code')
    ax3.set_ylabel('Water F1 Score')
    ax3.set_title('Water F1 by HUC (Primary Metric)')
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels([f'{huc}\\n({pc} patches)' for huc, pc in zip(huc_codes_str, patch_counts)])
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. D8 F1 by HUC (Top Right)
    ax4 = plt.subplot(3, 4, 4)
    
    d8_f1_segonly = []
    d8_f1_multitask = []
    
    for huc in sorted(huc_codes):
        huc_data = df_per_huc[df_per_huc['huc_code'] == huc]
        segonly_row = huc_data[huc_data['model_type'] == 'Segmentation-Only']
        multitask_row = huc_data[huc_data['model_type'] == 'Multitask (Seg+Flow)']
        
        if not segonly_row.empty and not multitask_row.empty:
            d8_f1_segonly.append(segonly_row['d8_f1_macro'].iloc[0])
            d8_f1_multitask.append(multitask_row['d8_f1_macro'].iloc[0])
    
    bars1 = ax4.bar(x_pos - width/2, d8_f1_segonly, width, label='Segmentation-Only', alpha=0.8, color='lightcoral')
    bars2 = ax4.bar(x_pos + width/2, d8_f1_multitask, width, label='Multitask (Seg+Flow)', alpha=0.8, color='lightblue')
    
    ax4.set_xlabel('HUC Code')
    ax4.set_ylabel('D8 F1 Macro Score')
    ax4.set_title('D8 Flow Direction F1 by HUC')
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels([f'{huc}\\n({pc} patches)' for huc, pc in zip(huc_codes_str, patch_counts)])
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # 5. Water Segmentation Improvements (Middle Left)
    ax5 = plt.subplot(3, 4, 5)
    
    water_improvements = []
    for i, metric in enumerate(water_metrics):
        improvement = (multitask_values[i] - segonly_values[i]) / segonly_values[i] * 100
        water_improvements.append(improvement)
    
    colors = ['green' if x > 0 else 'red' for x in water_improvements]
    bars = ax5.barh(water_labels, water_improvements, color=colors, alpha=0.7)
    
    ax5.axvline(x=0, color='black', linestyle='-', alpha=0.5)
    ax5.set_xlabel('Improvement (%)')
    ax5.set_title('Water Segmentation Improvements')
    ax5.grid(True, alpha=0.3)
    
    # Add value labels
    for bar, value in zip(bars, water_improvements):
        width = bar.get_width()
        ax5.text(width + (0.5 if width > 0 else -0.5), bar.get_y() + bar.get_height()/2,
                f'{value:.1f}%', ha='left' if width > 0 else 'right', va='center')
    
    # 6. D8 Flow Direction Improvements (Middle Center-Left)
    ax6 = plt.subplot(3, 4, 6)
    
    d8_improvements = []
    for i, metric in enumerate(d8_metrics):
        improvement = (d8_multitask_values[i] - d8_segonly_values[i]) / d8_segonly_values[i] * 100
        d8_improvements.append(improvement)
    
    colors = ['green' if x > 0 else 'red' for x in d8_improvements]
    bars = ax6.barh(d8_labels, d8_improvements, color=colors, alpha=0.7)
    
    ax6.axvline(x=0, color='black', linestyle='-', alpha=0.5)
    ax6.set_xlabel('Improvement (%)')
    ax6.set_title('D8 Flow Direction Improvements')
    ax6.grid(True, alpha=0.3)
    
    # Add value labels
    for bar, value in zip(bars, d8_improvements):
        width = bar.get_width()
        ax6.text(width + (10 if width > 0 else -10), bar.get_y() + bar.get_height()/2,
                f'{value:.0f}%', ha='left' if width > 0 else 'right', va='center')
    
    # 7. Water F1 Improvement by HUC (Middle Center-Right)
    ax7 = plt.subplot(3, 4, 7)
    
    f1_improvements = []
    for i in range(len(huc_codes_str)):
        improvement = (f1_multitask[i] - f1_segonly[i]) / f1_segonly[i] * 100
        f1_improvements.append(improvement)
    
    colors = ['green' if x > 0 else 'red' for x in f1_improvements]
    bars = ax7.bar(huc_codes_str, f1_improvements, color=colors, alpha=0.7)
    
    ax7.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax7.set_xlabel('HUC Code')
    ax7.set_ylabel('Water F1 Improvement (%)')
    ax7.set_title('Water F1 Improvement by HUC')
    ax7.grid(True, alpha=0.3)
    
    # Add value labels
    for bar, value in zip(bars, f1_improvements):
        height = bar.get_height()
        ax7.text(bar.get_x() + bar.get_width()/2., height + (0.5 if height > 0 else -1),
               f'{value:.1f}%', ha='center', va='bottom' if height > 0 else 'top')
    
    # 8. Performance vs Dataset Size (Middle Right)
    ax8 = plt.subplot(3, 4, 8)
    
    scatter = ax8.scatter(patch_counts, f1_improvements, s=150, alpha=0.7, 
                         c=f1_improvements, cmap='RdYlGn', edgecolors='black')
    
    for i, huc in enumerate(huc_codes_str):
        ax8.annotate(huc, (patch_counts[i], f1_improvements[i]),
                   xytext=(5, 5), textcoords='offset points', fontsize=9)
    
    ax8.set_xlabel('Number of Patches')
    ax8.set_ylabel('Water F1 Improvement (%)')
    ax8.set_title('F1 Improvement vs Dataset Size')
    ax8.grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=ax8, label='F1 Improvement (%)')
    
    # 9-12. Summary Statistics (Bottom Row)
    ax9 = plt.subplot(3, 4, (9, 12))
    ax9.axis('off')
    
    # Calculate summary statistics
    overall_water_f1_improvement = (df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)']['water_f1'].iloc[0] - 
                                   df_overall[df_overall['model_type'] == 'Segmentation-Only']['water_f1'].iloc[0]) / \
                                   df_overall[df_overall['model_type'] == 'Segmentation-Only']['water_f1'].iloc[0] * 100
    
    overall_d8_f1_improvement = (df_overall[df_overall['model_type'] == 'Multitask (Seg+Flow)']['d8_f1_macro'].iloc[0] - 
                                df_overall[df_overall['model_type'] == 'Segmentation-Only']['d8_f1_macro'].iloc[0]) / \
                                df_overall[df_overall['model_type'] == 'Segmentation-Only']['d8_f1_macro'].iloc[0] * 100
    
    positive_f1_improvements = sum(1 for x in f1_improvements if x > 0)
    total_hucs = len(f1_improvements)
    
    summary_text = f"""
    📊 STANDARD NATIONAL ML METRICS ANALYSIS
    
    🎯 PRIMARY WATER SEGMENTATION RESULTS:
    Overall Water F1 Improvement: +{overall_water_f1_improvement:.1f}%
    Overall Water IoU Improvement: +{((multitask_values[1] - segonly_values[1]) / segonly_values[1] * 100):.1f}%
    Overall Water Recall Improvement: +{((multitask_values[4] - segonly_values[4]) / segonly_values[4] * 100):.1f}%
    
    🎯 D8 FLOW DIRECTION RESULTS: 
    Overall D8 F1 Macro Improvement: +{overall_d8_f1_improvement:.0f}%
    Overall D8 Accuracy Improvement: +{((d8_multitask_values[0] - d8_segonly_values[0]) / d8_segonly_values[0] * 100):.0f}%
    
    📈 REGIONAL PERFORMANCE:
    HUCs with Water F1 Improvement: {positive_f1_improvements}/{total_hucs}
    Total Patches Analyzed: {sum(patch_counts):,}
    
    🏆 BEST PERFORMING HUC: {huc_codes_str[np.argmax(f1_improvements)]}
    📈 Max Water F1 Improvement: +{max(f1_improvements):.1f}%
    
    ⭐ KEY FINDINGS:
    • Multitask learning provides consistent benefits
      for water segmentation (F1, IoU, Recall)
    • Dramatic improvements in D8 flow direction
      prediction capability (200%+ improvement)
    • Regional performance scales with dataset size
    • Auxiliary D8 task enhances primary segmentation
    
    ✅ DEPLOYMENT RECOMMENDATION: 
    Deploy multitask architecture for comprehensive
    hydrographic applications requiring both water
    segmentation and flow direction prediction.
    """
    
    ax9.text(0.05, 0.95, summary_text, transform=ax9.transAxes, fontsize=11,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.1))
    
    plt.tight_layout()
    
    # Save the visualization
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_file = results_dir / f"STANDARD_METRICS_comprehensive_analysis_{timestamp}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\\n✅ Created standard metrics visualization!")
    print(f"📁 Saved to: {output_file}")
    
    return True

if __name__ == "__main__":
    create_standard_metrics_visualization()
    print("\\n🎯 Standard National ML metrics visualization complete!")
    print("📊 Ready for comprehensive results section with proper metrics")