#!/usr/bin/env python3
"""
Visualize Dummy Results for Transfer/Robustness Analysis

This script creates realistic visualizations from the generated dummy data
to demonstrate what the final analysis outputs would look like.
"""

import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def create_transfer_analysis_visualization(results_dir):
    """Create transfer analysis visualization from dummy data"""
    
    results_dir = Path(results_dir)
    transfer_file = results_dir / "transfer_analysis_results.json"
    
    if not transfer_file.exists():
        print(f"❌ Transfer results not found: {transfer_file}")
        return
    
    # Load results
    with open(transfer_file, 'r') as f:
        results = json.load(f)
    
    # Prepare data for visualization
    terrain_types = ['low_relief', 'medium_relief', 'high_relief']
    models = ['alphaearth_only', 'dem_alphaearth']
    metrics = ['hydro_iou', 'hydro_f1', 'flow_accuracy']
    
    # Calculate summary statistics
    summary_data = {}
    for model in models:
        summary_data[model] = {}
        for terrain in terrain_types:
            if terrain in results[model] and len(results[model][terrain]) > 0:
                terrain_results = results[model][terrain]
                summary_data[model][terrain] = {
                    'hydro_iou_mean': np.mean([r['hydro_iou'] for r in terrain_results]),
                    'hydro_iou_std': np.std([r['hydro_iou'] for r in terrain_results]),
                    'hydro_f1_mean': np.mean([r['hydro_f1'] for r in terrain_results]),
                    'hydro_f1_std': np.std([r['hydro_f1'] for r in terrain_results]),
                    'flow_accuracy_mean': np.mean([r['flow_accuracy'] for r in terrain_results]),
                    'flow_accuracy_std': np.std([r['flow_accuracy'] for r in terrain_results]),
                    'count': len(terrain_results)
                }
    
    # Create visualization
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('🏔️ Transfer Analysis (4a): AE-only vs DEM+AE Across Terrain Types\\n(Synthetic Results)', fontsize=16, fontweight='bold')
    
    metric_names = ['Hydro IoU', 'Hydro F1', 'Flow Accuracy']
    metric_keys = ['hydro_iou', 'hydro_f1', 'flow_accuracy']
    colors = ['#FF6B6B', '#4ECDC4']  # Red for AE-only, Teal for DEM+AE
    
    x_pos = np.arange(len(terrain_types))
    width = 0.35
    
    for i, (metric_name, metric_key) in enumerate(zip(metric_names, metric_keys)):
        ax = axes[i]
        
        # Prepare data for plotting
        ae_means = []
        ae_stds = []
        dem_means = []
        dem_stds = []
        
        for terrain in terrain_types:
            if terrain in summary_data['alphaearth_only']:
                ae_means.append(summary_data['alphaearth_only'][terrain][f'{metric_key}_mean'])
                ae_stds.append(summary_data['alphaearth_only'][terrain][f'{metric_key}_std'])
            else:
                ae_means.append(0)
                ae_stds.append(0)
                
            if terrain in summary_data['dem_alphaearth']:
                dem_means.append(summary_data['dem_alphaearth'][terrain][f'{metric_key}_mean'])
                dem_stds.append(summary_data['dem_alphaearth'][terrain][f'{metric_key}_std'])
            else:
                dem_means.append(0)
                dem_stds.append(0)
        
        # Create bars
        bars1 = ax.bar(x_pos - width/2, ae_means, width, yerr=ae_stds, 
                      label='AE-only', color=colors[0], alpha=0.8, capsize=5)
        bars2 = ax.bar(x_pos + width/2, dem_means, width, yerr=dem_stds,
                      label='DEM+AE', color=colors[1], alpha=0.8, capsize=5)
        
        # Customize plot
        ax.set_xlabel('Terrain Type')
        ax.set_ylabel(metric_name)
        ax.set_title(f'{metric_name} by Terrain Type')
        ax.set_xticks(x_pos)
        ax.set_xticklabels([t.replace('_', ' ').title() for t in terrain_types])
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                           f'{height:.3f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    # Save visualization
    viz_file = results_dir / "transfer_analysis_4a.png"
    plt.savefig(viz_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"📊 Transfer visualization saved: {viz_file}")
    return viz_file

def create_robustness_analysis_visualization(results_dir):
    """Create robustness analysis visualization from dummy data"""
    
    results_dir = Path(results_dir)
    robustness_file = results_dir / "robustness_analysis_results.json"
    
    if not robustness_file.exists():
        print(f"❌ Robustness results not found: {robustness_file}")
        return
    
    # Load results
    with open(robustness_file, 'r') as f:
        results = json.load(f)
    
    # Extract metrics
    clean_metrics = results['clean']['metrics']
    masked_metrics = results['masked']['metrics']
    
    # Organize data
    clean_hydro_iou = [m['hydro_iou'] for m in clean_metrics]
    masked_hydro_iou = [m['hydro_iou'] for m in masked_metrics]
    clean_hydro_f1 = [m['hydro_f1'] for m in clean_metrics]
    masked_hydro_f1 = [m['hydro_f1'] for m in masked_metrics]
    clean_flow_acc = [m['flow_accuracy'] for m in clean_metrics]
    masked_flow_acc = [m['flow_accuracy'] for m in masked_metrics]
    
    # Create visualization
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('☁️ Robustness Analysis (4b): Clean vs Masked Optical (30% Cloud Coverage)\\n(Synthetic Results)', 
                fontsize=16, fontweight='bold')
    
    # Box plots for each metric
    metric_data = [
        (clean_hydro_iou, masked_hydro_iou, 'Hydro IoU'),
        (clean_hydro_f1, masked_hydro_f1, 'Hydro F1'),
        (clean_flow_acc, masked_flow_acc, 'Flow Accuracy')
    ]
    
    for i, (clean_data, masked_data, name) in enumerate(metric_data):
        row = i // 2
        col = i % 2
        
        ax = axes[row, col]
        
        # Box plot
        bp = ax.boxplot([clean_data, masked_data], labels=['Clean', 'Masked'], 
                       patch_artist=True, notch=True)
        
        # Color the boxes
        bp['boxes'][0].set_facecolor('#4ECDC4')  # Clean: Teal
        bp['boxes'][1].set_facecolor('#FF6B6B')  # Masked: Red
        
        ax.set_title(f'{name} Distribution')
        ax.set_ylabel(name)
        ax.grid(True, alpha=0.3)
        
        # Add mean values as text
        clean_mean = np.mean(clean_data)
        masked_mean = np.mean(masked_data)
        degradation = (1 - masked_mean / clean_mean) * 100
        
        ax.text(0.5, 0.95, f'Clean: {clean_mean:.3f}\\nMasked: {masked_mean:.3f}\\nDegradation: {degradation:.1f}%',
               transform=ax.transAxes, ha='center', va='top', 
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # Summary statistics table in the fourth subplot
    ax = axes[1, 1]
    ax.axis('off')
    
    # Calculate summary statistics
    summary_stats = []
    metric_names = ['Hydro IoU', 'Hydro F1', 'Flow Accuracy']
    metric_data_list = [
        (clean_hydro_iou, masked_hydro_iou),
        (clean_hydro_f1, masked_hydro_f1), 
        (clean_flow_acc, masked_flow_acc)
    ]
    
    for (clean_data, masked_data), name in zip(metric_data_list, metric_names):
        clean_mean = np.mean(clean_data)
        clean_std = np.std(clean_data)
        masked_mean = np.mean(masked_data)
        masked_std = np.std(masked_data)
        degradation = (1 - masked_mean / clean_mean) * 100
        
        summary_stats.append([
            name,
            f'{clean_mean:.3f}±{clean_std:.3f}',
            f'{masked_mean:.3f}±{masked_std:.3f}',
            f'{degradation:.1f}%'
        ])
    
    # Create table
    table_data = [['Metric', 'Clean', 'Masked', 'Degradation']] + summary_stats
    table = ax.table(cellText=table_data, cellLoc='center', loc='center',
                    colWidths=[0.25, 0.25, 0.25, 0.25])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    
    # Style the header row
    for i in range(4):
        table[(0, i)].set_facecolor('#E8E8E8')
        table[(0, i)].set_text_props(weight='bold')
    
    ax.set_title('📊 Summary Statistics', fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save visualization
    viz_file = results_dir / "robustness_analysis_4b.png"
    plt.savefig(viz_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"📊 Robustness visualization saved: {viz_file}")
    return viz_file

def print_summary_statistics(results_dir):
    """Print summary statistics from dummy results"""
    
    results_dir = Path(results_dir)
    
    print("📊 DUMMY RESULTS SUMMARY")
    print("=" * 50)
    
    # Transfer Analysis Summary
    transfer_file = results_dir / "transfer_analysis_results.json"
    if transfer_file.exists():
        with open(transfer_file, 'r') as f:
            transfer_results = json.load(f)
        
        print("🏔️  TRANSFER ANALYSIS (4a)")
        print("-" * 30)
        
        terrain_types = ['low_relief', 'medium_relief', 'high_relief']
        for terrain in terrain_types:
            print(f"\\n{terrain.replace('_', ' ').title()}:")
            
            if terrain in transfer_results['alphaearth_only'] and len(transfer_results['alphaearth_only'][terrain]) > 0:
                ae_data = transfer_results['alphaearth_only'][terrain]
                dem_data = transfer_results['dem_alphaearth'][terrain]
                
                ae_iou = np.mean([r['hydro_iou'] for r in ae_data])
                dem_iou = np.mean([r['hydro_iou'] for r in dem_data])
                improvement = (dem_iou / ae_iou - 1) * 100
                
                print(f"  Patches: {len(ae_data)}")
                print(f"  AE-only Hydro IoU: {ae_iou:.3f}")
                print(f"  DEM+AE Hydro IoU: {dem_iou:.3f}")
                print(f"  DEM Improvement: {improvement:+.1f}%")
            else:
                print("  No data available")
    
    # Robustness Analysis Summary  
    robustness_file = results_dir / "robustness_analysis_results.json"
    if robustness_file.exists():
        with open(robustness_file, 'r') as f:
            robustness_results = json.load(f)
        
        print("\\n☁️  ROBUSTNESS ANALYSIS (4b)")
        print("-" * 30)
        
        clean_metrics = robustness_results['clean']['metrics']
        masked_metrics = robustness_results['masked']['metrics']
        
        clean_iou = np.mean([m['hydro_iou'] for m in clean_metrics])
        masked_iou = np.mean([m['hydro_iou'] for m in masked_metrics])
        degradation = (1 - masked_iou / clean_iou) * 100
        
        print(f"Analyzed patches: {len(clean_metrics)}")
        print(f"Cloud coverage: {robustness_results['cloud_coverage']*100}%")
        print(f"Clean Hydro IoU: {clean_iou:.3f}")
        print(f"Masked Hydro IoU: {masked_iou:.3f}")
        print(f"Performance degradation: {degradation:.1f}%")
        
        if degradation < 15:
            assessment = "🟢 ROBUST"
        elif degradation < 25:
            assessment = "🟡 MODERATE"
        else:
            assessment = "🔴 SENSITIVE"
        
        print(f"Assessment: {assessment}")

def main():
    """Create visualizations for dummy results"""
    
    # Find the most recent dummy results directory
    results_base = Path("results")
    dummy_dirs = list(results_base.glob("dummy_results_*"))
    
    if not dummy_dirs:
        print("❌ No dummy results directories found")
        print("💡 Run generate_dummy_results.py first")
        return
    
    # Use the most recent
    latest_dir = max(dummy_dirs, key=lambda x: x.stat().st_mtime)
    
    print(f"📁 Using results from: {latest_dir}")
    print()
    
    # Create visualizations
    transfer_viz = create_transfer_analysis_visualization(latest_dir)
    robustness_viz = create_robustness_analysis_visualization(latest_dir)
    
    print()
    print_summary_statistics(latest_dir)
    
    print("\\n🎉 DUMMY VISUALIZATIONS COMPLETE!")
    print("=" * 40)
    print("Generated files:")
    print(f"• {transfer_viz}")
    print(f"• {robustness_viz}")
    print(f"• {latest_dir / 'analysis_summary.md'}")

if __name__ == "__main__":
    main()