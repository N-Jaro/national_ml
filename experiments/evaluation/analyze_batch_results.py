#!/usr/bin/env python3
"""
Analyze MDMT batch evaluation results and generate comprehensive reports.
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import json
from datetime import datetime

def load_batch_results(results_dir: str) -> pd.DataFrame:
    """Load all batch evaluation results."""
    
    # Find the most recent detailed results file
    detailed_files = list(Path(results_dir).glob("mdmt_batch_evaluation_detailed_*.csv"))
    
    if not detailed_files:
        print(f"❌ No batch evaluation results found in {results_dir}")
        return pd.DataFrame()
    
    # Use the most recent file
    latest_file = max(detailed_files, key=os.path.getctime)
    print(f"📊 Loading results from: {latest_file}")
    
    df = pd.read_csv(latest_file)
    return df


def create_performance_comparison(df: pd.DataFrame, output_dir: str):
    """Create performance comparison visualizations."""
    
    if df.empty:
        return
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('MDMT Variants Performance Comparison', fontsize=16, fontweight='bold')
    
    # 1. D8 Accuracy by Variant (Box Plot)
    sns.boxplot(data=df, x='Variant', y='D8_Accuracy', ax=axes[0,0])
    axes[0,0].set_title('D8 Flow Direction Accuracy')
    axes[0,0].set_ylabel('Accuracy')
    axes[0,0].tick_params(axis='x', rotation=45)
    
    # 2. Water IoU by Variant (Box Plot)  
    sns.boxplot(data=df, x='Variant', y='Water_IoU', ax=axes[0,1])
    axes[0,1].set_title('Water Segmentation IoU')
    axes[0,1].set_ylabel('IoU')
    axes[0,1].tick_params(axis='x', rotation=45)
    
    # 3. Water Dice by Variant (Box Plot)
    sns.boxplot(data=df, x='Variant', y='Water_Dice', ax=axes[1,0])
    axes[1,0].set_title('Water Segmentation Dice')
    axes[1,0].set_ylabel('Dice Score')
    axes[1,0].tick_params(axis='x', rotation=45)
    
    # 4. Performance Correlation
    metrics_df = df[['Water_IoU', 'Water_Dice', 'D8_Accuracy']].corr()
    sns.heatmap(metrics_df, annot=True, cmap='coolwarm', center=0, ax=axes[1,1])
    axes[1,1].set_title('Metrics Correlation')
    
    plt.tight_layout()
    
    # Save plot
    plot_file = os.path.join(output_dir, f"mdmt_performance_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"📈 Performance comparison saved: {plot_file}")


def create_ranking_report(df: pd.DataFrame, output_dir: str):
    """Create ranking report for all variants and runs."""
    
    if df.empty:
        return
    
    # Calculate rankings for each metric
    rankings = pd.DataFrame()
    
    # Rank by D8 Accuracy (higher is better)
    d8_ranked = df.sort_values('D8_Accuracy', ascending=False).reset_index(drop=True)
    d8_ranked['D8_Rank'] = range(1, len(d8_ranked) + 1)
    
    # Rank by Water IoU (higher is better)
    iou_ranked = df.sort_values('Water_IoU', ascending=False).reset_index(drop=True)
    iou_ranked['IoU_Rank'] = range(1, len(iou_ranked) + 1)
    
    # Rank by Water Dice (higher is better)
    dice_ranked = df.sort_values('Water_Dice', ascending=False).reset_index(drop=True)
    dice_ranked['Dice_Rank'] = range(1, len(dice_ranked) + 1)
    
    # Combine rankings
    merged = df.copy()
    merged = merged.merge(d8_ranked[['Variant', 'Run', 'D8_Rank']], on=['Variant', 'Run'])
    merged = merged.merge(iou_ranked[['Variant', 'Run', 'IoU_Rank']], on=['Variant', 'Run'])
    merged = merged.merge(dice_ranked[['Variant', 'Run', 'Dice_Rank']], on=['Variant', 'Run'])
    
    # Calculate average rank
    merged['Avg_Rank'] = (merged['D8_Rank'] + merged['IoU_Rank'] + merged['Dice_Rank']) / 3
    
    # Sort by average rank
    final_ranking = merged.sort_values('Avg_Rank').reset_index(drop=True)
    final_ranking['Overall_Rank'] = range(1, len(final_ranking) + 1)
    
    # Save ranking report
    ranking_file = os.path.join(output_dir, f"mdmt_ranking_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    final_ranking.to_csv(ranking_file, index=False)
    
    print(f"🏆 Ranking report saved: {ranking_file}")
    
    # Print top 10 performers
    print(f"\n🏆 TOP 10 MDMT PERFORMERS:")
    print("="*80)
    top_10 = final_ranking.head(10)
    for i, row in top_10.iterrows():
        print(f"{row['Overall_Rank']:2d}. {row['Variant']:15s} {row['Run']:5s} | "
              f"D8: {row['D8_Accuracy']:.3f} | IoU: {row['Water_IoU']:.3f} | "
              f"Dice: {row['Water_Dice']:.3f} | Avg Rank: {row['Avg_Rank']:.1f}")
    
    return final_ranking


def create_variant_summary(df: pd.DataFrame, output_dir: str):
    """Create per-variant summary statistics."""
    
    if df.empty:
        return
    
    # Group by variant and calculate statistics
    variant_stats = df.groupby('Variant').agg({
        'D8_Accuracy': ['count', 'mean', 'std', 'min', 'max'],
        'Water_IoU': ['mean', 'std', 'min', 'max'],
        'Water_Dice': ['mean', 'std', 'min', 'max']
    }).round(4)
    
    # Flatten column names
    variant_stats.columns = ['_'.join(col).strip() for col in variant_stats.columns]
    variant_stats = variant_stats.rename(columns={'D8_Accuracy_count': 'Num_Runs'})
    
    # Save summary
    summary_file = os.path.join(output_dir, f"mdmt_variant_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    variant_stats.to_csv(summary_file)
    
    print(f"📊 Variant summary saved: {summary_file}")
    
    # Print summary to console
    print(f"\n📊 VARIANT PERFORMANCE SUMMARY:")
    print("="*100)
    print(variant_stats)
    
    return variant_stats


def identify_best_runs(df: pd.DataFrame, output_dir: str):
    """Identify best performing run for each variant."""
    
    if df.empty:
        return
    
    best_runs = []
    
    for variant in df['Variant'].unique():
        variant_df = df[df['Variant'] == variant]
        
        # Find best run for each metric
        best_d8 = variant_df.loc[variant_df['D8_Accuracy'].idxmax()]
        best_iou = variant_df.loc[variant_df['Water_IoU'].idxmax()]
        best_dice = variant_df.loc[variant_df['Water_Dice'].idxmax()]
        
        best_runs.append({
            'Variant': variant,
            'Best_D8_Run': best_d8['Run'],
            'Best_D8_Accuracy': best_d8['D8_Accuracy'],
            'Best_D8_Checkpoint': best_d8['Checkpoint'],
            'Best_IoU_Run': best_iou['Run'],
            'Best_IoU_Score': best_iou['Water_IoU'],
            'Best_IoU_Checkpoint': best_iou['Checkpoint'],
            'Best_Dice_Run': best_dice['Run'],
            'Best_Dice_Score': best_dice['Water_Dice'],
            'Best_Dice_Checkpoint': best_dice['Checkpoint']
        })
    
    best_runs_df = pd.DataFrame(best_runs)
    
    # Save best runs
    best_runs_file = os.path.join(output_dir, f"mdmt_best_runs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    best_runs_df.to_csv(best_runs_file, index=False)
    
    print(f"🥇 Best runs saved: {best_runs_file}")
    
    # Print best runs
    print(f"\n🥇 BEST PERFORMING RUNS PER VARIANT:")
    print("="*100)
    for _, row in best_runs_df.iterrows():
        print(f"\n{row['Variant'].upper()}:")
        print(f"  Best D8:   {row['Best_D8_Run']} (Acc: {row['Best_D8_Accuracy']:.3f})")
        print(f"  Best IoU:  {row['Best_IoU_Run']} (IoU: {row['Best_IoU_Score']:.3f})")
        print(f"  Best Dice: {row['Best_Dice_Run']} (Dice: {row['Best_Dice_Score']:.3f})")
    
    return best_runs_df


def main():
    parser = argparse.ArgumentParser(description="Analyze MDMT Batch Evaluation Results")
    parser.add_argument("--results-dir", type=str,
                       default="/u/nathanj/national_ml/experiments/evaluation/batch_results",
                       help="Directory containing batch evaluation results")
    parser.add_argument("--output-dir", type=str,
                       help="Output directory for analysis reports (defaults to results-dir)")
    
    args = parser.parse_args()
    
    if not args.output_dir:
        args.output_dir = args.results_dir
    
    print("📊 MDMT Batch Results Analysis")
    print("="*50)
    
    # Load results
    df = load_batch_results(args.results_dir)
    
    if df.empty:
        print("❌ No results to analyze")
        return
    
    print(f"📈 Loaded {len(df)} evaluation results")
    print(f"🧪 Variants: {df['Variant'].nunique()}")
    print(f"🔬 Total runs: {df.groupby('Variant')['Run'].nunique().sum()}")
    
    # Create analysis reports
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 1. Performance comparison plots
    create_performance_comparison(df, args.output_dir)
    
    # 2. Ranking report
    create_ranking_report(df, args.output_dir)
    
    # 3. Variant summary statistics
    create_variant_summary(df, args.output_dir)
    
    # 4. Best runs identification
    identify_best_runs(df, args.output_dir)
    
    print(f"\n🎉 Analysis complete! Check {args.output_dir} for all reports.")


if __name__ == "__main__":
    main()