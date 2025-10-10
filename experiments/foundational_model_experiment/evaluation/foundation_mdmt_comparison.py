#!/usr/bin/env python3
"""
Foundation vs MDMT Model Comparison Analysis
===========================================

Comprehensive analysis script that compares foundation models (Prithvi, Clay) 
with MDMT variants using standardized evaluation metrics.

Features:
- Load and merge results from both evaluation systems
- Statistical significance testing
- Performance profiling comparison
- Visualization and reporting
- Publication-ready tables and figures
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import json
from pathlib import Path
from datetime import datetime
import argparse
import logging
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FoundationMDMTComparison:
    """
    Comprehensive comparison framework for Foundation Models vs MDMT variants.
    """
    
    def __init__(self, output_dir: str = None):
        self.output_dir = Path(output_dir or "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/comparison_results")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define standard metrics for comparison
        self.water_metrics = ['water_dice', 'water_iou', 'water_accuracy', 'water_precision', 'water_recall', 'water_f1']
        self.d8_metrics = ['d8_accuracy', 'd8_precision_macro', 'd8_recall_macro', 'd8_f1_macro']
        
        self.results = {}
        
    def load_foundation_results(self, results_dir: str) -> pd.DataFrame:
        """Load foundation model results from CSV files."""
        
        results_path = Path(results_dir)
        if not results_path.exists():
            logger.error(f"Foundation results directory not found: {results_dir}")
            return pd.DataFrame()
        
        # Find all CSV files for foundation models
        csv_files = []
        for pattern in ['prithvi_*.csv', 'clay_*.csv']:
            csv_files.extend(list(results_path.glob(pattern)))
        
        if not csv_files:
            logger.warning(f"No foundation model results found in {results_dir}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(csv_files)} foundation model result files")
        
        # Load and combine all CSV files
        all_results = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"Loaded {len(df)} records from {csv_file.name}")
                all_results.append(df)
            except Exception as e:
                logger.error(f"Error loading {csv_file}: {e}")
                continue
        
        if not all_results:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_results, ignore_index=True)
        logger.info(f"Combined foundation results: {len(combined_df)} total records")
        
        return combined_df
    
    def load_mdmt_results(self, results_dir: str) -> pd.DataFrame:
        """Load MDMT variant results from CSV files."""
        
        results_path = Path(results_dir)
        if not results_path.exists():
            logger.error(f"MDMT results directory not found: {results_dir}")
            return pd.DataFrame()
        
        # Find all CSV files for MDMT variants
        csv_files = list(results_path.glob('*.csv'))
        
        if not csv_files:
            logger.warning(f"No MDMT results found in {results_dir}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(csv_files)} MDMT result files")
        
        # Load and combine all CSV files
        all_results = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                # Filter out foundation model results if they're mixed in
                if 'model_variant' in df.columns:
                    df = df[~df['model_variant'].str.contains('foundation', case=False, na=False)]
                if len(df) > 0:
                    logger.info(f"Loaded {len(df)} MDMT records from {csv_file.name}")
                    all_results.append(df)
            except Exception as e:
                logger.error(f"Error loading {csv_file}: {e}")
                continue
        
        if not all_results:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_results, ignore_index=True)
        logger.info(f"Combined MDMT results: {len(combined_df)} total records")
        
        return combined_df
    
    def standardize_model_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize model variant names for consistent comparison."""
        
        df = df.copy()
        
        # Create standardized model categories
        def categorize_model(variant):
            if pd.isna(variant):
                return 'unknown'
            
            variant = str(variant).lower()
            
            # Foundation models
            if 'prithvi' in variant:
                return 'Prithvi Foundation'
            elif 'clay' in variant:
                return 'Clay Foundation'
            
            # MDMT variants
            elif 'alphaearth' in variant and 'dem' not in variant:
                return 'MDMT AlphaEarth Only'
            elif 'dem' in variant and 'alphaearth' in variant:
                return 'MDMT DEM+AlphaEarth'
            elif 'dem' in variant and 'optical' in variant:
                return 'MDMT DEM+Optical'
            elif 'dem' in variant and 'sar' in variant:
                return 'MDMT DEM+SAR'
            elif 'dem' in variant and 'thermal' in variant:
                return 'MDMT DEM+Thermal'
            elif 'landsat6b' in variant or 'optical' in variant:
                return 'MDMT Landsat6B'
            elif 'dem' in variant and ('only' in variant or len(variant.split('_')) == 2):
                return 'MDMT DEM Only'
            else:
                return f'MDMT {variant.replace("mdmt_", "").replace("_", " ").title()}'
        
        df['model_category'] = df['model_variant'].apply(categorize_model)
        
        return df
    
    def compute_model_summaries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute summary statistics for each model variant."""
        
        # Group by model category
        summary_stats = []
        
        for model_cat in df['model_category'].unique():
            model_data = df[df['model_category'] == model_cat]
            
            stats_dict = {
                'model_category': model_cat,
                'n_runs': len(model_data['run_name'].unique()) if 'run_name' in model_data.columns else 1,
                'n_hucs': len(model_data['huc_id'].unique()) if 'huc_id' in model_data.columns else len(model_data),
                'total_samples': model_data['total_samples'].sum() if 'total_samples' in model_data.columns else len(model_data)
            }
            
            # Compute mean, std, and median for each metric
            for metric in self.water_metrics + self.d8_metrics:
                if metric in model_data.columns:
                    values = model_data[metric].dropna()
                    if len(values) > 0:
                        stats_dict[f'{metric}_mean'] = values.mean()
                        stats_dict[f'{metric}_std'] = values.std()
                        stats_dict[f'{metric}_median'] = values.median()
                        stats_dict[f'{metric}_min'] = values.min()
                        stats_dict[f'{metric}_max'] = values.max()
                    else:
                        stats_dict[f'{metric}_mean'] = np.nan
                        stats_dict[f'{metric}_std'] = np.nan
                        stats_dict[f'{metric}_median'] = np.nan
                        stats_dict[f'{metric}_min'] = np.nan
                        stats_dict[f'{metric}_max'] = np.nan
            
            summary_stats.append(stats_dict)
        
        return pd.DataFrame(summary_stats)
    
    def perform_statistical_tests(self, foundation_df: pd.DataFrame, mdmt_df: pd.DataFrame) -> Dict:
        """Perform statistical significance tests between foundation and MDMT models."""
        
        test_results = {}
        
        # Get unique model categories
        foundation_models = foundation_df['model_category'].unique()
        mdmt_models = mdmt_df['model_category'].unique()
        
        logger.info(f"Comparing {len(foundation_models)} foundation models with {len(mdmt_models)} MDMT variants")
        
        # Test each foundation model against each MDMT variant
        for foundation_model in foundation_models:
            foundation_data = foundation_df[foundation_df['model_category'] == foundation_model]
            
            for mdmt_model in mdmt_models:
                mdmt_data = mdmt_df[mdmt_df['model_category'] == mdmt_model]
                
                comparison_key = f"{foundation_model}_vs_{mdmt_model}"
                test_results[comparison_key] = {}
                
                # Test each metric
                for metric in self.water_metrics + self.d8_metrics:
                    if metric in foundation_data.columns and metric in mdmt_data.columns:
                        foundation_values = foundation_data[metric].dropna()
                        mdmt_values = mdmt_data[metric].dropna()
                        
                        if len(foundation_values) > 1 and len(mdmt_values) > 1:
                            # Perform Mann-Whitney U test (non-parametric)
                            statistic, p_value = stats.mannwhitneyu(
                                foundation_values, mdmt_values, alternative='two-sided'
                            )
                            
                            # Effect size (Cohen's d approximation)
                            effect_size = (foundation_values.mean() - mdmt_values.mean()) / \
                                        np.sqrt((foundation_values.var() + mdmt_values.var()) / 2)
                            
                            test_results[comparison_key][metric] = {
                                'statistic': float(statistic),
                                'p_value': float(p_value),
                                'significant': p_value < 0.05,
                                'effect_size': float(effect_size),
                                'foundation_mean': float(foundation_values.mean()),
                                'mdmt_mean': float(mdmt_values.mean()),
                                'foundation_better': foundation_values.mean() > mdmt_values.mean()
                            }
        
        return test_results
    
    def create_comparison_visualizations(self, foundation_df: pd.DataFrame, mdmt_df: pd.DataFrame):
        """Create comprehensive comparison visualizations."""
        
        # Combine datasets for plotting
        foundation_df['model_type'] = 'Foundation Model'
        mdmt_df['model_type'] = 'MDMT Variant'
        combined_df = pd.concat([foundation_df, mdmt_df], ignore_index=True)
        
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # 1. Key metrics comparison boxplot
        self._create_metrics_boxplot(combined_df)
        
        # 2. Model performance radar chart
        self._create_radar_chart(foundation_df, mdmt_df)
        
        # 3. Detailed performance heatmap
        self._create_performance_heatmap(combined_df)
        
        # 4. Statistical significance matrix
        self._create_significance_matrix(foundation_df, mdmt_df)
        
        logger.info(f"Visualizations saved to {self.output_dir}")
    
    def _create_metrics_boxplot(self, df: pd.DataFrame):
        """Create boxplot comparison of key metrics."""
        
        key_metrics = ['water_dice', 'water_iou', 'd8_accuracy']
        
        fig, axes = plt.subplots(1, len(key_metrics), figsize=(15, 6))
        
        for i, metric in enumerate(key_metrics):
            if metric in df.columns:
                sns.boxplot(data=df, x='model_type', y=metric, ax=axes[i])
                axes[i].set_title(f'{metric.replace("_", " ").title()}')
                axes[i].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'metrics_comparison_boxplot.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_radar_chart(self, foundation_df: pd.DataFrame, mdmt_df: pd.DataFrame):
        """Create radar chart comparing foundation vs MDMT performance."""
        
        # Compute mean performance for each model type
        foundation_summary = self.compute_model_summaries(foundation_df)
        mdmt_summary = self.compute_model_summaries(mdmt_df)
        
        # Select key metrics for radar chart
        radar_metrics = ['water_dice_mean', 'water_iou_mean', 'water_f1_mean', 'd8_accuracy_mean']
        
        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
        
        # Get mean values across all foundation and MDMT models
        foundation_means = foundation_summary[radar_metrics].mean()
        mdmt_means = mdmt_summary[radar_metrics].mean()
        
        # Prepare data for radar chart
        angles = np.linspace(0, 2 * np.pi, len(radar_metrics), endpoint=False)
        angles = np.concatenate((angles, [angles[0]]))  # Complete the circle
        
        foundation_values = np.concatenate((foundation_means.values, [foundation_means.values[0]]))
        mdmt_values = np.concatenate((mdmt_means.values, [mdmt_means.values[0]]))
        
        # Plot
        ax.plot(angles, foundation_values, 'o-', linewidth=2, label='Foundation Models')
        ax.fill(angles, foundation_values, alpha=0.25)
        
        ax.plot(angles, mdmt_values, 's-', linewidth=2, label='MDMT Variants')
        ax.fill(angles, mdmt_values, alpha=0.25)
        
        # Customize
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels([metric.replace('_mean', '').replace('_', ' ').title() for metric in radar_metrics])
        ax.set_ylim(0, 1)
        ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        ax.set_title('Foundation Models vs MDMT Variants\nPerformance Comparison', size=16, pad=20)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'performance_radar_chart.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_performance_heatmap(self, df: pd.DataFrame):
        """Create performance heatmap for all models."""
        
        # Compute summary for heatmap
        summary_df = self.compute_model_summaries(df)
        
        # Select metrics for heatmap
        heatmap_metrics = [f'{metric}_mean' for metric in self.water_metrics + self.d8_metrics[:3]]
        available_metrics = [m for m in heatmap_metrics if m in summary_df.columns]
        
        if not available_metrics:
            logger.warning("No metrics available for heatmap")
            return
        
        # Prepare data
        heatmap_data = summary_df.set_index('model_category')[available_metrics]
        
        # Create heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(heatmap_data, annot=True, cmap='RdYlBu_r', center=0.5, 
                   fmt='.3f', cbar_kws={'label': 'Performance Score'})
        plt.title('Model Performance Heatmap')
        plt.xlabel('Metrics')
        plt.ylabel('Model Variants')
        plt.xticks(rotation=45)
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'performance_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_significance_matrix(self, foundation_df: pd.DataFrame, mdmt_df: pd.DataFrame):
        """Create statistical significance matrix visualization."""
        
        # Perform statistical tests
        test_results = self.perform_statistical_tests(foundation_df, mdmt_df)
        
        if not test_results:
            logger.warning("No statistical test results available")
            return
        
        # Create significance matrix for water_dice metric
        foundation_models = foundation_df['model_category'].unique()
        mdmt_models = mdmt_df['model_category'].unique()
        
        significance_matrix = np.zeros((len(foundation_models), len(mdmt_models)))
        
        for i, foundation_model in enumerate(foundation_models):
            for j, mdmt_model in enumerate(mdmt_models):
                comparison_key = f"{foundation_model}_vs_{mdmt_model}"
                if comparison_key in test_results and 'water_dice' in test_results[comparison_key]:
                    p_value = test_results[comparison_key]['water_dice']['p_value']
                    significance_matrix[i, j] = -np.log10(p_value) if p_value > 0 else 10
        
        # Create heatmap
        plt.figure(figsize=(10, 6))
        sns.heatmap(significance_matrix, 
                   xticklabels=mdmt_models, 
                   yticklabels=foundation_models,
                   annot=True, fmt='.2f', cmap='Reds',
                   cbar_kws={'label': '-log10(p-value)'})
        plt.title('Statistical Significance Matrix\n(Water Dice Coefficient)')
        plt.xlabel('MDMT Variants')
        plt.ylabel('Foundation Models')
        plt.xticks(rotation=45)
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'significance_matrix.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def generate_comparison_report(self, foundation_df: pd.DataFrame, mdmt_df: pd.DataFrame):
        """Generate comprehensive comparison report."""
        
        # Compute summaries
        foundation_summary = self.compute_model_summaries(foundation_df)
        mdmt_summary = self.compute_model_summaries(mdmt_df)
        
        # Perform statistical tests
        statistical_tests = self.perform_statistical_tests(foundation_df, mdmt_df)
        
        # Generate markdown report
        report_path = self.output_dir / 'foundation_vs_mdmt_comparison_report.md'
        
        with open(report_path, 'w') as f:
            f.write("# Foundation Models vs MDMT Variants Comparison Report\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write(f"This report compares the performance of foundation models (Prithvi, Clay) ")
            f.write(f"against MDMT variants on water segmentation and flow direction prediction tasks.\n\n")
            
            f.write("### Dataset Overview\n")
            f.write(f"- **Foundation Models**: {len(foundation_df)} evaluations across {len(foundation_df['huc_id'].unique())} HUCs\n")
            f.write(f"- **MDMT Variants**: {len(mdmt_df)} evaluations across {len(mdmt_df['huc_id'].unique())} HUCs\n")
            f.write(f"- **Total Test Samples**: {foundation_df['total_samples'].sum() + mdmt_df['total_samples'].sum():,}\n\n")
            
            f.write("## Foundation Model Performance\n\n")
            f.write(foundation_summary.to_markdown(index=False))
            f.write("\n\n")
            
            f.write("## MDMT Variant Performance\n\n")
            f.write(mdmt_summary.to_markdown(index=False))
            f.write("\n\n")
            
            f.write("## Key Findings\n\n")
            self._write_key_findings(f, foundation_summary, mdmt_summary, statistical_tests)
            
            f.write("\n## Statistical Significance Tests\n\n")
            self._write_statistical_summary(f, statistical_tests)
            
            f.write("\n## Visualizations\n\n")
            f.write("- [Metrics Comparison Boxplot](metrics_comparison_boxplot.png)\n")
            f.write("- [Performance Radar Chart](performance_radar_chart.png)\n")
            f.write("- [Performance Heatmap](performance_heatmap.png)\n")
            f.write("- [Statistical Significance Matrix](significance_matrix.png)\n")
        
        # Save detailed results as JSON
        detailed_results = {
            'foundation_summary': foundation_summary.to_dict('records'),
            'mdmt_summary': mdmt_summary.to_dict('records'),
            'statistical_tests': statistical_tests,
            'metadata': {
                'generation_time': datetime.now().isoformat(),
                'foundation_records': len(foundation_df),
                'mdmt_records': len(mdmt_df)
            }
        }
        
        with open(self.output_dir / 'detailed_comparison_results.json', 'w') as f:
            json.dump(detailed_results, f, indent=2)
        
        logger.info(f"Comparison report generated: {report_path}")
        return report_path
    
    def _write_key_findings(self, f, foundation_summary, mdmt_summary, statistical_tests):
        """Write key findings section."""
        
        # Best performing models
        best_foundation_dice = foundation_summary.loc[foundation_summary['water_dice_mean'].idxmax()]
        best_mdmt_dice = mdmt_summary.loc[mdmt_summary['water_dice_mean'].idxmax()]
        
        f.write(f"### Performance Leaders\n\n")
        f.write(f"- **Best Foundation Model**: {best_foundation_dice['model_category']} ")
        f.write(f"(Dice: {best_foundation_dice['water_dice_mean']:.4f})\n")
        f.write(f"- **Best MDMT Variant**: {best_mdmt_dice['model_category']} ")
        f.write(f"(Dice: {best_mdmt_dice['water_dice_mean']:.4f})\n\n")
        
        # Overall comparison
        foundation_mean_dice = foundation_summary['water_dice_mean'].mean()
        mdmt_mean_dice = mdmt_summary['water_dice_mean'].mean()
        
        better_category = "Foundation Models" if foundation_mean_dice > mdmt_mean_dice else "MDMT Variants"
        difference = abs(foundation_mean_dice - mdmt_mean_dice)
        
        f.write(f"### Overall Comparison\n\n")
        f.write(f"- **Average Foundation Model Dice**: {foundation_mean_dice:.4f}\n")
        f.write(f"- **Average MDMT Variant Dice**: {mdmt_mean_dice:.4f}\n")
        f.write(f"- **Better Performing Category**: {better_category} (+{difference:.4f})\n\n")
    
    def _write_statistical_summary(self, f, statistical_tests):
        """Write statistical significance summary."""
        
        significant_comparisons = 0
        total_comparisons = 0
        
        for comparison, metrics in statistical_tests.items():
            for metric, results in metrics.items():
                total_comparisons += 1
                if results['significant']:
                    significant_comparisons += 1
        
        f.write(f"- **Total Comparisons**: {total_comparisons}\n")
        f.write(f"- **Significant Differences**: {significant_comparisons} ({100*significant_comparisons/total_comparisons:.1f}%)\n\n")
        
        # Highlight most significant comparisons
        f.write("### Most Significant Differences (p < 0.01)\n\n")
        
        highly_significant = []
        for comparison, metrics in statistical_tests.items():
            for metric, results in metrics.items():
                if results['p_value'] < 0.01:
                    highly_significant.append({
                        'comparison': comparison,
                        'metric': metric,
                        'p_value': results['p_value'],
                        'effect_size': results['effect_size'],
                        'foundation_better': results['foundation_better']
                    })
        
        highly_significant.sort(key=lambda x: x['p_value'])
        
        for item in highly_significant[:10]:  # Top 10 most significant
            direction = ">" if item['foundation_better'] else "<"
            f.write(f"- **{item['comparison']}** ({item['metric']}): p={item['p_value']:.2e}, ")
            f.write(f"Foundation {direction} MDMT (effect size: {item['effect_size']:.3f})\n")

def main():
    parser = argparse.ArgumentParser(description='Foundation vs MDMT Model Comparison Analysis')
    
    parser.add_argument('--foundation_results', type=str,
                       default='/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results',
                       help='Path to foundation model results directory')
    parser.add_argument('--mdmt_results', type=str,
                       default='/u/nathanj/national_ml/experiments/evaluation/ultra_fast_results',
                       help='Path to MDMT variant results directory')
    parser.add_argument('--output_dir', type=str,
                       default='/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/comparison_results',
                       help='Output directory for comparison results')
    
    args = parser.parse_args()
    
    logger.info("Starting Foundation vs MDMT Model Comparison Analysis")
    
    # Initialize comparison framework
    comparison = FoundationMDMTComparison(args.output_dir)
    
    # Load results
    logger.info("Loading foundation model results...")
    foundation_df = comparison.load_foundation_results(args.foundation_results)
    
    logger.info("Loading MDMT variant results...")
    mdmt_df = comparison.load_mdmt_results(args.mdmt_results)
    
    if foundation_df.empty:
        logger.error("No foundation model results found!")
        return
    
    if mdmt_df.empty:
        logger.error("No MDMT results found!")
        return
    
    # Standardize model names
    foundation_df = comparison.standardize_model_names(foundation_df)
    mdmt_df = comparison.standardize_model_names(mdmt_df)
    
    logger.info(f"Foundation models found: {foundation_df['model_category'].unique()}")
    logger.info(f"MDMT variants found: {mdmt_df['model_category'].unique()}")
    
    # Create visualizations
    logger.info("Creating comparison visualizations...")
    comparison.create_comparison_visualizations(foundation_df, mdmt_df)
    
    # Generate comprehensive report
    logger.info("Generating comparison report...")
    report_path = comparison.generate_comparison_report(foundation_df, mdmt_df)
    
    logger.info(f"Comparison analysis completed!")
    logger.info(f"Results saved to: {args.output_dir}")
    logger.info(f"Report available at: {report_path}")

if __name__ == "__main__":
    main()