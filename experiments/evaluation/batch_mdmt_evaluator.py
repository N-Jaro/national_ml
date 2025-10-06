#!/usr/bin/env python3
"""
Batch MDMT Evaluator - Evaluates multiple runs for each experiment variant.
Supports all 6 MDMT variants with run-specific result saving.
"""

import os
import sys
import argparse
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

# Add project root to path
sys.path.append('/u/nathanj/national_ml')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_run_config(config_file: str) -> List[Tuple[str, str]]:
    """
    Load run configuration from file.
    Returns list of (run_name, checkpoint_path) tuples.
    """
    runs = []
    if not os.path.exists(config_file):
        logger.warning(f"Config file not found: {config_file}")
        return runs
    
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('|')
                if len(parts) == 2:
                    run_name, checkpoint_path = parts
                    runs.append((run_name.strip(), checkpoint_path.strip()))
                else:
                    logger.warning(f"Invalid line format: {line}")
    
    logger.info(f"Loaded {len(runs)} runs from {config_file}")
    return runs


def get_evaluator_script(variant: str, use_ultra_fast: bool = True) -> str:
    """Get the appropriate evaluator script for each variant."""
    
    # Ultra-fast evaluators (use these when available for better performance)
    ultra_fast_map = {
        'alphaearth': 'ultra_fast_alphaearth_evaluator.py',
        'dem_only': 'ultra_fast_dem_only_evaluator.py',
        'dem_thermal': 'ultra_fast_dem_thermal_evaluator.py',
        'dem_sar': 'ultra_fast_dem_sar_evaluator.py',
        'dem_optical': 'ultra_fast_dem_optical_evaluator.py',
        'dem_alphaearth': 'ultra_fast_dem_alphaearth_evaluator.py',
        'landsat6b': 'ultra_fast_landsat6b_evaluator.py'
    }
    
    # Original evaluators (fallback)
    original_map = {
        'alphaearth': 'simple_alphaearth_evaluator.py',
        'dem_only': 'ultra_fast_dem_only_evaluator.py',  # No simple version, use ultra-fast
        'dem_thermal': 'simple_dem_thermal_evaluator.py',
        'dem_sar': 'simple_dem_sar_evaluator.py',
        'dem_optical': 'simple_dem_optical_evaluator.py',
        'dem_alphaearth': 'simple_dem_alphaearth_evaluator.py',
        'landsat6b': 'simple_landsat6b_evaluator.py'
    }
    
    # Choose ultra-fast if available and requested, otherwise use original
    if use_ultra_fast and variant in ultra_fast_map:
        evaluator_script = ultra_fast_map[variant]
        # Check if ultra-fast evaluator exists
        script_path = os.path.join(os.path.dirname(__file__), evaluator_script)
        if os.path.exists(script_path):
            logger.info(f"Using ultra-fast evaluator for {variant}: {evaluator_script}")
            return evaluator_script
        else:
            logger.warning(f"Ultra-fast evaluator not found for {variant}, falling back to original")
    
    return original_map.get(variant)


def run_single_evaluation(variant: str, run_name: str, checkpoint_path: str, 
                         output_dir: str, limit_batches: int = None, use_ultra_fast: bool = True) -> Dict:
    """
    Run evaluation for a single run and return results.
    """
    import subprocess
    import json
    import tempfile
    
    evaluator_script = get_evaluator_script(variant, use_ultra_fast)
    if not evaluator_script:
        logger.error(f"Unknown variant: {variant}")
        return {}
    
    script_path = f"/u/nathanj/national_ml/experiments/evaluation/{evaluator_script}"
    
    # Create run-specific output directory
    run_output_dir = os.path.join(output_dir, f"{variant}_{run_name}")
    os.makedirs(run_output_dir, exist_ok=True)
    
    # Build command
    cmd = [
        "python", script_path,
        "--checkpoint", checkpoint_path,
        "--output-dir", run_output_dir,
        "--device", "auto"  # Use 'auto' to select device
    ]
    
    if limit_batches:
        cmd.extend(["--limit-batches", str(limit_batches)])
    
    logger.info(f"Running {variant} {run_name}: {checkpoint_path}")
    
    try:
        # Run the evaluator
        result = subprocess.run(cmd, capture_output=True, text=True, cwd="/u/nathanj/national_ml")
        
        if result.returncode != 0:
            logger.error(f"Evaluation failed for {variant} {run_name}")
            logger.error(f"Error: {result.stderr}")
            return {}
        
        # Parse results from the output
        output = result.stdout
        
        # Extract metrics from the output (this is a simplified parser)
        metrics = parse_evaluation_output(output, variant, run_name, checkpoint_path)
        
        # Save individual run results
        results_file = os.path.join(run_output_dir, f"{variant}_{run_name}_results.json")
        with open(results_file, 'w') as f:
            json.dump(metrics, f, indent=2)

        logger.info(f"✅ Completed {variant} {run_name} - Water IoU: {metrics.get('mean_water_iou', 'N/A'):.3f}, Water Dice: {metrics.get('mean_water_dice', 'N/A'):.3f} D8 Acc: {metrics.get('mean_d8_accuracy', 'N/A'):.3f}")

        return metrics
        
    except Exception as e:
        logger.error(f"Error running evaluation for {variant} {run_name}: {e}")
        return {}


def parse_evaluation_output(output: str, variant: str, run_name: str, checkpoint_path: str) -> Dict:
    """
    Parse evaluation output to extract comprehensive metrics.
    Enhanced to handle all water segmentation and D8 classification metrics.
    """
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'variant': variant,
        'run_name': run_name,
        'checkpoint_path': checkpoint_path,
        # Water segmentation metrics
        'mean_water_iou': 0.0,
        'mean_water_dice': 0.0,
        'mean_water_precision': 0.0,
        'mean_water_recall': 0.0,
        'mean_water_f1': 0.0,
        'mean_water_accuracy': 0.0,
        # D8 classification metrics
        'mean_d8_accuracy': 0.0,
        'mean_d8_precision_macro': 0.0,
        'mean_d8_recall_macro': 0.0,
        'mean_d8_f1_macro': 0.0,
        'mean_d8_precision_weighted': 0.0,
        'mean_d8_recall_weighted': 0.0,
        'mean_d8_f1_weighted': 0.0
    }
    
    lines = output.split('\n')
    for line in lines:
        line = line.strip()
        
        # Water segmentation metrics
        if 'Mean Water IoU:' in line:
            try:
                metrics['mean_water_iou'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean Water Dice:' in line:
            try:
                metrics['mean_water_dice'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean Water Precision:' in line:
            try:
                metrics['mean_water_precision'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean Water Recall:' in line:
            try:
                metrics['mean_water_recall'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean Water F1-Score:' in line:
            try:
                metrics['mean_water_f1'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean Water Accuracy:' in line:
            try:
                metrics['mean_water_accuracy'] = float(line.split(':')[1].strip())
            except:
                pass
        
        # D8 classification metrics
        elif 'Mean D8 Accuracy:' in line:
            try:
                metrics['mean_d8_accuracy'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean D8 Precision (Macro):' in line:
            try:
                metrics['mean_d8_precision_macro'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean D8 Recall (Macro):' in line:
            try:
                metrics['mean_d8_recall_macro'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean D8 F1-Score (Macro):' in line:
            try:
                metrics['mean_d8_f1_macro'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean D8 Precision (Weighted):' in line:
            try:
                metrics['mean_d8_precision_weighted'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean D8 Recall (Weighted):' in line:
            try:
                metrics['mean_d8_recall_weighted'] = float(line.split(':')[1].strip())
            except:
                pass
        elif 'Mean D8 F1-Score (Weighted):' in line:
            try:
                metrics['mean_d8_f1_weighted'] = float(line.split(':')[1].strip())
            except:
                pass
    
    return metrics


def create_summary_report(all_results: Dict[str, List[Dict]], output_dir: str):
    """Create comprehensive summary report across all variants and runs."""
    
    summary_data = []
    
    for variant, results in all_results.items():
        if not results:
            continue
            
        for result in results:
            summary_data.append({
                'Variant': variant,
                'Run': result['run_name'],
                # Water segmentation metrics
                'Water_IoU': result['mean_water_iou'],
                'Water_Dice': result['mean_water_dice'],
                'Water_Precision': result['mean_water_precision'],
                'Water_Recall': result['mean_water_recall'],
                'Water_F1': result['mean_water_f1'],
                'Water_Accuracy': result['mean_water_accuracy'],
                # D8 classification metrics
                'D8_Accuracy': result['mean_d8_accuracy'],
                'D8_Precision_Macro': result['mean_d8_precision_macro'],
                'D8_Recall_Macro': result['mean_d8_recall_macro'],
                'D8_F1_Macro': result['mean_d8_f1_macro'],
                'D8_Precision_Weighted': result['mean_d8_precision_weighted'],
                'D8_Recall_Weighted': result['mean_d8_recall_weighted'],
                'D8_F1_Weighted': result['mean_d8_f1_weighted'],
                # Meta information
                'Checkpoint': os.path.basename(result['checkpoint_path']),
                'Timestamp': result['timestamp']
            })
    
    if not summary_data:
        logger.warning("No results to summarize")
        return
    
    # Create DataFrame and save
    df = pd.DataFrame(summary_data)
    
    # Summary statistics for key metrics
    summary_stats = df.groupby('Variant').agg({
        # Water segmentation metrics
        'Water_IoU': ['mean', 'std', 'min', 'max'],
        'Water_Dice': ['mean', 'std', 'min', 'max'],
        'Water_F1': ['mean', 'std', 'min', 'max'],
        'Water_Precision': ['mean', 'std', 'min', 'max'],
        'Water_Recall': ['mean', 'std', 'min', 'max'],
        'Water_Accuracy': ['mean', 'std', 'min', 'max'],
        # D8 classification metrics
        'D8_Accuracy': ['mean', 'std', 'min', 'max'],
        'D8_F1_Macro': ['mean', 'std', 'min', 'max'],
        'D8_F1_Weighted': ['mean', 'std', 'min', 'max']
    }).round(4)
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    detailed_file = os.path.join(output_dir, f"mdmt_batch_evaluation_detailed_{timestamp}.csv")
    df.to_csv(detailed_file, index=False)
    
    summary_file = os.path.join(output_dir, f"mdmt_batch_evaluation_summary_{timestamp}.csv")
    summary_stats.to_csv(summary_file)
    
    logger.info(f"📊 Summary report saved:")
    logger.info(f"  Detailed: {detailed_file}")
    logger.info(f"  Summary:  {summary_file}")
    
    # Print comprehensive summary to console
    print("\n" + "="*100)
    print("MDMT BATCH EVALUATION COMPREHENSIVE SUMMARY")
    print("="*100)
    print(f"Total Evaluations: {len(summary_data)}")
    print(f"Variants Evaluated: {len(all_results)}")
    
    # Key metrics summary table
    print("\nKEY METRICS SUMMARY (Mean ± Std):")
    print("-" * 100)
    for variant in df['Variant'].unique():
        variant_data = df[df['Variant'] == variant]
        print(f"\n{variant}:")
        print(f"  Water Segmentation:")
        print(f"    IoU:       {variant_data['Water_IoU'].mean():.3f} ± {variant_data['Water_IoU'].std():.3f}")
        print(f"    Dice:      {variant_data['Water_Dice'].mean():.3f} ± {variant_data['Water_Dice'].std():.3f}")
        print(f"    F1-Score:  {variant_data['Water_F1'].mean():.3f} ± {variant_data['Water_F1'].std():.3f}")
        print(f"    Precision: {variant_data['Water_Precision'].mean():.3f} ± {variant_data['Water_Precision'].std():.3f}")
        print(f"    Recall:    {variant_data['Water_Recall'].mean():.3f} ± {variant_data['Water_Recall'].std():.3f}")
        print(f"  D8 Flow Direction:")
        print(f"    Accuracy:  {variant_data['D8_Accuracy'].mean():.3f} ± {variant_data['D8_Accuracy'].std():.3f}")
        print(f"    F1-Macro:  {variant_data['D8_F1_Macro'].mean():.3f} ± {variant_data['D8_F1_Macro'].std():.3f}")
        print(f"    F1-Weighted: {variant_data['D8_F1_Weighted'].mean():.3f} ± {variant_data['D8_F1_Weighted'].std():.3f}")
    
    print("="*100)


def main():
    parser = argparse.ArgumentParser(description="Batch MDMT Evaluator for Multiple Runs")
    parser.add_argument("--variants", nargs='+', 
                       choices=['alphaearth', 'dem_only', 'dem_thermal', 'dem_sar', 'dem_optical', 'dem_alphaearth', 'landsat6b'],
                       default=['alphaearth', 'dem_only', 'dem_thermal', 'dem_sar', 'dem_optical', 'dem_alphaearth', 'landsat6b'],
                       help="Variants to evaluate")
    parser.add_argument("--config-dir", type=str,
                       default="/u/nathanj/national_ml/experiments/evaluation/run_configs",
                       help="Directory containing run configuration files")
    parser.add_argument("--output-dir", type=str,
                       default="/u/nathanj/national_ml/experiments/evaluation/batch_results",
                       help="Output directory for all results")
    parser.add_argument("--limit-batches", type=int,
                       help="Limit number of batches per HUC (for testing)")
    parser.add_argument("--max-runs", type=int, default=10,
                       help="Maximum number of runs to evaluate per variant")
    parser.add_argument("--dry-run", action="store_true",
                       help="Validate configurations without running evaluations")
    parser.add_argument("--use-ultra-fast", action="store_true", default=True,
                       help="Use ultra-fast evaluators when available (default: True)")
    parser.add_argument("--no-ultra-fast", dest="use_ultra_fast", action="store_false",
                       help="Force use of original evaluators")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    logger.info("🚀 Starting MDMT Batch Evaluation")
    logger.info(f"Variants: {args.variants}")
    logger.info(f"Output: {args.output_dir}")
    
    all_results = {}
    
    for variant in args.variants:
        logger.info(f"\n📋 Processing variant: {variant}")
        
        # Load run configuration
        config_file = os.path.join(args.config_dir, f"{variant}_runs.txt")
        runs = load_run_config(config_file)
        
        if not runs:
            logger.warning(f"No runs found for {variant}")
            continue
        
        # Limit runs if specified
        if len(runs) > args.max_runs:
            runs = runs[:args.max_runs]
            logger.info(f"Limited to {args.max_runs} runs")
        
        variant_results = []
        
        for run_name, checkpoint_path in runs:
            # Check if checkpoint exists
            if not os.path.exists(checkpoint_path):
                logger.warning(f"Checkpoint not found: {checkpoint_path}")
                if args.dry_run:
                    logger.info(f"  DRY RUN: Would skip {run_name} (missing checkpoint)")
                continue
            
            if args.dry_run:
                logger.info(f"  DRY RUN: Would evaluate {run_name} with {checkpoint_path}")
                continue
            
            # Run evaluation
            result = run_single_evaluation(
                variant, run_name, checkpoint_path, 
                args.output_dir, args.limit_batches, args.use_ultra_fast
            )
            
            if result:
                variant_results.append(result)
        
        all_results[variant] = variant_results
        logger.info(f"✅ Completed {len(variant_results)} evaluations for {variant}")
    
    if args.dry_run:
        logger.info("🧪 DRY RUN COMPLETE - No actual evaluations were performed")
    else:
        # Create summary report
        create_summary_report(all_results, args.output_dir)
        
        logger.info("🎉 Batch evaluation completed!")


if __name__ == "__main__":
    main()