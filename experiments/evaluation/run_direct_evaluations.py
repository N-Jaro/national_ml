#!/usr/bin/env python3
"""
Direct MDMT Evaluation Runner (No Subprocess)
Runs all variant evaluations directly by importing and calling evaluation functions

This approach:
1. Imports evaluation functions directly
2. Shows real-time output without subprocess overhead
3. Provides better error handling and debugging
4. Avoids environment and timeout issues
"""

import os
import sys
import argparse
import time
from datetime import datetime
from pathlib import Path
import logging

# Add paths for imports
sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/src')
sys.path.append('/u/nathanj/national_ml/experiments/models')
sys.path.append('/u/nathanj/national_ml/experiments/data')
sys.path.append('/u/nathanj/national_ml/experiments/evaluation')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('evaluation_direct_run.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Variant configurations with their evaluation functions
VARIANT_CONFIGS = {
    'alphaearth': {
        'config_file': 'run_configs/alphaearth_runs.txt',
        'evaluator_module': 'ultra_fast_alphaearth_evaluator',
        'evaluator_function': 'main'
    },
    'dem_alphaearth': {
        'config_file': 'run_configs/dem_alphaearth_runs.txt', 
        'evaluator_module': 'ultra_fast_dem_alphaearth_evaluator',
        'evaluator_function': 'main'
    },
    'dem_only': {
        'config_file': 'run_configs/dem_only_runs.txt',
        'evaluator_module': 'ultra_fast_dem_only_evaluator',
        'evaluator_function': 'main'
    },
    'dem_optical': {
        'config_file': 'run_configs/dem_optical_runs.txt',
        'evaluator_module': 'ultra_fast_dem_optical_evaluator',
        'evaluator_function': 'main'
    },
    'dem_sar': {
        'config_file': 'run_configs/dem_sar_runs.txt',
        'evaluator_module': 'ultra_fast_dem_sar_evaluator',
        'evaluator_function': 'main'
    },
    'dem_thermal': {
        'config_file': 'run_configs/dem_thermal_runs.txt',
        'evaluator_module': 'ultra_fast_dem_thermal_evaluator',
        'evaluator_function': 'main'
    },
    'landsat6b': {
        'config_file': 'run_configs/landsat6b_runs.txt',
        'evaluator_module': 'ultra_fast_landsat6b_evaluator',
        'evaluator_function': 'main'
    }
}

def parse_config_file(config_path):
    """Parse run configuration file to extract run names and checkpoint paths."""
    runs = []
    
    if not os.path.exists(config_path):
        logger.warning(f"Config file not found: {config_path}")
        return runs
    
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('|')
                if len(parts) == 2:
                    run_name, checkpoint_path = parts
                    if os.path.exists(checkpoint_path):
                        runs.append((run_name.strip(), checkpoint_path.strip()))
                    else:
                        logger.warning(f"Checkpoint not found: {checkpoint_path}")
                else:
                    logger.warning(f"Invalid config line: {line}")
    
    return runs

def run_evaluation_direct(evaluator_module, checkpoint_path, huc_list_path, run_name, variant):
    """Run a single evaluation directly by importing and calling the function."""
    try:
        logger.info(f"🚀 Starting {variant} evaluation for {run_name}")
        logger.info(f"Checkpoint: {os.path.basename(checkpoint_path)}")
        logger.info(f"HUC list: {huc_list_path}")
        
        # Import the evaluator module
        try:
            evaluator = __import__(evaluator_module)
        except ImportError as e:
            logger.error(f"Failed to import {evaluator_module}: {e}")
            return False, 0, f"Import error: {e}"
            
        # Temporarily modify sys.argv to pass arguments to the evaluator
        original_argv = sys.argv.copy()
        sys.argv = [
            evaluator_module + '.py',
            '--checkpoint', checkpoint_path,
            '--huc-list', huc_list_path
        ]
        
        start_time = time.time()
        
        try:
            # Call the main function of the evaluator
            evaluator.main()
            duration = time.time() - start_time
            logger.info(f"✅ {variant}/{run_name} completed successfully in {duration:.1f}s")
            return True, duration, "Success"
            
        except SystemExit as e:
            # Handle normal exit from argparse
            duration = time.time() - start_time
            if e.code == 0:
                logger.info(f"✅ {variant}/{run_name} completed successfully in {duration:.1f}s")
                return True, duration, "Success"
            else:
                logger.error(f"❌ {variant}/{run_name} exited with code {e.code}")
                return False, duration, f"Exit code: {e.code}"
        
        finally:
            # Restore original sys.argv
            sys.argv = original_argv
            
    except Exception as e:
        duration = time.time() - start_time if 'start_time' in locals() else 0
        logger.error(f"💥 {variant}/{run_name} crashed with exception: {str(e)}")
        return False, duration, str(e)

def run_variant_evaluations(variant, config, huc_list_path, dry_run=False):
    """Run all evaluations for a specific variant."""
    logger.info(f"🚀 Starting {variant.upper()} evaluations")
    
    # Parse configuration
    runs = parse_config_file(config['config_file'])
    if not runs:
        logger.warning(f"No valid runs found for {variant}")
        return []
    
    logger.info(f"Found {len(runs)} runs for {variant}")
    
    results = []
    for i, (run_name, checkpoint_path) in enumerate(runs, 1):
        logger.info(f"📊 Processing {variant} run {i}/{len(runs)}: {run_name}")
        
        if dry_run:
            logger.info(f"[DRY RUN] Would run: {config['evaluator_module']} with {checkpoint_path}")
            results.append((run_name, True, 0, "DRY RUN"))
            continue
        
        success, duration, message = run_evaluation_direct(
            config['evaluator_module'], 
            checkpoint_path, 
            huc_list_path, 
            run_name, 
            variant
        )
        
        results.append((run_name, success, duration, message))
        
        # Brief pause between runs
        time.sleep(2)
    
    return results

def generate_summary_report(all_results, start_time, huc_list_path):
    """Generate a comprehensive summary report."""
    total_time = time.time() - start_time
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    report_path = f"evaluation_direct_summary_{timestamp}.txt"
    
    with open(report_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("MDMT DIRECT EVALUATION SUMMARY REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"HUC List: {huc_list_path}\n")
        f.write(f"Total Runtime: {total_time:.1f} seconds ({total_time/60:.1f} minutes)\n")
        f.write("\n")
        
        # Overall statistics
        total_runs = sum(len(results) for results in all_results.values())
        successful_runs = sum(sum(1 for _, success, _, _ in results if success) 
                            for results in all_results.values())
        failed_runs = total_runs - successful_runs
        
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Variants: {len(all_results)}\n")
        f.write(f"Total Runs: {total_runs}\n")
        f.write(f"Successful: {successful_runs}\n")
        f.write(f"Failed: {failed_runs}\n")
        f.write(f"Success Rate: {successful_runs/total_runs*100:.1f}%\n")
        f.write("\n")
        
        # Per-variant breakdown
        for variant, results in all_results.items():
            f.write(f"VARIANT: {variant.upper()}\n")
            f.write("-" * 40 + "\n")
            
            variant_successful = sum(1 for _, success, _, _ in results if success)
            variant_total = len(results)
            variant_total_time = sum(duration for _, _, duration, _ in results)
            
            f.write(f"Runs: {variant_total}\n")
            f.write(f"Successful: {variant_successful}\n")
            f.write(f"Failed: {variant_total - variant_successful}\n")
            f.write(f"Success Rate: {variant_successful/variant_total*100:.1f}%\n")
            f.write(f"Total Time: {variant_total_time:.1f}s\n")
            f.write(f"Avg Time per Run: {variant_total_time/variant_total:.1f}s\n")
            f.write("\n")
            
            # Individual run details
            f.write("Individual Run Results:\n")
            for run_name, success, duration, message in results:
                status = "✅ SUCCESS" if success else "❌ FAILED"
                f.write(f"  {run_name}: {status} ({duration:.1f}s)\n")
                if not success:
                    f.write(f"    Error: {message[:100]}...\n")
            f.write("\n")
    
    logger.info(f"📊 Summary report saved to: {report_path}")
    return report_path

def main():
    parser = argparse.ArgumentParser(description='Run direct MDMT evaluations for all variants')
    parser.add_argument('--huc-list', default='test_huc_list.txt',
                        help='Path to HUC list file (default: test_huc_list.txt)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be run without actually running')
    parser.add_argument('--variant', choices=list(VARIANT_CONFIGS.keys()),
                        help='Run only specific variant (default: all variants)')
    parser.add_argument('--quick', action='store_true',
                        help='Use small test HUC list for quick testing')
    
    args = parser.parse_args()
    
    # Set HUC list path
    if args.quick:
        huc_list_path = 'test_huc_small.txt'
        logger.info("🏃 Quick mode: Using small test HUC list")
    else:
        huc_list_path = args.huc_list
    
    if not os.path.exists(huc_list_path):
        logger.error(f"HUC list file not found: {huc_list_path}")
        sys.exit(1)
    
    # Count HUCs
    with open(huc_list_path, 'r') as f:
        huc_count = sum(1 for line in f if line.strip() and not line.startswith('#'))
    
    logger.info(f"🎯 Starting direct evaluation with {huc_count} HUCs")
    
    if args.dry_run:
        logger.info("🔍 DRY RUN MODE - No actual evaluations will be performed")
    
    start_time = time.time()
    all_results = {}
    
    # Determine which variants to run
    variants_to_run = [args.variant] if args.variant else list(VARIANT_CONFIGS.keys())
    
    logger.info(f"🚀 Will process {len(variants_to_run)} variants: {', '.join(variants_to_run)}")
    
    # Run evaluations for each variant
    for variant in variants_to_run:
        config = VARIANT_CONFIGS[variant]
        
        try:
            results = run_variant_evaluations(variant, config, huc_list_path, args.dry_run)
            all_results[variant] = results
            
            # Log variant completion
            successful = sum(1 for _, success, _, _ in results if success)
            total = len(results)
            logger.info(f"🏁 {variant.upper()} completed: {successful}/{total} successful")
            
        except Exception as e:
            logger.error(f"💥 Failed to process variant {variant}: {str(e)}")
            all_results[variant] = []
    
    # Generate summary report
    if not args.dry_run:
        report_path = generate_summary_report(all_results, start_time, huc_list_path)
        
        # Print final summary to console
        total_runs = sum(len(results) for results in all_results.values())
        successful_runs = sum(sum(1 for _, success, _, _ in results if success) 
                            for results in all_results.values())
        
        logger.info("🎊 DIRECT EVALUATION BATCH COMPLETE!")
        logger.info(f"📊 Results: {successful_runs}/{total_runs} successful ({successful_runs/total_runs*100:.1f}%)")
        logger.info(f"⏱️  Total time: {(time.time() - start_time)/60:.1f} minutes") 
        logger.info(f"📄 Full report: {report_path}")
    else:
        logger.info("🔍 DRY RUN COMPLETE - No evaluations were actually performed")

if __name__ == "__main__":
    main()