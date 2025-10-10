#!/usr/bin/env python3
"""
Batch Foundation Model Evaluator
Runs evaluation for both Prithvi and Clay foundation models, following the same pattern 
as MDMT batch evaluation for direct comparison.
"""

import os
import sys
import argparse
import subprocess
import logging
from pathlib import Path
from datetime import datetime
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_run_config(model_type: str, config_dir: str) -> list:
    """Load run configuration from config files."""
    
    config_file = os.path.join(config_dir, f"{model_type}_runs.txt")
    
    if not os.path.exists(config_file):
        logger.error(f"Config file not found: {config_file}")
        return []
    
    runs = []
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if line.startswith('#') or not line:
                continue
            
            # Parse format: run_name|checkpoint_path
            if '|' in line:
                run_name, checkpoint_path = line.split('|', 1)
                runs.append({
                    'run_name': run_name.strip(),
                    'checkpoint_path': checkpoint_path.strip(),
                    'model_type': model_type
                })
    
    logger.info(f"Loaded {len(runs)} {model_type} runs from config")
    return runs

def verify_checkpoint_exists(run_config: dict) -> bool:
    """Verify that the checkpoint file exists."""
    
    checkpoint_path = run_config['checkpoint_path']
    if os.path.exists(checkpoint_path):
        logger.info(f"✓ Checkpoint verified: {run_config['run_name']}")
        return True
    else:
        logger.error(f"✗ Checkpoint not found: {checkpoint_path}")
        return False

def run_evaluation(run_config: dict, args: argparse.Namespace) -> bool:
    """Run evaluation for a single checkpoint."""
    
    model_type = run_config['model_type']
    checkpoint_path = run_config['checkpoint_path']
    run_name = run_config['run_name']
    
    # Determine evaluator script
    script_dir = Path(__file__).parent
    if model_type == "prithvi":
        evaluator_script = script_dir / "ultra_fast_prithvi_evaluator.py"
    elif model_type == "clay":
        evaluator_script = script_dir / "ultra_fast_clay_evaluator.py"
    else:
        logger.error(f"Unknown model type: {model_type}")
        return False
    
    if not evaluator_script.exists():
        logger.error(f"Evaluator script not found: {evaluator_script}")
        return False
    
    logger.info(f"Running {model_type} evaluation for checkpoint: {checkpoint_path}")
    logger.info(f"Run name: {run_name}")
    
    # Build command
    cmd = [
        sys.executable, str(evaluator_script),
        "--checkpoint", checkpoint_path,
        "--data_path", args.data_path,
        "--output_dir", args.output_dir,
        "--batch_size", str(args.batch_size),
        "--huc_file", args.huc_file,
        "--run_name", run_name,
        "--wandb_mode", args.wandb_mode
    ]
    
    if args.device:
        cmd.extend(["--device", args.device])
    
    # Run evaluation
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
        
        if result.returncode == 0:
            logger.info(f"Successfully completed {model_type} evaluation for {run_name}")
            return True
        else:
            logger.error(f"Evaluation failed for {model_type} {run_name}")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        timeout_msg = f" after {args.timeout} seconds" if args.timeout else ""
        logger.error(f"Evaluation timed out for {model_type} {run_name}{timeout_msg}")
        return False
    except Exception as e:
        logger.error(f"Error running evaluation for {model_type} {run_name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Batch Foundation Model Evaluator')
    
    # Model selection
    parser.add_argument('--models', type=str, nargs='+', 
                       choices=['prithvi', 'clay', 'all'], default=['all'],
                       help='Models to evaluate (default: all)')
    parser.add_argument('--config_dir', type=str, 
                       default='/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config',
                       help='Directory containing run configuration files')
    
    # Evaluation parameters
    parser.add_argument('--data_path', type=str, 
                       default='/projects/bcrm/nathanj/data/processed/test/patch_dataset',
                       help='Path to patch dataset')
    parser.add_argument('--output_dir', type=str, 
                       default='/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results',
                       help='Output directory for results')
    parser.add_argument('--batch_size', type=int, default=32, help='Batch size for evaluation')
    parser.add_argument('--device', type=str, default=None, help='Device to use (cuda/cpu, auto-detect if None)')
    parser.add_argument('--huc_file', type=str, 
                       default='/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt',
                       help='File containing list of test HUC codes')
    parser.add_argument('--wandb_mode', type=str, default='online', 
                       choices=['online', 'offline', 'disabled'],
                       help='Wandb logging mode')
    
    # Execution parameters
    parser.add_argument('--timeout', type=int, default=None, 
                       help='Timeout for each evaluation in seconds (default: None - no timeout)')
    parser.add_argument('--max_checkpoints', type=int, default=None,
                       help='Maximum number of checkpoints to evaluate per model')
    parser.add_argument('--dry_run', action='store_true',
                       help='Print what would be done without actually running')
    
    args = parser.parse_args()
    
    # Determine which models to evaluate
    if 'all' in args.models:
        models_to_evaluate = ['prithvi', 'clay']
    else:
        models_to_evaluate = args.models
    
    logger.info(f"Starting batch evaluation for models: {models_to_evaluate}")
    logger.info(f"Config directory: {args.config_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Keep track of all evaluations
    evaluation_summary = {
        'start_time': datetime.now().isoformat(),
        'models': {},
        'total_checkpoints': 0,
        'successful_evaluations': 0,
        'failed_evaluations': 0
    }
    
    # Process each model type
    for model_type in models_to_evaluate:
        logger.info(f"\n=== Processing {model_type.upper()} Model ===")
        
        # Load run configurations
        run_configs = load_run_config(model_type, args.config_dir)
        
        if not run_configs:
            logger.warning(f"No run configurations found for {model_type}")
            evaluation_summary['models'][model_type] = {
                'runs_found': 0,
                'runs_evaluated': 0,
                'successful': 0,
                'failed': 0,
                'runs': []
            }
            continue
        
        logger.info(f"Found {len(run_configs)} {model_type} runs in config")
        
        # Verify checkpoints exist
        valid_runs = []
        for run_config in run_configs:
            if verify_checkpoint_exists(run_config):
                valid_runs.append(run_config)
            else:
                logger.warning(f"Skipping {run_config['run_name']} - checkpoint not found")
        
        logger.info(f"Verified {len(valid_runs)} valid checkpoints")
        
        # Limit runs if requested
        if args.max_checkpoints:
            valid_runs = valid_runs[:args.max_checkpoints]
            logger.info(f"Limited to {len(valid_runs)} runs per max_checkpoints setting")
        
        # Initialize model summary
        evaluation_summary['models'][model_type] = {
            'runs_found': len(run_configs),
            'runs_evaluated': 0,
            'successful': 0,
            'failed': 0,
            'runs': []
        }
        
        # Process each run
        for i, run_config in enumerate(valid_runs):
            logger.info(f"\nProcessing run {i+1}/{len(valid_runs)}: {run_config['run_name']}")
            
            if args.dry_run:
                logger.info(f"DRY RUN: Would evaluate {run_config['checkpoint_path']}")
                run_summary = {
                    'checkpoint_path': run_config['checkpoint_path'],
                    'run_name': run_config['run_name'],
                    'status': 'dry_run'
                }
            else:
                # Run actual evaluation
                success = run_evaluation(run_config, args)
                
                run_summary = {
                    'checkpoint_path': run_config['checkpoint_path'],
                    'run_name': run_config['run_name'],
                    'status': 'success' if success else 'failed'
                }
                
                # Update counters
                evaluation_summary['models'][model_type]['runs_evaluated'] += 1
                evaluation_summary['total_checkpoints'] += 1
                
                if success:
                    evaluation_summary['models'][model_type]['successful'] += 1
                    evaluation_summary['successful_evaluations'] += 1
                else:
                    evaluation_summary['models'][model_type]['failed'] += 1
                    evaluation_summary['failed_evaluations'] += 1
            
            evaluation_summary['models'][model_type]['runs'].append(run_summary)
    
    # Finalize summary
    evaluation_summary['end_time'] = datetime.now().isoformat()
    
    # Save evaluation summary
    summary_path = os.path.join(args.output_dir, f"foundation_batch_evaluation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(summary_path, 'w') as f:
        json.dump(evaluation_summary, f, indent=2)
    
    # Print final summary
    logger.info("\n" + "="*60)
    logger.info("FOUNDATION MODEL BATCH EVALUATION SUMMARY")
    logger.info("="*60)
    
    for model_type, model_summary in evaluation_summary['models'].items():
        logger.info(f"\n{model_type.upper()} Model:")
        logger.info(f"  Runs found: {model_summary['runs_found']}")
        logger.info(f"  Runs evaluated: {model_summary['runs_evaluated']}")
        logger.info(f"  Successful: {model_summary['successful']}")
        logger.info(f"  Failed: {model_summary['failed']}")
    
    logger.info(f"\nOverall:")
    logger.info(f"  Total checkpoints: {evaluation_summary['total_checkpoints']}")
    logger.info(f"  Successful evaluations: {evaluation_summary['successful_evaluations']}")
    logger.info(f"  Failed evaluations: {evaluation_summary['failed_evaluations']}")
    logger.info(f"\nSummary saved to: {summary_path}")
    logger.info(f"Results saved to: {args.output_dir}")

if __name__ == "__main__":
    main()