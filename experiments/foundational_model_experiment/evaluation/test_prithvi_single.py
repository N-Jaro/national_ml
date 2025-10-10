#!/usr/bin/env python3
"""
Test Prithvi evaluation with a single run and small batch for quick verification.
"""

import sys
import os
import subprocess
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_prithvi_single_run():
    """Test Prithvi evaluation with run1 and small batch using terratorch_env."""
    
    # Get checkpoint from config
    config_file = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config/prithvi_runs.txt"
    
    if not os.path.exists(config_file):
        logger.error(f"Config file not found: {config_file}")
        return False
    
    # Read run1 checkpoint
    checkpoint_path = None
    with open(config_file, 'r') as f:
        for line in f:
            if line.startswith('run1|'):
                checkpoint_path = line.split('|', 1)[1].strip()
                break
    
    if not checkpoint_path:
        logger.error("run1 not found in config file")
        return False
    
    if not os.path.exists(checkpoint_path):
        logger.error(f"Checkpoint not found: {checkpoint_path}")
        return False
    
    logger.info(f"Using checkpoint: {checkpoint_path}")
    
    # Run evaluation with small batch using terratorch_env
    eval_script = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_prithvi_evaluator.py"
    
    # Use bash command to activate terratorch_env and run the evaluation
    cmd = [
        "bash", "-c",
        f"source /u/nathanj/miniconda3/etc/profile.d/conda.sh && "
        f"conda activate terratorch_env && "
        f"python {eval_script} "
        f"--checkpoint '{checkpoint_path}' "
        f"--data_path '/u/nathanj/national_ml/data/processed/patch_dataset' "
        f"--output_dir '/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_results' "
        f"--batch_size 8 "
        f"--device cpu "
        f"--huc_file '/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt' "
        f"--run_name 'test_run1' "
        f"--wandb_mode disabled"
    ]
    
    logger.info("Running Prithvi evaluation test with terratorch_env...")
    logger.info(f"Command: {' '.join(cmd)}")
    
    try:
        # Create output directory
        os.makedirs("/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_results", exist_ok=True)
        
        # Run the evaluation
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
        
        if result.returncode == 0:
            logger.info("✓ Prithvi evaluation test completed successfully!")
            logger.info("STDOUT:")
            print(result.stdout[-2000:])  # Last 2000 chars
            return True
        else:
            logger.error("✗ Prithvi evaluation test failed!")
            logger.error("STDERR:")
            print(result.stderr[-2000:])  # Last 2000 chars
            logger.error("STDOUT:")
            print(result.stdout[-2000:])
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("✗ Evaluation test timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"✗ Error running evaluation test: {e}")
        return False

def main():
    """Run the test."""
    
    logger.info("Testing Prithvi Foundation Model Evaluation")
    logger.info("=" * 50)
    
    # Check if we're in the right directory
    eval_dir = Path("/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation")
    if not eval_dir.exists():
        logger.error(f"Evaluation directory not found: {eval_dir}")
        return 1
    
    # Change to evaluation directory
    os.chdir(eval_dir)
    logger.info(f"Changed to directory: {eval_dir}")
    
    # Run the test
    success = test_prithvi_single_run()
    
    logger.info("\n" + "=" * 50)
    if success:
        logger.info("🎉 Prithvi evaluation test PASSED!")
        logger.info("You can now run the full evaluation with:")
        logger.info("  sbatch submit_prithvi_eval.sh")
        logger.info("  sbatch submit_foundation_batch_eval.sh")
        return 0
    else:
        logger.error("❌ Prithvi evaluation test FAILED!")
        logger.error("Please check the errors above and fix before running full evaluation.")
        return 1

if __name__ == "__main__":
    exit(main())