#!/usr/bin/env python3
"""
Setup script for Foundation Model Evaluation System
Creates necessary directories and verifies system requirements.
"""

import os
import sys
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_directories():
    """Create necessary directories for evaluation system."""
    
    base_dir = Path(__file__).parent
    
    directories = [
        base_dir / "ultra_fast_results",
        base_dir / "comparison_results", 
        base_dir / "slurm_logs",
        base_dir / "temp"
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def check_dependencies():
    """Check that required dependencies are available."""
    
    required_packages = [
        'torch',
        'numpy', 
        'pandas',
        'sklearn',
        'matplotlib',
        'seaborn',
        'scipy',
        'tqdm',
        'wandb'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package} is available")
        except ImportError:
            missing_packages.append(package)
            logger.error(f"✗ {package} is missing")
    
    if missing_packages:
        logger.error(f"Missing packages: {missing_packages}")
        logger.error("Please install missing packages before running evaluations")
        return False
    
    return True

def verify_data_paths():
    """Verify that expected data paths exist."""
    
    paths_to_check = [
        "/projects/bcrm/nathanj/data/processed/test/patch_dataset",
        "/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt",
        "/u/nathanj/national_ml/experiments/foundational_model_experiment/prithvi",
        "/u/nathanj/national_ml/experiments/foundational_model_experiment/clay"
    ]
    
    all_exist = True
    
    for path in paths_to_check:
        if os.path.exists(path):
            logger.info(f"✓ Path exists: {path}")
        else:
            logger.warning(f"✗ Path not found: {path}")
            all_exist = False
    
    return all_exist

def main():
    """Run setup process."""
    
    logger.info("Setting up Foundation Model Evaluation System")
    logger.info("=" * 50)
    
    # Create directories
    logger.info("\n1. Creating directories...")
    create_directories()
    
    # Check dependencies
    logger.info("\n2. Checking dependencies...")
    deps_ok = check_dependencies()
    
    # Verify data paths
    logger.info("\n3. Verifying data paths...")
    paths_ok = verify_data_paths()
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("Setup Summary:")
    logger.info(f"  Directories: Created")
    logger.info(f"  Dependencies: {'OK' if deps_ok else 'MISSING'}")
    logger.info(f"  Data paths: {'OK' if paths_ok else 'SOME MISSING'}")
    
    if deps_ok and paths_ok:
        logger.info("\n🎉 Setup completed successfully!")
        logger.info("Foundation model evaluation system is ready to use.")
        
        logger.info("\nNext steps:")
        logger.info("1. Ensure foundation models are trained")
        logger.info("2. Run: python test_evaluation_system.py")
        logger.info("3. Submit evaluations: sbatch submit_foundation_batch_eval.sh")
        
        return 0
    else:
        logger.error("\n❌ Setup incomplete. Please resolve issues above.")
        return 1

if __name__ == "__main__":
    exit(main())