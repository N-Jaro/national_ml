#!/usr/bin/env python3
"""
Configuration for Transfer and Robustness Analysis

Based on DEM sanity analysis findings with 10 representative HUCs
and terrain relief thresholds from quick_terrain_relief_analysis.py
"""

from dataclasses import dataclass
from typing import List, Dict

@dataclass
class TransferRobustnessConfig:
    """Configuration for transfer and robustness analysis"""
    
    # Representative HUCs from DEM sanity analysis (geographic and performance diversity)
    representative_hucs: List[str] = None
    
    # Model configuration
    model_checkpoint: str = "/u/nathanj/national_ml/outputs/models/all_modalities_alphaearth/all_modalities_alphaearth_task2_seed123_epoch=02_val_loss=0.6317.ckpt"
    
    # Data paths
    test_data_path: str = "/u/nathanj/national_ml/data/processed/patch_dataset"
    output_dir: str = "/u/nathanj/national_ml/experiments/transfer_robustness/results"
    
    # Terrain classification thresholds (from quick terrain relief analysis)
    # Based on 25th (35m) and 75th (114m) percentiles of relief distribution
    low_relief_threshold: float = 35.0    # < 35m relief = low terrain
    high_relief_threshold: float = 114.0  # > 114m relief = high terrain
    
    # Optical masking configuration (cloud simulation)
    optical_mask_probability: float = 0.3  # 30% of optical bands masked
    
    # Analysis parameters
    max_patches_per_huc: int = 500  # Limit for quick analysis
    device: str = "cpu"  # Default to CPU, will be updated in __post_init__
    
    def __post_init__(self):
        if self.representative_hucs is None:
            # Available HUCs from local processed data for analysis
            self.representative_hucs = [
                "03030005",  # Tennessee - Mixed terrain
                "04060102",  # Great Lakes region
                "07040006",  # Central plains
                "08020301",  # Arkansas-White-Red - Low relief
                "10270104",  # Missouri region
                "12090302",  # Texas region
                "14060004",  # Colorado - Mountain terrain
                "17100206",  # Pacific Northwest
                "18070103",  # California - Valley
                "19050401"   # California - Diverse terrain
            ]

# Default configuration instance
DEFAULT_CONFIG = TransferRobustnessConfig()

# Import torch here to avoid issues if not available
try:
    import torch
    DEFAULT_CONFIG.device = "cuda" if torch.cuda.is_available() else "cpu"
except ImportError:
    DEFAULT_CONFIG.device = "cpu"