#!/usr/bin/env python3
"""
Terrain Classifier for Transfer Analysis

Classifies patches into terrain categories based on DEM relief analysis.
Uses thresholds determined from quick_terrain_relief_analysis.py:
- Low relief: < 35m (25th percentile)
- High relief: > 114m (75th percentile)
- Mixed: 35-114m (middle 50%)
"""

import numpy as np
from typing import Dict, Tuple, Optional
import torch

class TerrainClassifier:
    """Classifies terrain based on DEM relief statistics"""
    
    def __init__(self, low_threshold: float = 35.0, high_threshold: float = 114.0):
        """
        Initialize terrain classifier
        
        Args:
            low_threshold: Relief threshold for low terrain (default 35m)
            high_threshold: Relief threshold for high terrain (default 114m)
        """
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        
    def calculate_relief(self, dem_patch: np.ndarray) -> float:
        """
        Calculate terrain relief (max - min elevation) for a DEM patch
        
        Args:
            dem_patch: DEM data array, shape (224, 224) or (1, 224, 224)
            
        Returns:
            Relief value in meters
        """
        if dem_patch.ndim == 3:
            dem_patch = dem_patch.squeeze()
            
        # Handle invalid/masked values
        valid_mask = ~np.isnan(dem_patch) & ~np.isinf(dem_patch)
        if not np.any(valid_mask):
            return 0.0
            
        valid_dem = dem_patch[valid_mask]
        relief = float(np.max(valid_dem) - np.min(valid_dem))
        return relief
        
    def classify_terrain(self, dem_patch: np.ndarray) -> str:
        """
        Classify terrain type based on relief
        
        Args:
            dem_patch: DEM data array
            
        Returns:
            Terrain class: 'low', 'mixed', or 'high'
        """
        relief = self.calculate_relief(dem_patch)
        
        if relief < self.low_threshold:
            return 'low'
        elif relief > self.high_threshold:
            return 'high'
        else:
            return 'mixed'
            
    def get_terrain_stats(self, dem_patch: np.ndarray) -> Dict:
        """
        Get detailed terrain statistics for a patch
        
        Args:
            dem_patch: DEM data array
            
        Returns:
            Dictionary with terrain statistics
        """
        if dem_patch.ndim == 3:
            dem_patch = dem_patch.squeeze()
            
        valid_mask = ~np.isnan(dem_patch) & ~np.isinf(dem_patch)
        if not np.any(valid_mask):
            return {
                'relief': 0.0,
                'terrain_class': 'low',
                'min_elevation': 0.0,
                'max_elevation': 0.0,
                'mean_elevation': 0.0,
                'std_elevation': 0.0,
                'valid_pixels': 0
            }
            
        valid_dem = dem_patch[valid_mask]
        relief = float(np.max(valid_dem) - np.min(valid_dem))
        
        return {
            'relief': relief,
            'terrain_class': self.classify_terrain(dem_patch),
            'min_elevation': float(np.min(valid_dem)),
            'max_elevation': float(np.max(valid_dem)),
            'mean_elevation': float(np.mean(valid_dem)),
            'std_elevation': float(np.std(valid_dem)),
            'valid_pixels': int(np.sum(valid_mask))
        }
        
    def filter_patches_by_terrain(self, patch_files: list, terrain_type: str, 
                                  max_patches: Optional[int] = None) -> list:
        """
        Filter patch files by terrain type
        
        Args:
            patch_files: List of patch file paths
            terrain_type: Target terrain type ('low', 'mixed', 'high')
            max_patches: Maximum number of patches to return
            
        Returns:
            List of patch files matching terrain type
        """
        matching_patches = []
        
        for patch_file in patch_files:
            try:
                # Load patch to check terrain
                patch_data = np.load(patch_file)
                if 'dem' in patch_data:
                    dem = patch_data['dem']
                    if self.classify_terrain(dem) == terrain_type:
                        matching_patches.append(patch_file)
                        if max_patches and len(matching_patches) >= max_patches:
                            break
            except Exception as e:
                print(f"Warning: Could not process {patch_file}: {e}")
                continue
                
        return matching_patches