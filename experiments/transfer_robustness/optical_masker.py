#!/usr/bin/env python3
"""
Optical Masker for Robustness Analysis  

Simulates cloud cover by randomly masking optical bands to test model robustness
to missing optical data. Uses 30% masking probability by default.
"""

import numpy as np
import torch
from typing import Dict, Tuple, Optional

class OpticalMasker:
    """Masks optical bands to simulate cloud cover for robustness testing"""
    
    def __init__(self, mask_probability: float = 0.3, mask_value: float = 0.0):
        """
        Initialize optical masker
        
        Args:
            mask_probability: Probability of masking each optical band (default 30%)
            mask_value: Value to use for masked pixels (default 0.0)
        """
        self.mask_probability = mask_probability
        self.mask_value = mask_value
        
    def create_optical_mask(self, optical_shape: Tuple, seed: Optional[int] = None) -> np.ndarray:
        """
        Create random mask for optical bands
        
        Args:
            optical_shape: Shape of optical data (channels, height, width)
            seed: Random seed for reproducibility
            
        Returns:
            Boolean mask array where True = masked pixel
        """
        if seed is not None:
            np.random.seed(seed)
            
        # Create mask with same spatial dimensions as optical data
        if len(optical_shape) == 3:
            channels, height, width = optical_shape
            mask = np.random.random((height, width)) < self.mask_probability
            # Broadcast to all channels
            mask = np.broadcast_to(mask[None, :, :], (channels, height, width))
        else:
            mask = np.random.random(optical_shape) < self.mask_probability
            
        return mask
        
    def apply_optical_mask(self, optical_data: np.ndarray, mask: Optional[np.ndarray] = None,
                          seed: Optional[int] = None) -> Tuple[np.ndarray, np.ndarray]:
        """
        Apply masking to optical data
        
        Args:
            optical_data: Optical data array, shape (channels, height, width)
            mask: Pre-computed mask (if None, creates new mask)
            seed: Random seed for mask generation
            
        Returns:
            Tuple of (masked_optical_data, mask_used)
        """
        if mask is None:
            mask = self.create_optical_mask(optical_data.shape, seed=seed)
            
        masked_optical = optical_data.copy()
        masked_optical[mask] = self.mask_value
        
        return masked_optical, mask
        
    def apply_patch_masking(self, patch_data: Dict, seed: Optional[int] = None) -> Tuple[Dict, Dict]:
        """
        Apply optical masking to a complete patch
        
        Args:
            patch_data: Dictionary containing patch data (must have 'optical' key)
            seed: Random seed for reproducibility
            
        Returns:
            Tuple of (masked_patch_data, masking_info)
        """
        if 'optical' not in patch_data:
            return patch_data, {'masked': False, 'mask_coverage': 0.0}
            
        optical_data = patch_data['optical']
        
        # Apply masking
        masked_optical, mask = self.apply_optical_mask(optical_data, seed=seed)
        
        # Create masked patch data
        masked_patch = patch_data.copy()
        masked_patch['optical'] = masked_optical
        
        # Calculate masking statistics
        mask_coverage = float(np.mean(mask))
        
        masking_info = {
            'masked': True,
            'mask_coverage': mask_coverage,
            'mask_probability': self.mask_probability,
            'mask_shape': mask.shape,
            'total_masked_pixels': int(np.sum(mask))
        }
        
        return masked_patch, masking_info
        
    def batch_mask_patches(self, patch_list: list, seed_base: int = 42) -> list:
        """
        Apply masking to a batch of patches with different seeds
        
        Args:
            patch_list: List of patch file paths or loaded patch data
            seed_base: Base seed for reproducible masking
            
        Returns:
            List of (masked_patch_data, masking_info) tuples
        """
        masked_patches = []
        
        for i, patch in enumerate(patch_list):
            # Load patch if needed
            if isinstance(patch, str):
                patch_data = np.load(patch)
                patch_data = {key: patch_data[key] for key in patch_data.keys()}
            else:
                patch_data = patch
                
            # Apply masking with unique seed
            masked_patch, mask_info = self.apply_patch_masking(
                patch_data, seed=seed_base + i
            )
            
            masked_patches.append((masked_patch, mask_info))
            
        return masked_patches
        
    def get_masking_statistics(self, mask: np.ndarray) -> Dict:
        """
        Calculate detailed statistics for a mask
        
        Args:
            mask: Boolean mask array
            
        Returns:
            Dictionary with masking statistics
        """
        return {
            'mask_coverage': float(np.mean(mask)),
            'total_pixels': int(mask.size),
            'masked_pixels': int(np.sum(mask)),
            'unmasked_pixels': int(np.sum(~mask)),
            'mask_shape': mask.shape
        }