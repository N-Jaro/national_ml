#!/usr/bin/env python3
"""
Optical Masking for Robustness Analysis
Simulate cloud coverage by masking optical bands.
"""

import numpy as np
import torch
from typing import Union, Tuple, Dict
from config import config

class OpticalMasker:
    """Simulate cloud coverage by masking optical bands."""
    
    def __init__(self, cloud_coverage: float = None, fill_value: float = None):
        """
        Initialize optical masker.
        
        Args:
            cloud_coverage: Fraction of pixels to mask (0.0-1.0)
            fill_value: Value to use for masked pixels
        """
        self.cloud_coverage = cloud_coverage or config.cloud_coverage
        self.fill_value = fill_value or config.cloud_fill_value
        
        print(f"☁️  Optical Masker initialized:")
        print(f"   Cloud coverage: {self.cloud_coverage*100}%")
        print(f"   Fill value: {self.fill_value}")
    
    def generate_cloud_mask(self, shape: Tuple[int, ...], seed: int = None) -> np.ndarray:
        """
        Generate random cloud mask with specified coverage.
        
        Args:
            shape: Shape of the mask (H, W) or (B, C, H, W)
            seed: Random seed for reproducibility
            
        Returns:
            mask: Binary mask where 1 = cloud, 0 = clear
        """
        if seed is not None:
            np.random.seed(seed)
        
        # Generate random mask
        mask = np.random.random(shape) < self.cloud_coverage
        return mask.astype(np.float32)
    
    def apply_cloud_mask(self, optical_data: Union[np.ndarray, torch.Tensor], 
                        mask: Union[np.ndarray, torch.Tensor] = None,
                        seed: int = None) -> Union[np.ndarray, torch.Tensor]:
        """
        Apply cloud mask to optical data.
        
        Args:
            optical_data: Optical bands, shape (C, H, W) or (B, C, H, W)
            mask: Pre-computed mask, or None to generate new mask
            seed: Random seed if generating new mask
            
        Returns:
            masked_optical: Optical data with clouds masked
        """
        is_torch = isinstance(optical_data, torch.Tensor)
        device = optical_data.device if is_torch else None
        
        # Convert to numpy for processing
        if is_torch:
            optical_np = optical_data.cpu().numpy()
        else:
            optical_np = optical_data.copy()
        
        # Generate mask if not provided
        if mask is None:
            if optical_np.ndim == 3:  # (C, H, W)
                mask_shape = optical_np.shape[-2:]  # (H, W)
            elif optical_np.ndim == 4:  # (B, C, H, W)
                mask_shape = optical_np.shape[-2:]  # (H, W)
            else:
                raise ValueError(f"Unexpected optical data shape: {optical_np.shape}")
            
            mask = self.generate_cloud_mask(mask_shape, seed=seed)
        
        # Convert mask to numpy if needed
        if isinstance(mask, torch.Tensor):
            mask = mask.cpu().numpy()
        
        # Apply mask
        masked_optical = optical_np.copy()
        
        if optical_np.ndim == 3:  # (C, H, W)
            # Broadcast mask to all channels
            mask_expanded = mask[np.newaxis, :, :]  # (1, H, W)
            masked_optical = masked_optical * (1 - mask_expanded) + self.fill_value * mask_expanded
            
        elif optical_np.ndim == 4:  # (B, C, H, W)
            # Apply same mask to all batches and channels
            mask_expanded = mask[np.newaxis, np.newaxis, :, :]  # (1, 1, H, W)
            masked_optical = masked_optical * (1 - mask_expanded) + self.fill_value * mask_expanded
        
        # Convert back to torch if needed
        if is_torch:
            masked_optical = torch.from_numpy(masked_optical).to(device)
            if isinstance(mask, np.ndarray):
                mask = torch.from_numpy(mask).to(device)
        
        return masked_optical
    
    def mask_batch_data(self, batch_data: Dict, seed: int = None) -> Tuple[Dict, np.ndarray]:
        """
        Apply cloud masking to a batch of data.
        
        Args:
            batch_data: Dictionary containing batch data with 'optical' key
            seed: Random seed for reproducibility
            
        Returns:
            masked_batch: Batch data with optical bands masked
            cloud_mask: Applied cloud mask
        """
        if 'optical' not in batch_data:
            raise ValueError("Batch data must contain 'optical' key")
        
        masked_batch = batch_data.copy()
        
        # Apply cloud mask to optical data
        masked_optical = self.apply_cloud_mask(batch_data['optical'], seed=seed)
        
        # Generate mask for reporting (same shape as used)
        optical_shape = batch_data['optical'].shape
        if optical_shape[-2:] == (224, 224):  # Assume (C, 224, 224) or (B, C, 224, 224)
            mask_shape = (224, 224)
        else:
            mask_shape = optical_shape[-2:]
        
        if seed is not None:
            np.random.seed(seed)
        cloud_mask = self.generate_cloud_mask(mask_shape, seed=seed)
        
        masked_batch['optical'] = masked_optical
        
        return masked_batch, cloud_mask
    
    def calculate_masking_statistics(self, original_data: Union[np.ndarray, torch.Tensor],
                                   masked_data: Union[np.ndarray, torch.Tensor]) -> Dict:
        """
        Calculate statistics comparing original and masked data.
        
        Args:
            original_data: Original optical data
            masked_data: Cloud-masked optical data
            
        Returns:
            stats: Dictionary with masking statistics
        """
        if isinstance(original_data, torch.Tensor):
            original_data = original_data.cpu().numpy()
        if isinstance(masked_data, torch.Tensor):
            masked_data = masked_data.cpu().numpy()
        
        # Calculate actual masking percentage
        mask = (masked_data == self.fill_value).astype(float)
        actual_coverage = np.mean(mask)
        
        # Calculate data changes
        data_change = np.abs(original_data - masked_data)
        mean_change = np.mean(data_change)
        
        stats = {
            "expected_coverage": self.cloud_coverage,
            "actual_coverage": float(actual_coverage),
            "mean_data_change": float(mean_change),
            "fill_value": self.fill_value,
            "original_mean": float(np.mean(original_data)),
            "masked_mean": float(np.mean(masked_data))
        }
        
        return stats
    
    def print_statistics(self, stats: Dict):
        """Print cloud masking statistics."""
        print(f"\n☁️  CLOUD MASKING STATISTICS")
        print("=" * 40)
        print(f"Expected coverage: {stats['expected_coverage']*100:.1f}%")
        print(f"Actual coverage: {stats['actual_coverage']*100:.1f}%")
        print(f"Fill value: {stats['fill_value']}")
        print(f"Original data mean: {stats['original_mean']:.3f}")
        print(f"Masked data mean: {stats['masked_mean']:.3f}")
        print(f"Mean data change: {stats['mean_data_change']:.3f}")

def test_optical_masker():
    """Test optical masker with sample data."""
    print("🧪 TESTING OPTICAL MASKER")
    print("=" * 40)
    
    masker = OpticalMasker()
    
    # Create test optical data (simulating 6-band Landsat)
    optical_data = np.random.randn(6, 224, 224) * 0.1 + 0.5  # 6 bands, normalized
    
    print(f"Original optical shape: {optical_data.shape}")
    print(f"Original data range: {optical_data.min():.3f} - {optical_data.max():.3f}")
    
    # Test cloud masking
    masked_optical = masker.apply_cloud_mask(optical_data, seed=42)
    
    print(f"Masked optical shape: {masked_optical.shape}")
    print(f"Masked data range: {masked_optical.min():.3f} - {masked_optical.max():.3f}")
    
    # Calculate statistics
    stats = masker.calculate_masking_statistics(optical_data, masked_optical)
    masker.print_statistics(stats)
    
    # Test batch processing
    batch_data = {'optical': optical_data, 'dem': np.random.randn(1, 224, 224)}
    masked_batch, cloud_mask = masker.mask_batch_data(batch_data, seed=42)
    
    print(f"\nBatch masking:")
    print(f"Cloud mask shape: {cloud_mask.shape}")
    print(f"Cloud mask coverage: {np.mean(cloud_mask)*100:.1f}%")
    print(f"Batch keys: {list(masked_batch.keys())}")
    
    print("\n✅ Optical masker test completed!")

if __name__ == "__main__":
    test_optical_masker()