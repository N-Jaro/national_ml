#!/usr/bin/env python3
"""
Test the fixed safe_to_uint8_rgb function
"""

import numpy as np

def safe_to_uint8_rgb(img: np.ndarray) -> np.ndarray:
    """Convert normalized image to uint8 RGB format for visualization."""
    if img.ndim == 2:  # Grayscale (H, W)
        img = np.stack([img, img, img], axis=-1)  # Convert to (H, W, 3)
    elif img.ndim == 3 and img.shape[-1] == 1:  # (H, W, 1) 
        img = np.repeat(img, 3, axis=-1)  # Convert to (H, W, 3)
    elif img.ndim == 3 and img.shape[-1] == 3:  # Already RGB (H, W, 3)
        pass  # Keep as is
    else:
        # Fallback: convert to grayscale and then RGB
        if img.ndim == 3:
            img = img.mean(axis=-1)  # Average channels to get grayscale
        img = np.stack([img, img, img], axis=-1)  # Convert to (H, W, 3)
    
    # Ensure values are in [0, 1] then convert to uint8
    img = np.clip(img, 0, 1)
    return (img * 255).astype(np.uint8)

def test_safe_to_uint8_rgb():
    print("🧪 Testing safe_to_uint8_rgb function...")
    
    # Test case 1: 2D grayscale (H, W)
    img_2d = np.random.rand(224, 224).astype(np.float32)
    result_2d = safe_to_uint8_rgb(img_2d)
    print(f"✅ 2D input {img_2d.shape} -> {result_2d.shape}, dtype: {result_2d.dtype}")
    
    # Test case 2: 3D grayscale (H, W, 1)
    img_3d_1 = np.random.rand(224, 224, 1).astype(np.float32)
    result_3d_1 = safe_to_uint8_rgb(img_3d_1)
    print(f"✅ 3D-1 input {img_3d_1.shape} -> {result_3d_1.shape}, dtype: {result_3d_1.dtype}")
    
    # Test case 3: 3D RGB (H, W, 3) - the problematic case
    img_3d_3 = np.random.rand(224, 224, 3).astype(np.float32)
    result_3d_3 = safe_to_uint8_rgb(img_3d_3)
    print(f"✅ 3D-3 input {img_3d_3.shape} -> {result_3d_3.shape}, dtype: {result_3d_3.dtype}")
    
    # Test case 4: Edge case - 3D with weird channels
    img_3d_weird = np.random.rand(224, 224, 5).astype(np.float32)
    result_3d_weird = safe_to_uint8_rgb(img_3d_weird)
    print(f"✅ 3D-weird input {img_3d_weird.shape} -> {result_3d_weird.shape}, dtype: {result_3d_weird.dtype}")
    
    # Verify all outputs are (H, W, 3) uint8
    all_results = [result_2d, result_3d_1, result_3d_3, result_3d_weird]
    for i, result in enumerate(all_results, 1):
        assert result.shape == (224, 224, 3), f"Test {i}: Expected (224, 224, 3), got {result.shape}"
        assert result.dtype == np.uint8, f"Test {i}: Expected uint8, got {result.dtype}"
        assert result.min() >= 0 and result.max() <= 255, f"Test {i}: Values out of range [0, 255]"
    
    print("\n✅ All tests passed! The safe_to_uint8_rgb function now handles all cases correctly.")
    print("   - All outputs are (H, W, 3) uint8 format")
    print("   - No dimension mismatches should occur in np.concatenate")

if __name__ == "__main__":
    test_safe_to_uint8_rgb()