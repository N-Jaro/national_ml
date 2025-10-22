#!/usr/bin/env python3
"""
Terrain Classification for Transfer Analysis
Calculate terrain relief and classify patches into terrain bins.
"""

import numpy as np
import torch
from typing import Dict, List, Tuple, Union
from config import config

class TerrainClassifier:
    """Classify patches based on terrain relief (elevation range)."""
    
    def __init__(self, low_threshold: float = None, high_threshold: float = None):
        """
        Initialize terrain classifier.
        
        Args:
            low_threshold: Relief threshold for low terrain (default from config)
            high_threshold: Relief threshold for high terrain (default from config)
        """
        self.low_threshold = low_threshold or config.low_relief_threshold
        self.high_threshold = high_threshold or config.high_relief_threshold
        
        print(f"🏔️  Terrain Classifier initialized:")
        print(f"   Low relief: <{self.low_threshold}m")
        print(f"   High relief: >{self.high_threshold}m")
    
    def calculate_relief(self, dem_patch: Union[np.ndarray, torch.Tensor]) -> float:
        """
        Calculate terrain relief (elevation range) for a single patch.
        
        Args:
            dem_patch: DEM data array, shape (H, W) or (1, H, W)
            
        Returns:
            relief: Elevation range in meters
        """
        if isinstance(dem_patch, torch.Tensor):
            dem_patch = dem_patch.cpu().numpy()
        
        # Handle different shapes
        if dem_patch.ndim == 3:
            dem_patch = dem_patch[0]  # Take first channel if (C, H, W)
        elif dem_patch.ndim == 4:
            dem_patch = dem_patch[0, 0]  # Take first batch, first channel if (B, C, H, W)
        
        if dem_patch.size == 0:
            return 0.0
        
        # Calculate relief = max - min elevation
        relief = float(np.max(dem_patch) - np.min(dem_patch))
        return relief
    
    def classify_terrain(self, relief: float) -> str:
        """
        Classify terrain based on relief value.
        
        Args:
            relief: Relief value in meters
            
        Returns:
            terrain_class: 'low_relief', 'medium_relief', or 'high_relief'
        """
        if relief < self.low_threshold:
            return "low_relief"
        elif relief > self.high_threshold:
            return "high_relief"
        else:
            return "medium_relief"
    
    def classify_patch(self, dem_patch: Union[np.ndarray, torch.Tensor]) -> Tuple[float, str]:
        """
        Calculate relief and classify a single patch.
        
        Args:
            dem_patch: DEM data array
            
        Returns:
            relief: Relief value in meters
            terrain_class: Terrain classification
        """
        relief = self.calculate_relief(dem_patch)
        terrain_class = self.classify_terrain(relief)
        return relief, terrain_class
    
    def classify_batch(self, dem_batch: Union[np.ndarray, torch.Tensor]) -> List[Tuple[float, str]]:
        """
        Classify a batch of patches.
        
        Args:
            dem_batch: Batch of DEM patches, shape (B, C, H, W) or (B, H, W)
            
        Returns:
            classifications: List of (relief, terrain_class) tuples
        """
        classifications = []
        
        if isinstance(dem_batch, torch.Tensor):
            dem_batch = dem_batch.cpu().numpy()
        
        # Handle different batch shapes
        if dem_batch.ndim == 4:  # (B, C, H, W)
            for i in range(dem_batch.shape[0]):
                dem_patch = dem_batch[i, 0] if dem_batch.shape[1] > 0 else dem_batch[i]
                relief, terrain_class = self.classify_patch(dem_patch)
                classifications.append((relief, terrain_class))
        elif dem_batch.ndim == 3:  # (B, H, W)
            for i in range(dem_batch.shape[0]):
                relief, terrain_class = self.classify_patch(dem_batch[i])
                classifications.append((relief, terrain_class))
        else:
            raise ValueError(f"Unexpected batch shape: {dem_batch.shape}")
        
        return classifications
    
    def get_terrain_statistics(self, classifications: List[Tuple[float, str]]) -> Dict:
        """
        Calculate statistics for terrain classifications.
        
        Args:
            classifications: List of (relief, terrain_class) tuples
            
        Returns:
            stats: Dictionary with terrain statistics
        """
        reliefs = [r for r, _ in classifications]
        terrain_classes = [t for _, t in classifications]
        
        # Count by terrain class
        terrain_counts = {}
        for terrain_class in terrain_classes:
            terrain_counts[terrain_class] = terrain_counts.get(terrain_class, 0) + 1
        
        # Relief statistics
        reliefs_array = np.array(reliefs)
        
        stats = {
            "total_patches": len(classifications),
            "terrain_counts": terrain_counts,
            "relief_stats": {
                "mean": float(np.mean(reliefs_array)),
                "std": float(np.std(reliefs_array)),
                "min": float(np.min(reliefs_array)),
                "max": float(np.max(reliefs_array)),
                "median": float(np.median(reliefs_array))
            },
            "terrain_percentages": {
                terrain: count / len(classifications) * 100
                for terrain, count in terrain_counts.items()
            },
            "thresholds": {
                "low_threshold": self.low_threshold,
                "high_threshold": self.high_threshold
            }
        }
        
        return stats
    
    def print_statistics(self, stats: Dict):
        """Print terrain classification statistics."""
        print(f"\n📊 TERRAIN CLASSIFICATION STATISTICS")
        print("=" * 40)
        print(f"Total patches: {stats['total_patches']}")
        print(f"Relief range: {stats['relief_stats']['min']:.1f} - {stats['relief_stats']['max']:.1f}m")
        print(f"Mean relief: {stats['relief_stats']['mean']:.1f} ± {stats['relief_stats']['std']:.1f}m")
        
        print(f"\nTerrain distribution:")
        for terrain, count in stats['terrain_counts'].items():
            percentage = stats['terrain_percentages'][terrain]
            print(f"  {terrain:>12}: {count:4d} patches ({percentage:5.1f}%)")

def test_terrain_classifier():
    """Test terrain classifier with sample data."""
    print("🧪 TESTING TERRAIN CLASSIFIER")
    print("=" * 40)
    
    classifier = TerrainClassifier()
    
    # Create test patches with known relief
    low_relief_patch = np.random.randn(224, 224) * 5 + 100  # ~5m relief
    high_relief_patch = np.random.randn(224, 224) * 200 + 1000  # ~200m relief
    
    # Test single patch classification
    relief1, class1 = classifier.classify_patch(low_relief_patch)
    relief2, class2 = classifier.classify_patch(high_relief_patch)
    
    print(f"Test patch 1: {relief1:.1f}m relief → {class1}")
    print(f"Test patch 2: {relief2:.1f}m relief → {class2}")
    
    # Test batch classification
    batch = np.stack([low_relief_patch, high_relief_patch])
    batch_classifications = classifier.classify_batch(batch)
    
    print(f"Batch classification: {len(batch_classifications)} patches")
    
    # Test statistics
    stats = classifier.get_terrain_statistics(batch_classifications)
    classifier.print_statistics(stats)
    
    print("\n✅ Terrain classifier test completed!")

if __name__ == "__main__":
    test_terrain_classifier()