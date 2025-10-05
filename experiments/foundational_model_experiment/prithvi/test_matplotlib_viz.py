#!/usr/bin/env python3
"""
Test matplotlib subplot visualization for Prithvi
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

def test_matplotlib_visualization():
    print("🧪 Testing matplotlib subplot visualization...")
    
    # Create sample data similar to what we'd have in training
    H, W = 224, 224
    
    # Simulate the 7 images we want to display
    rgb_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    dem_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    thermal_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    sar_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    gt_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    pred_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    prob_rgb = np.random.rand(H, W, 3).astype(np.uint8) * 255
    
    print(f"✅ Created sample images: {rgb_rgb.shape}, dtype: {rgb_rgb.dtype}")
    
    # Test the matplotlib subplot creation
    try:
        fig, axes = plt.subplots(2, 4, figsize=(16, 8))
        fig.suptitle(f'Sample 1 - Epoch 0', fontsize=14)
        
        # Top row: [RGB Optical | DEM | Thermal | SAR]
        axes[0, 0].imshow(rgb_rgb)
        axes[0, 0].set_title('RGB Optical')
        axes[0, 0].axis('off')
        
        axes[0, 1].imshow(dem_rgb)
        axes[0, 1].set_title('DEM')
        axes[0, 1].axis('off')
        
        axes[0, 2].imshow(thermal_rgb)
        axes[0, 2].set_title('Thermal')
        axes[0, 2].axis('off')
        
        axes[0, 3].imshow(sar_rgb)
        axes[0, 3].set_title('SAR')
        axes[0, 3].axis('off')
        
        # Bottom row: [GT Water | Pred Water | Pred Prob | blank]
        axes[1, 0].imshow(gt_rgb)
        axes[1, 0].set_title('Ground Truth Water')
        axes[1, 0].axis('off')
        
        axes[1, 1].imshow(pred_rgb)
        axes[1, 1].set_title('Predicted Water')
        axes[1, 1].axis('off')
        
        axes[1, 2].imshow(prob_rgb)
        axes[1, 2].set_title('Prediction Probability')
        axes[1, 2].axis('off')
        
        axes[1, 3].axis('off')  # blank
        
        plt.tight_layout()
        
        # Save the figure to test
        plt.savefig('test_visualization.png', dpi=100, bbox_inches='tight')
        print("✅ Matplotlib subplot created successfully!")
        print("   - 2x4 grid layout")
        print("   - Individual titles for each subplot") 
        print("   - Professional appearance with tight_layout()")
        print("   - Saved as 'test_visualization.png'")
        
        plt.close(fig)  # Clean up memory
        
        print("\n🎨 Visualization Features:")
        print("   - Top row: Input modalities (RGB, DEM, Thermal, SAR)")
        print("   - Bottom row: Outputs (GT Water, Pred Water, Prob, blank)")
        print("   - Each subplot has descriptive title")
        print("   - Consistent layout and sizing")
        
    except Exception as e:
        print(f"❌ Error creating matplotlib visualization: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    test_matplotlib_visualization()