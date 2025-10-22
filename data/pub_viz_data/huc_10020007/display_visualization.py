#!/usr/bin/env python3
"""
Simple script to display the generated publication visualization
"""

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from pathlib import Path

def show_visualization():
    """Display the generated publication visualization."""
    
    viz_path = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/visualizations/huc_10020007_publication_multimodal_visualization.png")
    
    if not viz_path.exists():
        print(f"❌ Visualization not found at: {viz_path}")
        return
    
    # Load and display the image
    img = mpimg.imread(viz_path)
    
    plt.figure(figsize=(20, 16))
    plt.imshow(img)
    plt.axis('off')
    plt.title('HUC 10020007 - Publication Multimodal Visualization', fontsize=16, pad=20)
    plt.tight_layout()
    
    print(f"📸 Displaying: {viz_path}")
    print(f"📏 Image shape: {img.shape}")
    
    plt.show()

if __name__ == "__main__":
    show_visualization()