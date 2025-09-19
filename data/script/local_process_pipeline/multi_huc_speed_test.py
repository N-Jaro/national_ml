#!/usr/bin/env python3
"""
Multi-HUC Speed Test - Test processing multiple HUCs with optimizations.
"""

import sys
import os
import logging
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from batch_processor import BatchProcessor

def main():
    """Test multi-HUC processing with speed optimizations."""
    
    print("🚀 Multi-HUC Speed Test")
    print("=" * 50)
    
    try:
        processor = BatchProcessor()
        
        # Test configuration - multiple HUCs but limited sources for speed
        huc_ids = ["10020007", "10020008"]  # 2 HUCs for testing
        data_sources = ["dem", "landsat"]   # Faster sources first
        output_dir = Path("speed_test_results")
        
        print(f"📍 Testing HUCs: {huc_ids}")
        print(f"🛰️  Sources: {data_sources}")
        print(f"📁 Output: {output_dir}")
        
        # Run batch processing with fast mode
        print(f"\n⚡ Starting fast batch processing...")
        
        batch_summary = processor.process_multiple_hucs(
            huc_ids=huc_ids,
            data_sources=data_sources,
            start_date="2023-06-01",
            end_date="2023-09-30",
            output_dir=output_dir,
            fast_mode=True
        )
        
        print(f"\n✅ Multi-HUC test completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
