#!/usr/bin/env python3
"""
Test using the main pipeline config dates for Landsat download.
Demonstrates how to use the centralized config dates vs test-specific dates.
"""

import ee
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Settings
from local_config import LocalProcessingSettings
from data_manager import LocalDataManager

# Import the main downloader
from landsat_bulk_downloader import LandsatBulkDownloader

def test_with_config_dates():
    """Test using the main config dates from the pipeline."""
    
    # Initialize EE
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print(f"✅ Initialized GEE with project: {settings.GEE_PROJECT_ID}")
    
    # Initialize components
    config = LocalProcessingSettings()
    data_manager = LocalDataManager(config)
    downloader = LandsatBulkDownloader(config, data_manager)
    
    # Show the config dates
    print(f"\n📅 Main Pipeline Configuration:")
    print(f"   START_DATE: {config.START_DATE}")
    print(f"   END_DATE: {config.END_DATE}")
    print(f"   Date span: {(datetime.strptime(config.END_DATE, '%Y-%m-%d') - datetime.strptime(config.START_DATE, '%Y-%m-%d')).days} days")
    
    # Test HUC
    huc_id = "10020007"
    
    print(f"\n🎯 Testing options for HUC {huc_id}:")
    
    # Option 1: Use full config date range (might be very large!)
    print(f"\n1️⃣ Option 1: Full config date range")
    print(f"   Would download: {config.START_DATE} to {config.END_DATE}")
    print(f"   ⚠️ This is a {(datetime.strptime(config.END_DATE, '%Y-%m-%d') - datetime.strptime(config.START_DATE, '%Y-%m-%d')).days}-day span - very large dataset!")
    
    # Option 2: Use seasonal subset of config dates
    config_start = datetime.strptime(config.START_DATE, '%Y-%m-%d')
    config_end = datetime.strptime(config.END_DATE, '%Y-%m-%d')
    
    # Find summer 2023 within the config range
    summer_2023_start = max(config_start, datetime(2023, 6, 1))
    summer_2023_end = min(config_end, datetime(2023, 9, 30))
    
    print(f"\n2️⃣ Option 2: Seasonal subset (Summer 2023)")
    print(f"   Would download: {summer_2023_start.strftime('%Y-%m-%d')} to {summer_2023_end.strftime('%Y-%m-%d')}")
    print(f"   📏 This is a {(summer_2023_end - summer_2023_start).days}-day span - more manageable")
    
    # Option 3: Use monthly subset for testing
    monthly_start = datetime(2023, 7, 1)
    monthly_end = datetime(2023, 7, 31)
    
    print(f"\n3️⃣ Option 3: Monthly subset for testing (July 2023)")
    print(f"   Would download: {monthly_start.strftime('%Y-%m-%d')} to {monthly_end.strftime('%Y-%m-%d')}")
    print(f"   ⚡ This is a {(monthly_end - monthly_start).days}-day span - fastest for testing")
    
    # Let user choose or default to Option 3
    choice = input(f"\nChoose option (1, 2, or 3) or press Enter for Option 3: ").strip()
    
    if choice == "1":
        start_date = config.START_DATE
        end_date = config.END_DATE
        print(f"🚀 Using full config date range")
    elif choice == "2":
        start_date = summer_2023_start.strftime('%Y-%m-%d')
        end_date = summer_2023_end.strftime('%Y-%m-%d')
        print(f"🌞 Using summer 2023 subset")
    else:
        start_date = monthly_start.strftime('%Y-%m-%d')
        end_date = monthly_end.strftime('%Y-%m-%d')
        print(f"⚡ Using July 2023 for fast testing")
    
    print(f"\n📊 Starting download with:")
    print(f"   HUC: {huc_id}")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Resolution: 30m")
    
    try:
        # Use the new method that can take config dates
        output_path = downloader.download_landsat_for_huc(
            huc_id, start_date, end_date, resolution=30.0
        )
        
        print(f"\n✅ Download completed: {output_path}")
        
        # Verify the output file
        import rasterio
        if os.path.exists(output_path):
            with rasterio.open(output_path) as src:
                print(f"📊 Output verification:")
                print(f"   Dimensions: {src.shape}")
                print(f"   Bands: {src.count}")
                print(f"   CRS: {src.crs}")
                print(f"   File size: {os.path.getsize(output_path) / 1e6:.1f} MB")
                
                # Check if this looks like valid data
                if src.count == 7:
                    print(f"   ✅ Multi-band Landsat data successfully downloaded!")
                    
                    # Quick data stats
                    sample_band = src.read(1)
                    valid_pixels = (~np.isnan(sample_band)).sum()
                    total_pixels = sample_band.size
                    coverage = (valid_pixels / total_pixels) * 100
                    print(f"   📈 Data coverage: {coverage:.1f}% valid pixels")
                else:
                    print(f"   ⚠️ Expected 7 bands, got {src.count}")
        else:
            print(f"❌ Output file not found: {output_path}")
            
    except Exception as e:
        print(f"❌ Download failed: {e}")
        import traceback
        traceback.print_exc()

def show_date_configuration_options():
    """Show different ways to configure dates for the pipeline."""
    
    config = Settings()
    
    print(f"📅 LANDSAT DATE CONFIGURATION OPTIONS")
    print(f"=" * 50)
    
    print(f"\n🔧 Current Config Settings (config.py):")
    print(f"   START_DATE = '{config.START_DATE}'")
    print(f"   END_DATE = '{config.END_DATE}'")
    
    # Calculate some useful date ranges
    config_start = datetime.strptime(config.START_DATE, '%Y-%m-%d')
    config_end = datetime.strptime(config.END_DATE, '%Y-%m-%d')
    total_days = (config_end - config_start).days
    
    print(f"\n📊 Analysis:")
    print(f"   Total span: {total_days} days ({total_days/365.25:.1f} years)")
    print(f"   Covers: {config_start.year} to {config_end.year}")
    
    print(f"\n💡 Recommended Usage Patterns:")
    
    print(f"\n1️⃣ Production Processing:")
    print(f"   Use full config range for comprehensive datasets")
    print(f"   Good for: Final model training, full-scale analysis")
    print(f"   Duration: {total_days} days")
    
    print(f"\n2️⃣ Seasonal Analysis:")
    print(f"   Use 3-6 month windows (e.g., summer 2023)")
    print(f"   Good for: Seasonal studies, reduced cloud cover")
    print(f"   Example: 2023-06-01 to 2023-09-30 (122 days)")
    
    print(f"\n3️⃣ Testing & Development:")
    print(f"   Use 1-3 month windows")
    print(f"   Good for: Pipeline testing, rapid iteration")
    print(f"   Example: 2023-07-01 to 2023-07-31 (31 days)")
    
    print(f"\n4️⃣ Event-based Processing:")
    print(f"   Use specific date ranges around events")
    print(f"   Good for: Disaster response, change detection")
    print(f"   Example: 2023-08-15 to 2023-09-15 (31 days)")
    
    print(f"\n🎯 Recommendation:")
    if total_days > 365:
        print(f"   Config spans {total_days/365.25:.1f} years - consider seasonal subsets for most tasks")
    else:
        print(f"   Config spans {total_days} days - reasonable for most processing tasks")

if __name__ == "__main__":
    # Show configuration options first
    show_date_configuration_options()
    
    # Ask if user wants to proceed with test
    proceed = input(f"\nProceed with download test? (y/n): ").strip().lower()
    if proceed in ['y', 'yes']:
        test_with_config_dates()
    else:
        print(f"📋 To update config dates, edit: /u/nathanj/national_ml/data/script/config.py")
        print(f"    Lines 28-29: START_DATE and END_DATE")