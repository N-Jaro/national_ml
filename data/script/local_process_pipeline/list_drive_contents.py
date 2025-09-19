#!/usr/bin/env python3
"""
Check what folders exist in Google Drive.
"""

import ee
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Settings
from drive_manager import GoogleDriveManager

def list_drive_folders():
    """List all folders in Google Drive."""
    
    # Initialize EE
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print(f"✅ Initialized GEE with project: {settings.GEE_PROJECT_ID}")
    
    # Initialize Drive manager
    try:
        # Change to script directory where credentials are located
        original_cwd = os.getcwd()
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        os.chdir(script_dir)
        
        drive_manager = GoogleDriveManager(settings)
        os.chdir(original_cwd)  # Restore original directory
        print("✅ Drive manager initialized successfully")
    except Exception as e:
        if 'original_cwd' in locals():
            os.chdir(original_cwd)
        print(f"❌ Drive manager failed: {e}")
        return
    
    # List all folders
    try:
        query = "mimeType='application/vnd.google-apps.folder' and trashed=false"
        fields = "files(id, name)"
        results = drive_manager.service.files().list(q=query, fields=fields, spaces='drive').execute()
        items = results.get('files', [])
        
        print("📁 Available folders in Google Drive:")
        for item in items:
            print(f"  - {item['name']} (ID: {item['id']})")
            
        # Also list files in the root to see if there are any GeoTIFF files
        query = "mimeType='image/tiff' and trashed=false"
        fields = "files(id, name, parents)"
        results = drive_manager.service.files().list(q=query, fields=fields, spaces='drive').execute()
        items = results.get('files', [])
        
        print("📄 Available GeoTIFF files in Google Drive:")
        for item in items:
            parents = item.get('parents', ['root'])
            print(f"  - {item['name']} (ID: {item['id']}, Parent: {parents[0]})")
            
        # Look specifically for our test file
        query = "name contains 'test_multiband_landsat' and trashed=false"
        fields = "files(id, name, parents)"
        results = drive_manager.service.files().list(q=query, fields=fields, spaces='drive').execute()
        items = results.get('files', [])
        
        print("🎯 Test files found:")
        for item in items:
            parents = item.get('parents', ['root'])
            print(f"  - {item['name']} (ID: {item['id']}, Parent: {parents[0]})")
            
            # Download this file directly
            if item['name'].endswith('.tif'):
                print(f"📥 Downloading {item['name']}...")
                request = drive_manager.service.files().get_media(fileId=item['id'])
                
                import io
                from googleapiclient.http import MediaIoBaseDownload
                
                file_path = f"/tmp/{item['name']}"
                with io.FileIO(file_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        print(f"  Download progress: {int(status.progress() * 100)}%")
                
                # Check the file
                import rasterio
                with rasterio.open(file_path) as src:
                    print(f"🎉 SUCCESS: Downloaded file has {src.count} bands!")
                    print(f"🔍 Band descriptions: {src.descriptions}")
                    print(f"🔍 Data types: {src.dtypes}")
                    print(f"🔍 Shape: {src.shape}")
                    print(f"📁 File saved to: {file_path}")
        
    except Exception as e:
        print(f"❌ Error listing folders: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    list_drive_folders()