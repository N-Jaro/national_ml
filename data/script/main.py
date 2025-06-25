
from config import Settings
from drive_manager import GoogleDriveManager
from gee_tools import GEEWorkflow


if __name__ == '__main__':
    settings = Settings()
    drive = GoogleDriveManager(settings)
    gee = GEEWorkflow(settings, drive)
    
    
    if drive.service:
        
        print("\n--- Start the GEE export task ---")
        
        gee.launch_all_export_tasks()
        gee.monitor_and_organize_tasks()

        print("\n--- Finished ---")