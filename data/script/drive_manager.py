import os
import time
import io
from config import Settings
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


class GoogleDriveManager:
    
    def __init__(self, settings:Settings):
        self.settings = settings
        self.service = self._authenticate()
    
    def _authenticate(self):
        creds = None
        SCOPES = ['https://www.googleapis.com/auth/drive']
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.settings.GDRIVE_CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        
        try:
            service = build('drive', 'v3', credentials=creds)
            print("Google Drive API successfully initialized.")
            return service
        except HttpError as error:
            print(f'Error occur when launched Google Drive service: {error}')
            return None
        
    def merge_duplicate_folders(self, folder_name):
        """
        Search for all folders with the specified name in Google Drive,
        merge their contents into the first found folder, and delete the others.
        :param drive_service: Google Drive API service instance
        :param folder_name: The name of the folders to search for and merge
        """
        print(f"\n--- Searching named '{folder_name}' folder ---")
        
        try:
            # search for folders with the specified name
            query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
            fields = "files(id, name, parents)"
            results = self.service.files().list(q=query, fields=fields, spaces='drive').execute()
            folders = results.get('files', [])

            if len(folders) <= 1:
                print(f"Found {len(folders)} folders named '{folder_name}'. No need to merge.")
                return

            print(f"Found {len(folders)} folders. Get ready to merge...")
            time.sleep(2) 

            # Specify the first folder as the target and the rest as the sources
            target_folder = folders.pop(0)
            source_folders = folders
            
            print(f"Target folder: '{target_folder['name']}' (ID: {target_folder['id']})")

            # Traverse all the source folders
            for source in source_folders:
                print(f"\n--- Processing source folder: '{source['name']}' (ID: {source['id']}) ---")
                
                page_token = None
                while True:
                    # search for all items in the source folder
                    response = self.service.files().list(
                        q=f"'{source['id']}' in parents and trashed=false",
                        fields="nextPageToken, files(id, name, parents)",
                        spaces='drive',
                        pageToken=page_token
                    ).execute()
                    
                    items_to_move = response.get('files', [])

                    if not items_to_move:
                        print("There is no content to be moved in this source folder")
                        break

                    # move each item to the target folder
                    for item in items_to_move:
                        print(f"  Moving: '{item['name']}' (ID: {item['id']})")
                        previous_parents = ",".join(item.get('parents', []))
                        
                        try:
                            self.service.files().update(
                                fileId=item['id'],
                                addParents=target_folder['id'],
                                removeParents=previous_parents,
                                fields='id, parents'
                            ).execute()
                        except HttpError as error:
                            print(f"    An error {error} occurred when moving item '{item['name']}'")

                    page_token = response.get('nextPageToken', None)
                    if page_token is None:
                        break
                
                # Delete the emptied source folder
                print(f"--- Source folder '{source['name']}' is empty, ready to delete ---")
                try:
                    self.service.files().delete(fileId=source['id']).execute()
                    print(f"Successfully deleted the folder (ID: {source['id']})")
                except HttpError as error:
                    print(f"A error{error} occur when deleting folder '{source['name']}'. ")

            print("\n--- All duplicate folders have been merged successfully! ---")

        except HttpError as error:
            print(f"An error occurred when performing the merge operation: {error}")


    def move_folder(self, folder_to_move_name, destination_folder_name):   
        print(f"\n--- Prepare to put the folder '{folder_to_move_name}' to '{destination_folder_name}' ---")

        folder_a_id, original_parent_id = self.get_folder_info(folder_to_move_name)
        if not folder_a_id or not original_parent_id:
            print("Cannot continue the move operation because the source folder does not exist or is not accessible.")
            return

        # get the destination folder ID
        folder_b_id, _ = self.get_folder_info(destination_folder_name)
        if not folder_b_id:
            print("Cannot continue the move operation because the destination folder does not exist or is not accessible.")
            return
            
        print(f"Moved folder '{folder_to_move_name}' ID: {folder_a_id}")
        print(f"Original Parent ID: {original_parent_id}")
        print(f"Destination folder '{destination_folder_name}' ID: {folder_b_id}")

        try:
            print("The movement operation is being performed...")
            self.service.files().update(
                fileId=folder_a_id,
                addParents=folder_b_id,
                removeParents=original_parent_id,
                fields='id, parents'  # fields which fileds the API should return
            ).execute()
            print("--- Operation successful! The folder has been moved. ---")
        except HttpError as error:
            print(f"An error occurred when moving the folder: {error}")


    def get_folder_info(self, folder_name):
        try:
            query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
            fields = "files(id, name, parents)"
            results = self.service.files().list(q=query, fields=fields, spaces='drive').execute()
            items = results.get('files', [])
            
            if not items:
                print(f"Error：cannot found'{folder_name}' folder。")
                return None, None
            
            if len(items) > 1:
                print(f"Warining：found multiple '{folder_name}' folder. will use the first one found.")
                
            folder_id = items[0]['id']
            parent_id = items[0].get('parents', [None])[0]
            return folder_id, parent_id

        except HttpError as error:
            print(f"A error {error} occur when found'{folder_name}' folder.")
            return None, None
        

    def get_gdrive_folder_id(self,folder_name, parent_id=None):
        """get the ID of a Google Drive folder by its name"""
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        try:
            results =self.service.files().list(q=query, fields="files(id)").execute()
            items = results.get('files', [])
            return items[0]['id'] if items else None
        except HttpError as error:
            print(f"A error {error} occur when found'{folder_name}' folder.")
            return None


    def download_folder_recursively(self,folder_id, local_path):
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        query = f"'{folder_id}' in parents"
        results = self.service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        items = results.get('files', [])

        for item in items:
            item_name = item['name']
            item_id = item['id']
            item_path = os.path.join(local_path, item_name)

            if item['mimeType'] == 'application/vnd.google-apps.folder':
                print(f"Enter the subfolder: {item_path}")
                self.download_folder_recursively(item_id, item_path)
            else:
                print(f"Ready to download: {item_path}")
                request = self.service.files().get_media(fileId=item_id)
                with io.FileIO(item_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        print(f"  Download progress: {int(status.progress() * 100)}%")
                print(f"  Download finished: {item_name}")
        
    