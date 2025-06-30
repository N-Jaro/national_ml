# gdrive_manager.py

import os
import io
import time
from config import Settings

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

class GoogleDriveManager:
    """
    Handles authentication and file/folder operations with Google Drive,
    including logic to merge duplicate folders created by GEE.
    """
    def __init__(self, settings: Settings):
        self.settings = settings
        self.service = self._authenticate()
        if self.service:
            print("Google Drive Manager successfully initialized.")
        else:
            raise ConnectionError("Failed to initialize Google Drive Manager.")

    def _authenticate(self):
        # ... (Authentication logic remains the same)
        creds = None
        token_file = self.settings.GDRIVE_TOKEN_FILE
        credentials_file = self.settings.GDRIVE_CREDENTIALS_FILE
        scopes = self.settings.GDRIVE_SCOPES
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes=scopes, redirect_uri='urn:ietf:wg:oauth:2.0:oob')
                auth_url, _ = flow.authorization_url(prompt='consent')
                print("\nPlease go to this URL to authorize the application:")
                print(auth_url)
                code = input('\nEnter the authorization code here: ')
                flow.fetch_token(code=code)
                creds = flow.credentials
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        try:
            return build('drive', 'v3', credentials=creds)
        except HttpError as error:
            return None

    def merge_and_download_huc_outputs(self, huc_id: str, local_destination_path: str):
        """
        Finds all GEE-exported folders for a HUC, merges them on Drive,
        downloads the result, and cleans up.
        """
        print(f"\nProcessing Google Drive outputs for HUC {huc_id}...")
        os.makedirs(local_destination_path, exist_ok=True)
        
        try:
            # 1. Find all folders matching the HUC ID name
            query = f"mimeType='application/vnd.google-apps.folder' and name='{huc_id}' and trashed=false"
            results = self.service.files().list(q=query, fields="files(id, name, parents)", spaces='drive').execute()
            folders = results.get('files', [])

            if not folders:
                print(f"  - No folders found on Google Drive for HUC '{huc_id}'. Skipping.")
                return

            print(f"  - Found {len(folders)} folder(s) for HUC '{huc_id}'.")
            
            # 2. Merge if necessary
            target_folder = folders.pop(0) # Designate the first folder as the master
            if folders: # If there are more folders to merge
                print(f"  - Merging content into target folder: {target_folder['name']} (ID: {target_folder['id']})")
                for source_folder in folders:
                    self._move_folder_contents(source_folder['id'], target_folder['id'])
                    # Delete the now-empty source folder
                    print(f"  - Deleting empty source folder: {source_folder['name']}")
                    self.service.files().delete(fileId=source_folder['id']).execute()

            # 3. Download the contents of the final merged folder
            print(f"  - Downloading final contents from '{target_folder['name']}' to '{local_destination_path}'...")
            self._download_contents(target_folder['id'], local_destination_path)

            # 4. Clean up: delete the final, now-empty remote folder
            print(f"  - Cleaning up final remote folder: {target_folder['name']}")
            self.service.files().delete(fileId=target_folder['id']).execute()

        except HttpError as error:
            print(f"An error occurred processing HUC {huc_id}: {error}")

    def _move_folder_contents(self, source_folder_id: str, target_folder_id: str):
        """Moves all items from a source folder to a target folder."""
        query = f"'{source_folder_id}' in parents and trashed=false"
        response = self.service.files().list(q=query, fields='files(id, parents)', spaces='drive').execute()
        files_to_move = response.get('files', [])
        
        for file_item in files_to_move:
            previous_parents = ",".join(file_item.get('parents', []))
            self.service.files().update(
                fileId=file_item['id'],
                addParents=target_folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()

    def _download_contents(self, folder_id: str, local_path: str):
        """Helper to download all files from a given folder ID."""
        query = f"'{folder_id}' in parents and trashed = false"
        response = self.service.files().list(q=query, fields='files(id, name, mimeType)', spaces='drive').execute()
        items = response.get('files', [])
        
        if not items:
            print("    - Note: Remote folder is empty.")

        for item in items:
            item_path = os.path.join(local_path, item['name'])
            request = self.service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            with open(item_path, 'wb') as f:
                f.write(fh.getvalue())
