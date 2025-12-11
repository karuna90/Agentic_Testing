import os
import base64
import io
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

#With BL_Number
import re
from PyPDF2 import PdfReader

import string

# 1. Define the scopes (permissions) your agent needs
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly', # Read emails
    'https://www.googleapis.com/auth/drive.file'      # Create/Edit files in Drive
]

def authenticate_google_services():
    """Handles the OAuth2 authentication flow."""
    creds = None
    # The file token.json stores the user's access and refresh tokens.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds

def create_drive_folder(drive_service, folder_name):
    """Creates a new folder in Google Drive and returns its ID."""
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    
    # Check if folder already exists (optional, but good practice)
    # For simplicity, this code always creates a new one.
    file = drive_service.files().create(body=file_metadata, fields='id').execute()
    print(f"Created Drive Folder: {folder_name} (ID: {file.get('id')})")
    return file.get('id')

def get_gmail_attachments(gmail_service, user_id='me'):
    """Searches for PDF attachments and yields them."""
    # Search for messages with PDF attachments and having subject line "test 123"
    query = 'has:attachment filename:pdf subject:"test 123"'
    results = gmail_service.users().messages().list(userId=user_id, q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print("No emails with PDF attachments found.")
        return

    print(f"Found {len(messages)} emails with PDFs. Processing...")

    for message in messages:
        msg = gmail_service.users().messages().get(userId=user_id, id=message['id']).execute()
        
        # Iterate through the parts of the email payload
        for part in msg['payload'].get('parts', []):
            if part['filename'] and part['filename'].lower().endswith('.pdf'):
                if 'data' in part['body']:
                    data = part['body']['data']
                else:
                    att_id = part['body']['attachmentId']
                    att = gmail_service.users().messages().attachments().get(
                        userId=user_id, messageId=message['id'], id=att_id).execute()
                    data = att['data']
                
                # Decode the attachment data
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                
                yield part['filename'], file_data

def upload_to_drive(drive_service, folder_id, filename, file_data):
    """Uploads a file data stream to a specific Drive folder."""
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    
    # Create an in-memory byte stream for the upload
    media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='application/pdf')
    
    file = drive_service.files().create(
        body=file_metadata, 
        media_body=media, 
        fields='id'
    ).execute()
    print(f"Uploaded: {filename}")

# Helper function to grab only the continuous ID string
def _get_alphanumeric_block(text, start_index):
    """Starts at the index and grabs the first continuous block of letters and digits."""
    bl_id = ""
    # We only care about letters and numbers
    valid_chars = string.ascii_letters + string.digits
    
    # Iterate through the text starting from the specified index
    for char in text[start_index:]:
        if char in valid_chars:
            bl_id += char
        elif bl_id: 
            # If we've started building the ID and hit a space/newline/punctuation, stop.
            break
            
    return bl_id

def extract_bl_number_from_pdf(pdf_data):
    """
    Reads PDF data and extracts an identifier by searching for keywords 
    and then precisely isolating the alphanumeric block that follows.
    """
    
    pdf_file = io.BytesIO(pdf_data)
    reader = PdfReader(pdf_file)
    
    full_text = ""
    # Extract text, remove newlines, and normalize spaces
    for page in reader.pages:
        full_text += page.extract_text().replace('\n', ' ').replace('\r', ' ').replace('  ', ' ')
    
    # --- Generic Keyword Search ---
    
    # Common keywords that precede the BL number
    keywords = ["B/L:", "BL:", "B/L No.", "Bill of Lading Number:", "Shipment No."]
    
    # Case-insensitivity prep: search text is uppercase
    full_text_upper = full_text.upper()
    
    for keyword in keywords:
        keyword_upper = keyword.upper()
        
        if keyword_upper in full_text_upper:
            
            # 1. Find the starting position of the keyword
            start_index = full_text_upper.find(keyword_upper)
            
            # 2. Set the search start index immediately after the keyword in the ORIGINAL text
            search_start = start_index + len(keyword_upper)
            
            # 3. Use the helper function to precisely extract the next alphanumeric block
            found_id = _get_alphanumeric_block(full_text, search_start).upper()
            
            if found_id:
                # Success: Return the ID prefixed for clarity
                return f"BL-{found_id}"

    # Fallback if no specific keyword is found after searching all pages
    return "BL_Number_Not_Found"

def main():
    # Authenticate and build services
    creds = authenticate_google_services()
    gmail_service = build('gmail', 'v1', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    # List to store all files from all matching emails
    all_files_to_upload = []
    
    # Variable to hold the BL number from the FIRST (latest) email
    latest_bl_number = None

    # Get attachments from Gmail and process them
    # The get_gmail_attachments function yields one file at a time
    for filename, file_data in get_gmail_attachments(gmail_service):
        
        # --- STEP 1: Extract the BL Number from the current PDF ---
        current_bl_number = extract_bl_number_from_pdf(file_data)
        
        # --- STEP 2: Store the BL number from the LATEST email ---
        if latest_bl_number is None:
            # Since the API usually returns most recent first, this is the latest.
            latest_bl_number = current_bl_number
            
        # --- STEP 3: Save the file data for later upload ---
        all_files_to_upload.append({
            'filename': filename,
            'file_data': file_data
        })
    
    # --- STEP 4: Create the folder based on the LATEST BL number ---
    if not all_files_to_upload:
        print("No documents were processed or found matching the criteria.")
        return
        
    folder_name = f"Shipment Documents - {latest_bl_number}"
    folder_id = create_drive_folder(drive_service, folder_name)
    
    # Check if the BL number extraction failed for naming the folder
    if "Not_Found" in latest_bl_number:
         print("WARNING: Could not find BL Number in the LATEST PDF. Folder named generically.")

    # --- STEP 5: Loop through all saved files and upload them ---
    for file_info in all_files_to_upload:
        upload_to_drive(
            drive_service, 
            folder_id, 
            file_info['filename'], 
            file_info['file_data']
        )
        
    print(f"Done! All relevant documents saved to the folder: {folder_name}")

if __name__ == '__main__':
    main()