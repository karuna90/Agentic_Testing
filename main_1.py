import os
import io
import base64
import re
from datetime import datetime

# Google API Clients (for I/O)
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# LLM Client (for Extraction/Reasoning)
from google import genai
from PyPDF2 import PdfReader

# --- CONFIGURATION AND AUTHENTICATION (Same as before) ---
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/drive.file'
]

def authenticate_google_services():
    # (Authentication logic remains the same)
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def create_drive_folder(drive_service, folder_name):
    # (Drive Folder creation logic remains the same)
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    file = drive_service.files().create(body=file_metadata, fields='id').execute()
    print(f"Created Drive Folder: {folder_name}")
    return file.get('id')

def upload_to_drive(drive_service, folder_id, filename, file_data):
    # (File upload logic remains the same)
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='application/pdf')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Uploaded: {filename}")

def get_gmail_attachments(gmail_service, user_id='me'):
    # (Gmail search and download logic remains the same)
    # Search for messages with PDF attachments AND the specific subject line
    query = 'has:attachment filename:pdf subject:"test 123"'
    results = gmail_service.users().messages().list(userId=user_id, q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print("No emails with PDF attachments matching the criteria found.")
        return

    print(f"Found {len(messages)} emails with PDFs. Processing...")

    # The order returned by the API is typically most recent first, which we rely on
    for message in messages:
        msg = gmail_service.users().messages().get(userId=user_id, id=message['id']).execute()
        
        for part in msg['payload'].get('parts', []):
            if part['filename'] and part['filename'].lower().endswith('.pdf'):
                if 'data' in part['body']:
                    data = part['body']['data']
                else:
                    att_id = part['body']['attachmentId']
                    att = gmail_service.users().messages().attachments().get(
                        userId=user_id, messageId=message['id'], id=att_id).execute()
                    data = att['data']
                
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                yield part['filename'], file_data


# --- LLM TOOL: THE AGENT'S REASONING ENGINE ---

def extract_bl_number_llm_tool(pdf_data):
    """
    Uses the Gemini LLM to reliably extract the BL number from fuzzy text.
    This replaces all complex RegEx/String splitting logic.
    """
    # 1. Convert PDF binary data to plain text using PyPDF2 (The LLM cannot read binary files)
    pdf_file = io.BytesIO(pdf_data)
    reader = PdfReader(pdf_file)
    full_text = ""
    for page in reader.pages:
        full_text += page.extract_text().replace('\n', ' ').replace('\r', ' ').replace('  ', ' ')
    
    # Check if text extraction failed
    if not full_text.strip():
        return "BL_Number_Not_Found"
    
    # 2. Define the extraction goal (The Agent's Prompt)
    prompt = f"""
    Analyze the text provided below. Your sole objective is to extract the primary 
    Bill of Lading (B/L) number. 
    
    You MUST ignore container numbers, seal numbers, invoice numbers, or reference IDs.
    The B/L number is typically an alphanumeric string 8 to 12 characters long.
    
    OUTPUT FORMAT: ONLY the extracted B/L number (e.g., ABC12345678). 
    If you cannot find a clear B/L number, output the exact phrase: BL_Number_Not_Found
    
    --- DOCUMENT TEXT ---
    {full_text[:3000]}
    """
    
    try:
        # Initialize the LLM client (reads GEMINI_API_KEY from environment)
        client = genai.Client()
        
        # Call the LLM
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={"temperature": 0.0} # Set low temperature for reliable extraction
        )
        
        # Clean the output to ensure it matches the expected format
        extracted_bl = response.text.strip().upper()
        
        # Final safety check on the LLM's output
        if extracted_bl in ("BL_NUMBER_NOT_FOUND", "BL NUMBER NOT FOUND"):
            return "BL_Number_Not_Found"
        
        # Use a secondary RegEx to clean up any unwanted prefixes/suffixes 
        # that the LLM might have added (e.g., "BL: " or trailing commas)
        # This is the self-correction layer.
        clean_match = re.search(r'([A-Z0-9]{8,})', extracted_bl)
        if clean_match:
             return f"BL-{clean_match.group(1)}"
        else:
             return f"BL-{extracted_bl}"
             
    except Exception as e:
        print(f"LLM Extraction Error: {e}")
        return "BL_Number_Not_Found"


# --- MAIN ORCHESTRATOR ---

def main():
    creds = authenticate_google_services()
    gmail_service = build('gmail', 'v1', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    all_files_to_upload = []
    latest_bl_number = None

    # Step 1: Gather files and extract the BL number from the latest email (first file)
    for filename, file_data in get_gmail_attachments(gmail_service):
        
        # --- LLM AGENT CALL ---
        # The agent asks the LLM tool for the BL number instead of running RegEx
        current_bl_number = extract_bl_number_llm_tool(file_data) 
        
        if latest_bl_number is None:
            # Set the folder name based on the BL number from the latest email
            latest_bl_number = current_bl_number
            
        # Store all files for the final upload stage
        all_files_to_upload.append({
            'filename': filename,
            'file_data': file_data
        })
    
    # Step 2: Create the folder and upload
    if not all_files_to_upload:
        print("Agent found no documents to process.")
        return
        
    # Use the BL number from the latest document to name the folder
    folder_name = f"Shipment Documents - {latest_bl_number}"
    folder_id = create_drive_folder(drive_service, folder_name)
    
    if "Not_Found" in latest_bl_number:
         print("Agent WARNING: Could not reliably extract BL Number from latest PDF. Using generic name.")

    # Loop through all saved files and upload them to the single folder
    for file_info in all_files_to_upload:
        upload_to_drive(
            drive_service, 
            folder_id, 
            file_info['filename'], 
            file_info['file_data']
        )
        
    print(f"Agent Success! All documents saved to: {folder_name}")

if __name__ == '__main__':
    main()
