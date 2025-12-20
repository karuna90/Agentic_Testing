import os
import io
import base64
import re
import json
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

# --- CONFIGURATION ---
# NOTE: You MUST delete token.json and rerun the script to get all these permissions!
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets' 
]

# Sheet ID from the URL: https://docs.google.com/spreadsheets/d/15A7BklQEKv3a7PGg4kr7QZK6sA8nlDuMfX49XDZwxdo/edit?gid=0#gid=0
SPREADSHEET_ID = '15A7BklQEKv3a7PGg4kr7QZK6sA8nlDuMfX49XDZwxdo'
SHEET_RANGE = 'Sheet1!A:E'  # Range to read (Container No., X, BL No.)

# --- I/O AND AUTHENTICATION FUNCTIONS ---

def authenticate_google_services():
    """Handles OAuth 2.0 flow and builds Gmail, Drive, and Sheets services."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Force re-authentication if new scopes are required (delete token.json first!)
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    sheets_service = build('sheets', 'v4', credentials=creds)
    return creds, sheets_service

def create_drive_folder(drive_service, folder_name):
    """Creates a folder in the root of My Drive."""
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    file = drive_service.files().create(body=file_metadata, fields='id').execute()
    print(f"Created Drive Folder: {folder_name}")
    return file.get('id')

def upload_to_drive(drive_service, folder_id, filename, file_data):
    """Uploads the file data to the specified Drive folder."""
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='application/pdf')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Uploaded: {filename}")

def get_gmail_attachments(gmail_service, user_id='me'):
    """Searches Gmail and yields the filename and binary data for matching PDFs."""
    # Customize this query! E.g., 'has:attachment filename:pdf subject:"BL Documents"'
    query = 'has:attachment filename:pdf subject:"test 1234"' 
    
    results = gmail_service.users().messages().list(userId=user_id, q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print("No emails with PDF attachments matching the criteria found.")
        return

    print(f"Agent found {len(messages)} matching emails. Processing...")

    # Order is usually most recent first, which satisfies the "latest email" requirement
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

def get_sheet_data(sheets_service):
    """Retrieves all data from the Sheet's defined range."""
    print("Agent is reading Google Sheet for container data...")
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_RANGE
    ).execute()
    return result.get('values', [])

def update_sheet_cell(sheets_service, row_index, bl_number, bl_column_letter='E'):
    """Updates the specified cell (BL Column) in the Sheet."""
    
    # Calculate A1 notation for the target cell (e.g., C5 for row index 4)
    # Sheets rows are 1-indexed (row_index + 1), Python lists are 0-indexed.
    range_name = f'Sheet1!{bl_column_letter}{row_index + 1}' 
    
    print(f"Updating Sheet at {range_name} with BL: {bl_number}")
    
    value_input_option = 'USER_ENTERED'
    value_range_body = {
        'values': [[bl_number]]
    }
    
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, 
        range=range_name, 
        valueInputOption=value_input_option, 
        body=value_range_body
    ).execute()


# --- LLM TOOL: SHIPPING DETAILS EXTRACTION ---

def extract_shipping_details_llm_tool(pdf_data):
    """
    Uses the Gemini LLM to reliably extract both BL and Container numbers 
    from fuzzy PDF text by demanding a structured JSON output. Includes
    a robust RegEx filter to clean the LLM's response.
    """
    pdf_file = io.BytesIO(pdf_data)
    reader = PdfReader(pdf_file)
    full_text = ""
    # Extract text from all pages and normalize whitespace
    for page in reader.pages:
        full_text += page.extract_text().replace('\n', ' ').replace('\r', ' ').replace('  ', ' ')
    
    if not full_text.strip():
        return None

    # The prompt explicitly demands a structured JSON output
    prompt = f"""
    Analyze the text provided below. Extract the primary Bill of Lading (B/L) number 
    and the main Container Number (CN).
    
    OUTPUT FORMAT: ONLY a valid JSON object with two keys: 'BL_Number' and 'Container_Number'.
    Do NOT include any surrounding text, explanations, or formatting other than the JSON object itself.
    If a number cannot be found, set its value to 'NOT_FOUND'. 
    
    Example Output: {{"BL_Number": "ABC12345678", "Container_Number": "TEMU1234567"}}
    
    --- DOCUMENT TEXT ---
    {full_text[:3000]}
    """
    
    try:
        client = genai.Client()
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={"temperature": 0.0}
        )
        
        # --- THE CRITICAL FIX: Slicing the JSON out of the response ---
        llm_output = response.text.strip()
        
        # RegEx to find the first block that starts with '{' and ends with '}'
        # and capture the content in between. This ignores preambles/epilogues.
        json_match = re.search(r'\{.*\}', llm_output, re.DOTALL)
        
        if not json_match:
            print(f"LLM Output was not a recognizable JSON string: {llm_output[:50]}...")
            return None
        
        clean_json_string = json_match.group(0)
        # --- END OF CRITICAL FIX ---
        
        # Parse the clean JSON string
        details = json.loads(clean_json_string)
        
        # Clean and uppercase the results for standardization
        details['BL_Number'] = details.get('BL_Number', 'NOT_FOUND').strip().upper()
        details['Container_Number'] = details.get('Container_Number', 'NOT_FOUND').strip().upper()

        return details
        
    except Exception as e:
        # This will now only catch true JSON format errors or connectivity issues
        print(f"LLM Extraction/JSON Parsing Error AFTER CLEANUP: {e}")
        return None

# --- MAIN ORCHESTRATOR ---

def main():
    """Orchestrates the entire agent workflow: Gmail -> LLM -> Sheets -> Drive."""
    
    # Authenticate all services
    creds, sheets_service = authenticate_google_services()
    gmail_service = build('gmail', 'v1', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    all_files_to_upload = []
    latest_bl_number = None
    container_number = None

    # Step 1: Gather files and extract data from the latest email
    for filename, file_data in get_gmail_attachments(gmail_service):
        
        # Agent uses the LLM tool for reliable data extraction
        details = extract_shipping_details_llm_tool(file_data)
        
        if details is None:
            print("Skipping file: LLM failed to extract details.")
            continue
            
        # We only need the BL and CN from the FIRST (latest) email for naming/matching
        if latest_bl_number is None:
            latest_bl_number = details['BL_Number']
            container_number = details['Container_Number']
            
        # Store all files for the final upload stage
        all_files_to_upload.append({
            'filename': filename,
            'file_data': file_data
        })
    
    if not all_files_to_upload:
        print("Agent found no documents to process.")
        return

    # --- Step 2: SHEETS LOOKUP AND UPDATE ---
    
    if container_number and "NOT_FOUND" not in container_number:
        sheet_data = get_sheet_data(sheets_service)
        # Search the sheet for the container number (starting after header row)
        # We now search in column D, which is index 3 of the sheet_data row
        CONTAINER_COLUMN_INDEX = 3 # D is the 4th column (0-indexed)

        for i, row in enumerate(sheet_data):
            # i > 0 skips the header row
            # Ensure the row exists and has enough columns (at least 4)
                if i > 0 and len(row) > CONTAINER_COLUMN_INDEX and container_number in row[CONTAINER_COLUMN_INDEX]: 
                    # Found a match! Update the BL column (Column E)
                    update_sheet_cell(sheets_service, i, latest_bl_number, bl_column_letter='E')
                    print(f"Sheet updated for Container {container_number} in Column E.")
                    break        
                else:
                    print(f"WARNING: Container {container_number} not found in the Google Sheet for update.")    
    
    # --- Step 3: DRIVE UPLOAD ---
    
    # Create the folder based on the extracted BL number
    folder_name = f"Shipment Documents - {latest_bl_number}"
    folder_id = create_drive_folder(drive_service, folder_name)
    
    if "NOT_FOUND" in latest_bl_number:
         print("Agent WARNING: Could not reliably extract BL Number. Folder named generically.")

    # Loop through all saved files and upload them to the single folder
    for file_info in all_files_to_upload:
        upload_to_drive(
            drive_service, 
            folder_id, 
            file_info['filename'], 
            file_info['file_data']
        )
        
    print(f"Agent Success! Documents processed and saved to: {folder_name}")

if __name__ == '__main__':
    main()
