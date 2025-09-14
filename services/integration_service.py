"""
MetroMind Integration Service
External system integrations with per-user configuration
Supports email, SharePoint, WhatsApp, Google Drive and other external data sources
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator, Field
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Union
import asyncio
import json
import logging
import hashlib
import uuid
from enum import Enum
import httpx
import base64

# Email and messaging imports
try:
    import imaplib
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email import message_from_bytes
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

# Google Drive imports
try:
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import Flow
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    import pickle
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_AVAILABLE = False

try:
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.auth.authentication_context import AuthenticationContext
    SHAREPOINT_AVAILABLE = True
except ImportError:
    SHAREPOINT_AVAILABLE = False

# Import our models and config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, User, Integration, Document, AnalyticsRecord
from config import service_config, integration_config
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)

# Integration types and statuses
class IntegrationType(str, Enum):
    EMAIL_IMAP = "email_imap"
    EMAIL_SMTP = "email_smtp"
    SHAREPOINT = "sharepoint"
    WHATSAPP_BUSINESS = "whatsapp_business"
    GOOGLE_DRIVE = "google_drive"
    DROPBOX = "dropbox"
    WEBHOOK = "webhook"
    FTP = "ftp"
    REST_API = "rest_api"

class IntegrationStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    TESTING = "testing"
    PENDING_APPROVAL = "pending_approval"

class SyncDirection(str, Enum):
    INBOUND = "inbound"     # Import from external system
    OUTBOUND = "outbound"   # Export to external system
    BIDIRECTIONAL = "bidirectional"

# Pydantic models
class IntegrationConfig(BaseModel):
    """Base configuration for integrations"""
    name: str
    description: Optional[str] = None
    auto_sync: bool = True
    sync_interval_minutes: int = 30
    sync_direction: SyncDirection = SyncDirection.INBOUND
    enabled: bool = True
    
class EmailIMAPConfig(IntegrationConfig):
    """IMAP email integration configuration"""
    server: str = Field(..., description="IMAP server hostname")
    port: int = Field(993, description="IMAP server port")
    username: str = Field(..., description="Email username")
    password: str = Field(..., description="Email password")
    use_ssl: bool = True
    folder: str = "INBOX"
    mark_as_read: bool = True
    process_attachments: bool = True
    max_emails_per_sync: int = 50

class SharePointConfig(IntegrationConfig):
    """SharePoint integration configuration"""
    site_url: str = Field(..., description="SharePoint site URL")
    username: str = Field(..., description="SharePoint username")
    password: str = Field(..., description="SharePoint password")
    document_library: str = "Documents"
    folder_path: str = ""
    file_types: List[str] = ["pdf", "docx", "xlsx", "pptx"]
    include_metadata: bool = True

class GoogleDriveConfig(IntegrationConfig):
    """Google Drive integration configuration"""
    credentials_json: str = Field(..., description="Google OAuth2 credentials JSON")
    folder_id: Optional[str] = Field(None, description="Specific folder ID to monitor (None for root)")
    file_types: List[str] = ["pdf", "docx", "xlsx", "pptx", "txt", "jpg", "png"]
    include_shared_drives: bool = False
    max_files_per_sync: int = 100
    download_files: bool = True
    monitor_changes: bool = True

class WhatsAppConfig(IntegrationConfig):
    """WhatsApp Business API configuration"""
    phone_number_id: str = Field(..., description="WhatsApp phone number ID")
    access_token: str = Field(..., description="WhatsApp access token")
    webhook_verify_token: str = Field(..., description="Webhook verification token")
    webhook_url: str = Field(..., description="Webhook URL for receiving messages")

class WebhookConfig(IntegrationConfig):
    """Webhook integration configuration"""
    webhook_url: str = Field(..., description="Webhook URL")
    secret: Optional[str] = None
    headers: Dict[str, str] = {}
    method: str = "POST"
    content_type: str = "application/json"

class IntegrationCreateRequest(BaseModel):
    user_id: int
    integration_type: IntegrationType
    config: Dict[str, Any]
    is_global: bool = False  # If true, applies to all users
    
class IntegrationResponse(BaseModel):
    id: int
    user_id: Optional[int]
    integration_type: str
    name: str
    status: str
    last_sync: Optional[datetime]
    next_sync: Optional[datetime]
    sync_count: int
    error_count: int
    is_global: bool
    created_at: datetime

class SyncRequest(BaseModel):
    integration_id: int
    force_full_sync: bool = False

class SyncResult(BaseModel):
    success: bool
    items_processed: int
    items_imported: int
    items_failed: int
    duration_seconds: float
    errors: List[str] = []
    metadata: Dict[str, Any] = {}

# FastAPI app
app = FastAPI(
    title="MetroMind Integration Service",
    description="External system integrations with per-user configuration",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Integration processors
class BaseIntegrationProcessor:
    """Base class for integration processors"""
    
    def __init__(self, integration: Integration):
        self.integration = integration
        self.config = integration.config
        
    async def test_connection(self) -> Dict[str, Any]:
        """Test the integration connection"""
        raise NotImplementedError
        
    async def sync(self, force_full_sync: bool = False) -> SyncResult:
        """Perform synchronization"""
        raise NotImplementedError
        
    async def get_status(self) -> Dict[str, Any]:
        """Get integration status"""
        return {
            "status": self.integration.status,
            "last_sync": self.integration.last_sync,
            "next_sync": self.integration.next_sync,
            "sync_count": self.integration.sync_count,
            "error_count": self.integration.error_count
        }

class EmailIMAPProcessor(BaseIntegrationProcessor):
    """IMAP email integration processor"""
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test IMAP connection"""
        try:
            if not EMAIL_AVAILABLE:
                return {"success": False, "error": "Email libraries not available"}
            
            server = self.config.get("server")
            port = self.config.get("port", 993)
            username = self.config.get("username")
            password = self.config.get("password")
            use_ssl = self.config.get("use_ssl", True)
            
            if use_ssl:
                mail = imaplib.IMAP4_SSL(server, port)
            else:
                mail = imaplib.IMAP4(server, port)
            
            mail.login(username, password)
            mail.select(self.config.get("folder", "INBOX"))
            
            # Get message count
            status, messages = mail.search(None, "ALL")
            message_count = len(messages[0].split()) if messages[0] else 0
            
            mail.close()
            mail.logout()
            
            return {
                "success": True,
                "message_count": message_count,
                "server": server,
                "folder": self.config.get("folder", "INBOX")
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def sync(self, force_full_sync: bool = False) -> SyncResult:
        """Sync emails from IMAP server"""
        start_time = datetime.now()
        result = SyncResult(
            success=False,
            items_processed=0,
            items_imported=0,
            items_failed=0,
            duration_seconds=0.0
        )
        
        try:
            if not EMAIL_AVAILABLE:
                result.errors.append("Email libraries not available")
                return result
            
            server = self.config.get("server")
            port = self.config.get("port", 993)
            username = self.config.get("username")
            password = self.config.get("password")
            use_ssl = self.config.get("use_ssl", True)
            folder = self.config.get("folder", "INBOX")
            max_emails = self.config.get("max_emails_per_sync", 50)
            
            # Connect to IMAP server
            if use_ssl:
                mail = imaplib.IMAP4_SSL(server, port)
            else:
                mail = imaplib.IMAP4(server, port)
            
            mail.login(username, password)
            mail.select(folder)
            
            # Search for emails
            search_criteria = "ALL"
            if not force_full_sync and self.integration.last_sync:
                # Only get emails since last sync
                since_date = self.integration.last_sync.strftime("%d-%b-%Y")
                search_criteria = f'SINCE {since_date}'
            
            status, messages = mail.search(None, search_criteria)
            email_ids = messages[0].split()[-max_emails:]  # Get last N emails
            
            result.items_processed = len(email_ids)
            
            # Process emails
            all_document_ids = []
            for email_id in email_ids:
                try:
                    document_ids = await self._process_email(mail, email_id)
                    all_document_ids.extend(document_ids)
                    result.items_imported += len(document_ids)
                except Exception as e:
                    result.items_failed += 1
                    result.errors.append(f"Failed to process email {email_id}: {str(e)}")
            
            # Store document IDs in result metadata
            result.metadata['document_ids'] = all_document_ids
            
            mail.close()
            mail.logout()
            
            result.success = True
            
        except Exception as e:
            result.errors.append(f"IMAP sync failed: {str(e)}")
        
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result
    
    async def _process_email(self, mail: imaplib.IMAP4, email_id: bytes):
        """Process individual email and save attachments as documents"""
        try:
            # Fetch email
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            email_body = msg_data[0][1]
            email_message = message_from_bytes(email_body)
            
            # Extract email metadata
            subject = email_message["Subject"] or "No Subject"
            sender = email_message["From"]
            date_str = email_message["Date"]
            
            # Process attachments
            document_ids = []
            
            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_disposition() == 'attachment':
                        filename = part.get_filename()
                        if filename:
                            # Save attachment as document
                            doc_id = await self._save_attachment_as_document(
                                part, filename, subject, sender, date_str
                            )
                            if doc_id:
                                document_ids.append(doc_id)
            
            # Also create document for email content if no attachments
            if not document_ids:
                content = ""
                if email_message.is_multipart():
                    for part in email_message.walk():
                        if part.get_content_type() == "text/plain":
                            content += part.get_payload(decode=True).decode('utf-8', errors='ignore')
                else:
                    content = email_message.get_payload(decode=True).decode('utf-8', errors='ignore')
                
                if content.strip():
                    doc_id = await self._save_email_as_document(content, subject, sender, date_str)
                    if doc_id:
                        document_ids.append(doc_id)
            
            # Mark email as read if configured
            if self.config.get("mark_as_read", True):
                mail.store(email_id, '+FLAGS', '\\Seen')
                
            logger.info(f"Processed email: {subject} from {sender}, created {len(document_ids)} documents")
            return document_ids
            
        except Exception as e:
            logger.error(f"Error processing email {email_id}: {e}")
            return []
    
    async def _save_attachment_as_document(self, part, filename: str, subject: str, sender: str, date_str: str) -> Optional[str]:
        """Save email attachment as document"""
        try:
            import os
            import hashlib
            from pathlib import Path
            from database import get_db, Document, DocumentCategory, Priority
            
            # Get file content
            file_content = part.get_payload(decode=True)
            
            # Create unique filename
            file_hash = hashlib.sha256(file_content).hexdigest()
            file_ext = Path(filename).suffix
            unique_filename = f"{file_hash}{file_ext}"
            
            # Save file to disk
            upload_dir = Path("data/uploads/email")
            upload_dir.mkdir(parents=True, exist_ok=True)
            file_path = upload_dir / unique_filename
            
            with open(file_path, 'wb') as f:
                f.write(file_content)
            
            # Create document record
            db = next(get_db())
            
            document = Document(
                id=str(uuid.uuid4()),
                filename=unique_filename,
                original_filename=filename,
                file_path=str(file_path),
                file_size=len(file_content),
                mime_type=part.get_content_type() or "application/octet-stream",
                file_hash=file_hash,
                title=f"Email Attachment: {filename}",
                description=f"From email: {subject}\nSender: {sender}\nDate: {date_str}",
                category=DocumentCategory.GENERAL,
                priority=Priority.MEDIUM,
                uploaded_by="email_integration",
                created_at=datetime.now(timezone.utc),
                status="PROCESSING"
            )
            
            db.add(document)
            db.commit()
            db.refresh(document)
            
            document_id = document.id
            db.close()
            
            return document_id
            
        except Exception as e:
            logger.error(f"Error saving attachment {filename}: {e}")
            return None
    
    async def _save_email_as_document(self, content: str, subject: str, sender: str, date_str: str) -> Optional[str]:
        """Save email content as text document"""
        try:
            import hashlib
            from pathlib import Path
            from database import get_db, Document, DocumentCategory, Priority
            
            # Create text file
            text_content = f"Subject: {subject}\nFrom: {sender}\nDate: {date_str}\n\n{content}"
            content_bytes = text_content.encode('utf-8')
            file_hash = hashlib.sha256(content_bytes).hexdigest()
            filename = f"email_{file_hash}.txt"
            
            # Save file to disk
            upload_dir = Path("data/uploads/email")
            upload_dir.mkdir(parents=True, exist_ok=True)
            file_path = upload_dir / filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(text_content)
            
            # Create document record
            db = next(get_db())
            
            document = Document(
                id=str(uuid.uuid4()),
                filename=filename,
                original_filename=f"Email: {subject}",
                file_path=str(file_path),
                file_size=len(content_bytes),
                mime_type="text/plain",
                file_hash=file_hash,
                extracted_text=content,
                title=f"Email: {subject}",
                description=f"Sender: {sender}\nDate: {date_str}",
                category=DocumentCategory.GENERAL,
                priority=Priority.MEDIUM,
                uploaded_by="email_integration",
                created_at=datetime.now(timezone.utc),
                status="PROCESSING"
            )
            
            db.add(document)
            db.commit()
            db.refresh(document)
            
            document_id = document.id
            db.close()
            
            return document_id
            
        except Exception as e:
            logger.error(f"Error saving email content: {e}")
            return None

class GoogleDriveProcessor(BaseIntegrationProcessor):
    """Google Drive integration processor"""
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Google Drive connection"""
        try:
            if not GOOGLE_DRIVE_AVAILABLE:
                return {"status": "error", "message": "Google Drive API not available"}
            
            service = await self._get_drive_service()
            if not service:
                return {"status": "error", "message": "Failed to authenticate with Google Drive"}
            
            # Test by getting user info
            about = service.about().get(fields="user").execute()
            return {
                "status": "success", 
                "message": f"Connected to Google Drive as {about['user']['emailAddress']}"
            }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
    
    async def sync(self) -> Dict[str, Any]:
        """Sync files from Google Drive"""
        try:
            service = await self._get_drive_service()
            if not service:
                return {"status": "error", "message": "Failed to authenticate with Google Drive"}
            
            folder_id = self.config.get("folder_id")
            file_types = self.config.get("file_types", ["pdf", "docx", "xlsx", "pptx"])
            max_files = self.config.get("max_files_per_sync", 100)
            download_files = self.config.get("download_files", True)
            
            # Build query
            query = "trashed=false"
            if folder_id:
                query += f" and '{folder_id}' in parents"
            
            # Add file type filter
            if file_types:
                type_queries = []
                for file_type in file_types:
                    type_queries.append(f"name contains '.{file_type}'")
                query += f" and ({' or '.join(type_queries)})"
            
            # Get files
            results = service.files().list(
                q=query,
                pageSize=max_files,
                fields="files(id,name,mimeType,modifiedTime,size,webViewLink,parents)"
            ).execute()
            
            files = results.get('files', [])
            document_ids = []
            
            for file_info in files:
                try:
                    doc_id = await self._process_drive_file(service, file_info, download_files)
                    if doc_id:
                        document_ids.append(doc_id)
                except Exception as e:
                    logger.error(f"Error processing file {file_info['name']}: {e}")
                    continue
            
            return {
                "status": "success",
                "message": f"Processed {len(document_ids)} files from Google Drive",
                "files_processed": len(document_ids),
                "document_ids": document_ids
            }
            
        except Exception as e:
            logger.error(f"Google Drive sync error: {e}")
            return {"status": "error", "message": str(e)}
    
    async def _get_drive_service(self):
        """Get authenticated Google Drive service"""
        try:
            credentials_json = self.config.get("credentials_json")
            if not credentials_json:
                return None
            
            # Parse credentials
            import json
            creds_data = json.loads(credentials_json)
            
            # Create credentials object
            creds = Credentials.from_authorized_user_info(creds_data)
            
            # Build service
            service = build('drive', 'v3', credentials=creds)
            return service
            
        except Exception as e:
            logger.error(f"Error creating Drive service: {e}")
            return None
    
    async def _process_drive_file(self, service, file_info: dict, download_files: bool = True) -> Optional[str]:
        """Process a file from Google Drive"""
        try:
            import os
            import hashlib
            from pathlib import Path
            from database import get_db, Document, DocumentCategory, Priority
            
            file_id = file_info['id']
            filename = file_info['name']
            mime_type = file_info['mimeType']
            modified_time = file_info['modifiedTime']
            web_link = file_info['webViewLink']
            
            # Check if we already have this file
            with next(get_db()) as db:
                existing_doc = db.query(Document).filter(
                    Document.source_id == file_id,
                    Document.source_type == "google_drive"
                ).first()
                
                if existing_doc:
                    logger.info(f"File {filename} already exists in database")
                    return str(existing_doc.id)
            
            file_content = None
            file_path = None
            
            if download_files:
                # Download file content
                file_content = service.files().get_media(fileId=file_id).execute()
                
                # Save to disk
                file_hash = hashlib.sha256(file_content).hexdigest()
                file_ext = Path(filename).suffix
                unique_filename = f"{file_hash}{file_ext}"
                
                upload_dir = Path("data/uploads/google_drive")
                upload_dir.mkdir(parents=True, exist_ok=True)
                file_path = upload_dir / unique_filename
                
                with open(file_path, 'wb') as f:
                    f.write(file_content)
            
            # Create document record
            with next(get_db()) as db:
                document = Document(
                    title=filename,
                    file_path=str(file_path) if file_path else None,
                    file_size=len(file_content) if file_content else file_info.get('size', 0),
                    mime_type=mime_type,
                    source_type="google_drive",
                    source_id=file_id,
                    source_url=web_link,
                    category=DocumentCategory.INCOMING,
                    priority=Priority.MEDIUM,
                    user_id=self.integration.user_id,
                    status="pending_processing",
                    metadata_={
                        "drive_file_id": file_id,
                        "modified_time": modified_time,
                        "web_link": web_link,
                        "integration_id": self.integration.id
                    }
                )
                
                db.add(document)
                db.commit()
                db.refresh(document)
                
                # Trigger AI processing
                if file_content:
                    await self._trigger_ai_processing(str(document.id), file_content)
                
                logger.info(f"Created document {document.id} for Drive file: {filename}")
                return str(document.id)
                
        except Exception as e:
            logger.error(f"Error processing Drive file {file_info.get('name', 'unknown')}: {e}")
            return None
    
    async def _trigger_ai_processing(self, document_id: str, file_content: bytes):
        """Trigger AI processing for the document"""
        try:
            # Call AI service to process document
            async with httpx.AsyncClient() as client:
                files = {"file": ("document", file_content)}
                response = await client.post(
                    f"http://localhost:{service_config.ai_ml_service_port}/ai/process-document/{document_id}",
                    files=files,
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    logger.info(f"AI processing triggered for document {document_id}")
                else:
                    logger.warning(f"AI processing failed for document {document_id}: {response.text}")
                    
        except Exception as e:
            logger.error(f"Error triggering AI processing: {e}")
    
    def get_auth_url(self) -> str:
        """Generate OAuth2 authorization URL"""
        if not GOOGLE_DRIVE_AVAILABLE:
            raise Exception("Google Drive API not available")
            
        from google_auth_oauthlib.flow import Flow
        
        if not os.path.exists(self.config.credentials_file):
            raise Exception("Google Drive credentials file not found")
            
        flow = Flow.from_client_secrets_file(
            self.config.credentials_file,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        flow.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'  # For desktop apps
        
        auth_url, _ = flow.authorization_url(prompt='consent')
        return auth_url
    
    def exchange_code_for_tokens(self, code: str) -> bool:
        """Exchange authorization code for access tokens"""
        if not GOOGLE_DRIVE_AVAILABLE:
            raise Exception("Google Drive API not available")
            
        from google_auth_oauthlib.flow import Flow
        
        try:
            flow = Flow.from_client_secrets_file(
                self.config.credentials_file,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            flow.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'
            
            flow.fetch_token(code=code)
            
            # Save credentials
            with open(self.config.token_file, 'w') as token_file:
                token_file.write(flow.credentials.to_json())
            
            return True
        except Exception as e:
            logger.error(f"Failed to exchange code for tokens: {str(e)}")
            return False


class SharePointProcessor(BaseIntegrationProcessor):
    """SharePoint integration processor"""
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test SharePoint connection"""
        try:
            if not SHAREPOINT_AVAILABLE:
                return {"success": False, "error": "SharePoint libraries not available"}
            
            site_url = self.config.get("site_url")
            username = self.config.get("username")
            password = self.config.get("password")
            
            # Create authentication context
            auth_ctx = AuthenticationContext(site_url)
            auth_ctx.acquire_token_for_user(username, password)
            
            # Create client context
            ctx = ClientContext(site_url, auth_ctx)
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            
            return {
                "success": True,
                "site_title": web.properties.get("Title", ""),
                "site_url": site_url
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def sync(self, force_full_sync: bool = False) -> SyncResult:
        """Sync documents from SharePoint"""
        start_time = datetime.now()
        result = SyncResult(
            success=False,
            items_processed=0,
            items_imported=0,
            items_failed=0,
            duration_seconds=0.0
        )
        
        try:
            if not SHAREPOINT_AVAILABLE:
                result.errors.append("SharePoint libraries not available")
                return result
            
            site_url = self.config.get("site_url")
            username = self.config.get("username")
            password = self.config.get("password")
            document_library = self.config.get("document_library", "Documents")
            folder_path = self.config.get("folder_path", "")
            file_types = self.config.get("file_types", ["pdf", "docx", "xlsx"])
            
            # Connect to SharePoint
            auth_ctx = AuthenticationContext(site_url)
            auth_ctx.acquire_token_for_user(username, password)
            ctx = ClientContext(site_url, auth_ctx)
            
            # Get document library
            lists = ctx.web.lists
            doc_lib = lists.get_by_title(document_library)
            
            # Query for files
            folder = doc_lib.root_folder
            if folder_path:
                folder = doc_lib.root_folder.folders.get_by_path(folder_path)
            
            files = folder.files
            ctx.load(files)
            ctx.execute_query()
            
            result.items_processed = len(files)
            
            # Process files
            for file in files:
                try:
                    file_ext = file.properties.get("Name", "").split(".")[-1].lower()
                    if file_ext in file_types:
                        await self._process_sharepoint_file(ctx, file)
                        result.items_imported += 1
                except Exception as e:
                    result.items_failed += 1
                    result.errors.append(f"Failed to process file {file.properties.get('Name')}: {str(e)}")
            
            result.success = True
            
        except Exception as e:
            result.errors.append(f"SharePoint sync failed: {str(e)}")
        
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result
    
    async def _process_sharepoint_file(self, ctx, file):
        """Process individual SharePoint file"""
        file_name = file.properties.get("Name", "")
        file_size = file.properties.get("Length", 0)
        modified_date = file.properties.get("TimeLastModified")
        
        # Download file content (simplified)
        logger.info(f"Processed SharePoint file: {file_name} ({file_size} bytes)")

class WhatsAppProcessor(BaseIntegrationProcessor):
    """WhatsApp Business API processor"""
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test WhatsApp API connection"""
        try:
            phone_number_id = self.config.get("phone_number_id")
            access_token = self.config.get("access_token")
            
            # Test API call to get phone number info
            url = f"https://graph.facebook.com/v18.0/{phone_number_id}"
            headers = {"Authorization": f"Bearer {access_token}"}
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "success": True,
                        "phone_number": data.get("display_phone_number"),
                        "verified_name": data.get("verified_name")
                    }
                else:
                    return {"success": False, "error": f"API error: {response.status_code}"}
                    
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def sync(self, force_full_sync: bool = False) -> SyncResult:
        """Sync WhatsApp messages (webhook-based, so mainly status check)"""
        return SyncResult(
            success=True,
            items_processed=0,
            items_imported=0,
            items_failed=0,
            duration_seconds=0.1,
            metadata={"note": "WhatsApp uses webhook for real-time message processing"}
        )

class WebhookProcessor(BaseIntegrationProcessor):
    """Webhook integration processor"""
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test webhook endpoint"""
        try:
            webhook_url = self.config.get("webhook_url")
            method = self.config.get("method", "POST")
            headers = self.config.get("headers", {})
            
            test_payload = {"test": True, "timestamp": datetime.now().isoformat()}
            
            async with httpx.AsyncClient() as client:
                if method.upper() == "POST":
                    response = await client.post(webhook_url, json=test_payload, headers=headers)
                elif method.upper() == "GET":
                    response = await client.get(webhook_url, headers=headers)
                else:
                    return {"success": False, "error": f"Unsupported method: {method}"}
                
                return {
                    "success": response.status_code < 400,
                    "status_code": response.status_code,
                    "response_time_ms": response.elapsed.total_seconds() * 1000
                }
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def sync(self, force_full_sync: bool = False) -> SyncResult:
        """Webhook sync (mainly for testing)"""
        test_result = await self.test_connection()
        
        return SyncResult(
            success=test_result["success"],
            items_processed=1,
            items_imported=1 if test_result["success"] else 0,
            items_failed=0 if test_result["success"] else 1,
            duration_seconds=test_result.get("response_time_ms", 0) / 1000,
            errors=[] if test_result["success"] else [test_result.get("error", "Unknown error")]
        )

# Integration manager
class IntegrationManager:
    """Manages integration processors"""
    
    def __init__(self):
        self.processors = {
            IntegrationType.EMAIL_IMAP: EmailIMAPProcessor,
            IntegrationType.GOOGLE_DRIVE: GoogleDriveProcessor,
            IntegrationType.SHAREPOINT: SharePointProcessor,
            IntegrationType.WHATSAPP_BUSINESS: WhatsAppProcessor,
            IntegrationType.WEBHOOK: WebhookProcessor,
        }
    
    def get_processor(self, integration: Integration) -> BaseIntegrationProcessor:
        """Get processor for integration type"""
        processor_class = self.processors.get(integration.integration_type)
        if not processor_class:
            raise ValueError(f"No processor available for integration type: {integration.integration_type}")
        
        return processor_class(integration)
    
    async def test_integration(self, integration: Integration) -> Dict[str, Any]:
        """Test an integration"""
        processor = self.get_processor(integration)
        return await processor.test_connection()
    
    async def sync_integration(self, integration: Integration, force_full_sync: bool = False) -> SyncResult:
        """Sync an integration"""
        processor = self.get_processor(integration)
        return await processor.sync(force_full_sync)

# Global integration manager
integration_manager = IntegrationManager()

# Background sync scheduler
class SyncScheduler:
    """Schedules and manages background synchronization"""
    
    def __init__(self):
        self.running = False
        self.active_syncs = set()
    
    async def start(self):
        """Start the sync scheduler"""
        self.running = True
        asyncio.create_task(self._run_scheduler())
        logger.info("Sync scheduler started")
    
    async def stop(self):
        """Stop the sync scheduler"""
        self.running = False
        logger.info("Sync scheduler stopped")
    
    async def _run_scheduler(self):
        """Main scheduler loop"""
        while self.running:
            try:
                await self._check_and_sync()
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)
    
    async def _check_and_sync(self):
        """Check for integrations that need syncing"""
        try:
            # Import the database session here to avoid circular imports
            from database import get_db
            db = next(get_db())
            
            # Get active integrations that need syncing
            now = datetime.now(timezone.utc)
            
            integrations_to_sync = db.query(Integration).filter(
                Integration.status == IntegrationStatus.ACTIVE,
                Integration.auto_sync == True,
                (Integration.next_sync.is_(None)) | (Integration.next_sync <= now)
            ).all()
            
            for integration in integrations_to_sync:
                if integration.id not in self.active_syncs:
                    # Start background sync
                    asyncio.create_task(self.sync_integration_async(integration.id))
                    
            db.close()
            
        except Exception as e:
            logger.error(f"Error checking integrations for sync: {e}")
    
    async def sync_integration_async(self, integration_id: int):
        """Sync integration in background"""
        if integration_id in self.active_syncs:
            logger.warning(f"Integration {integration_id} is already syncing")
            return
        
        self.active_syncs.add(integration_id)
        
        try:
            from database import get_db
            db = next(get_db())
            
            # Get integration from database
            integration = db.query(Integration).filter(Integration.id == integration_id).first()
            if not integration:
                logger.error(f"Integration {integration_id} not found")
                return
            
            logger.info(f"Background sync started for integration {integration_id} ({integration.integration_type})")
            
            # Get the appropriate processor
            processor = integration_manager.get_processor(integration)
            if not processor:
                logger.error(f"No processor found for integration type {integration.integration_type}")
                return
            
            # Perform the sync
            sync_result = await processor.sync()
            
            # Update integration with sync results
            integration.last_sync = datetime.now(timezone.utc)
            integration.sync_count += 1
            
            if sync_result.success:
                # Calculate next sync time
                sync_interval = integration.config.get('sync_interval_minutes', 30)
                integration.next_sync = datetime.now(timezone.utc) + timedelta(minutes=sync_interval)
                integration.status = IntegrationStatus.ACTIVE
                integration.last_error = None
                
                logger.info(f"Sync completed for integration {integration_id}: {sync_result.items_processed} items processed, {sync_result.items_imported} imported")
                
                # Process imported documents with AI/ML
                if sync_result.items_imported > 0:
                    await self._process_imported_documents(integration, sync_result)
                    
            else:
                integration.error_count += 1
                integration.last_error = "; ".join(sync_result.errors)
                logger.error(f"Sync failed for integration {integration_id}: {sync_result.errors}")
            
            db.commit()
            db.close()
            
        except Exception as e:
            logger.error(f"Background sync failed for integration {integration_id}: {e}")
        
        finally:
            self.active_syncs.discard(integration_id)
    
    async def _process_imported_documents(self, integration: Integration, sync_result: SyncResult):
        """Process newly imported documents with AI/ML services"""
        try:
            import httpx
            
            # Get document IDs from sync result metadata
            document_ids = sync_result.metadata.get('document_ids', [])
            
            for doc_id in document_ids:
                try:
                    # Call AI/ML service for document processing
                    async with httpx.AsyncClient() as client:
                        ai_response = await client.post(
                            f"http://localhost:8004/process_document",
                            json={"document_id": doc_id},
                            timeout=30
                        )
                        
                        if ai_response.status_code == 200:
                            logger.info(f"AI processing completed for document {doc_id}")
                        else:
                            logger.warning(f"AI processing failed for document {doc_id}")
                    
                    # Call notification service to notify users
                    async with httpx.AsyncClient() as client:
                        notification_response = await client.post(
                            f"http://localhost:8006/auto_notify_document",
                            json={"document_id": doc_id, "source": integration.integration_type},
                            timeout=10
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing document {doc_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error in post-import processing: {e}")

# Global sync scheduler
sync_scheduler = SyncScheduler()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    await sync_scheduler.start()
    logger.info("Integration service started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await sync_scheduler.stop()

# API Endpoints

@app.get("/integrations/google-drive/auth-url")
async def get_google_drive_auth_url():
    """Get Google Drive OAuth2 authorization URL"""
    try:
        # Create a temporary config to get auth URL
        temp_config = GoogleDriveConfig(
            credentials_file=integration_config.google_drive_credentials_file,
            token_file=integration_config.google_drive_token_file,
            folder_ids=[]
        )
        temp_processor = GoogleDriveProcessor(temp_config)
        auth_url = temp_processor.get_auth_url()
        return {"auth_url": auth_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate auth URL: {str(e)}")

@app.post("/integrations/google-drive/exchange-code")
async def exchange_google_drive_code(
    code: str = Body(..., embed=True)
):
    """Exchange authorization code for tokens"""
    try:
        # Create a temporary config to exchange code
        temp_config = GoogleDriveConfig(
            credentials_file=integration_config.google_drive_credentials_file,
            token_file=integration_config.google_drive_token_file,
            folder_ids=[]
        )
        temp_processor = GoogleDriveProcessor(temp_config)
        success = temp_processor.exchange_code_for_tokens(code)
        if success:
            return {"success": True, "message": "Authorization successful"}
        else:
            raise HTTPException(status_code=400, detail="Failed to exchange code for tokens")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to exchange code: {str(e)}")

@app.post("/setup-automation")
async def setup_automation(
    email_config: Optional[Dict[str, Any]] = None,
    whatsapp_config: Optional[Dict[str, Any]] = None,
    db: Session = Depends(get_db)
):
    """Set up default automation integrations"""
    try:
        created_integrations = []
        
        # Setup email integration if config provided
        if email_config:
            email_integration = Integration(
                user_id=None,  # Global integration
                integration_type=IntegrationType.EMAIL_IMAP,
                name="Email Automation",
                config=await encrypt_config(email_config),
                status=IntegrationStatus.ACTIVE,
                is_global=True,
                auto_sync=True,
                sync_interval_minutes=5,  # Check every 5 minutes
                created_at=datetime.now(timezone.utc),
                next_sync=datetime.now(timezone.utc) + timedelta(minutes=5)
            )
            
            db.add(email_integration)
            created_integrations.append("email")
        
        # Setup WhatsApp integration if config provided
        if whatsapp_config:
            whatsapp_integration = Integration(
                user_id=None,  # Global integration
                integration_type=IntegrationType.WHATSAPP_BUSINESS,
                name="WhatsApp Automation",
                config=await encrypt_config(whatsapp_config),
                status=IntegrationStatus.ACTIVE,
                is_global=True,
                auto_sync=True,
                sync_interval_minutes=2,  # Check every 2 minutes
                created_at=datetime.now(timezone.utc),
                next_sync=datetime.now(timezone.utc) + timedelta(minutes=2)
            )
            
            db.add(whatsapp_integration)
            created_integrations.append("whatsapp")
        
        db.commit()
        
        return {
            "success": True,
            "message": f"Automation setup completed for: {', '.join(created_integrations)}",
            "integrations": created_integrations
        }
        
    except Exception as e:
        logger.error(f"Failed to setup automation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/integrations", response_model=IntegrationResponse)
async def create_integration(
    request: IntegrationCreateRequest,
    db: Session = Depends(get_db)
):
    """Create a new integration"""
    try:
        # Encrypt sensitive config data
        encrypted_config = await encrypt_config(request.config)
        
        # Create integration
        integration = Integration(
            user_id=request.user_id if not request.is_global else None,
            integration_type=request.integration_type,
            name=request.config.get("name", f"{request.integration_type}_integration"),
            config=encrypted_config,
            status=IntegrationStatus.TESTING,
            is_global=request.is_global,
            created_at=datetime.now(timezone.utc)
        )
        
        db.add(integration)
        db.commit()
        db.refresh(integration)
        
        # Test the integration
        try:
            test_result = await integration_manager.test_integration(integration)
            if test_result.get("success"):
                integration.status = IntegrationStatus.ACTIVE
            else:
                integration.status = IntegrationStatus.ERROR
                integration.last_error = test_result.get("error", "Unknown error")
            
            db.commit()
            
        except Exception as e:
            integration.status = IntegrationStatus.ERROR
            integration.last_error = str(e)
            db.commit()
        
        return IntegrationResponse(
            id=integration.id,
            user_id=integration.user_id,
            integration_type=integration.integration_type,
            name=integration.name,
            status=integration.status,
            last_sync=integration.last_sync,
            next_sync=integration.next_sync,
            sync_count=integration.sync_count,
            error_count=integration.error_count,
            is_global=integration.is_global,
            created_at=integration.created_at
        )
        
    except Exception as e:
        logger.error(f"Failed to create integration: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/integrations")
async def list_integrations(
    user_id: Optional[int] = None,
    integration_type: Optional[IntegrationType] = None,
    status: Optional[IntegrationStatus] = None,
    db: Session = Depends(get_db)
):
    """List integrations with optional filters"""
    try:
        query = db.query(Integration)
        
        if user_id is not None:
            # Get user-specific and global integrations
            query = query.filter(
                (Integration.user_id == user_id) | (Integration.is_global == True)
            )
        
        if integration_type:
            query = query.filter(Integration.integration_type == integration_type)
        
        if status:
            query = query.filter(Integration.status == status)
        
        integrations = query.all()
        
        return {
            "integrations": [
                IntegrationResponse(
                    id=i.id,
                    user_id=i.user_id,
                    integration_type=i.integration_type,
                    name=i.name,
                    status=i.status,
                    last_sync=i.last_sync,
                    next_sync=i.next_sync,
                    sync_count=i.sync_count,
                    error_count=i.error_count,
                    is_global=i.is_global,
                    created_at=i.created_at
                ) for i in integrations
            ],
            "total": len(integrations)
        }
        
    except Exception as e:
        logger.error(f"Failed to list integrations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/integrations/{integration_id}")
async def get_integration(
    integration_id: int,
    db: Session = Depends(get_db)
):
    """Get specific integration details"""
    try:
        integration = db.query(Integration).filter(Integration.id == integration_id).first()
        if not integration:
            raise HTTPException(status_code=404, detail="Integration not found")
        
        # Decrypt config for display (remove sensitive fields)
        display_config = await decrypt_config_for_display(integration.config)
        
        return {
            "integration": IntegrationResponse(
                id=integration.id,
                user_id=integration.user_id,
                integration_type=integration.integration_type,
                name=integration.name,
                status=integration.status,
                last_sync=integration.last_sync,
                next_sync=integration.next_sync,
                sync_count=integration.sync_count,
                error_count=integration.error_count,
                is_global=integration.is_global,
                created_at=integration.created_at
            ),
            "config": display_config,
            "last_error": integration.last_error
        }
        
    except Exception as e:
        logger.error(f"Failed to get integration {integration_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/integrations/{integration_id}/test")
async def test_integration(
    integration_id: int,
    db: Session = Depends(get_db)
):
    """Test an integration connection"""
    try:
        integration = db.query(Integration).filter(Integration.id == integration_id).first()
        if not integration:
            raise HTTPException(status_code=404, detail="Integration not found")
        
        # Decrypt config
        integration.config = await decrypt_config(integration.config)
        
        # Test connection
        test_result = await integration_manager.test_integration(integration)
        
        # Update status based on test result
        if test_result.get("success"):
            integration.status = IntegrationStatus.ACTIVE
            integration.last_error = None
        else:
            integration.status = IntegrationStatus.ERROR
            integration.last_error = test_result.get("error", "Unknown error")
        
        integration.last_tested = datetime.now(timezone.utc)
        db.commit()
        
        return {
            "success": test_result.get("success", False),
            "result": test_result,
            "integration_status": integration.status
        }
        
    except Exception as e:
        logger.error(f"Failed to test integration {integration_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/integrations/{integration_id}/sync", response_model=SyncResult)
async def sync_integration(
    integration_id: int,
    request: SyncRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Trigger integration synchronization"""
    try:
        integration = db.query(Integration).filter(Integration.id == integration_id).first()
        if not integration:
            raise HTTPException(status_code=404, detail="Integration not found")
        
        if integration.status != IntegrationStatus.ACTIVE:
            raise HTTPException(
                status_code=400, 
                detail=f"Integration is not active (status: {integration.status})"
            )
        
        # Decrypt config
        integration.config = await decrypt_config(integration.config)
        
        # Perform sync
        sync_result = await integration_manager.sync_integration(
            integration, 
            request.force_full_sync
        )
        
        # Update integration metadata
        integration.last_sync = datetime.now(timezone.utc)
        integration.sync_count += 1
        
        if not sync_result.success:
            integration.error_count += 1
            integration.last_error = "; ".join(sync_result.errors)
        else:
            integration.last_error = None
        
        # Calculate next sync time
        if integration.auto_sync:
            sync_interval = integration.config.get("sync_interval_minutes", 30)
            integration.next_sync = datetime.now(timezone.utc) + timedelta(minutes=sync_interval)
        
        db.commit()
        
        return sync_result
        
    except Exception as e:
        logger.error(f"Failed to sync integration {integration_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/integrations/{integration_id}")
async def update_integration(
    integration_id: int,
    config: Dict[str, Any],
    db: Session = Depends(get_db)
):
    """Update integration configuration"""
    try:
        integration = db.query(Integration).filter(Integration.id == integration_id).first()
        if not integration:
            raise HTTPException(status_code=404, detail="Integration not found")
        
        # Encrypt new config
        encrypted_config = await encrypt_config(config)
        
        # Update integration
        integration.config = encrypted_config
        integration.updated_at = datetime.now(timezone.utc)
        integration.status = IntegrationStatus.TESTING  # Reset to testing status
        
        db.commit()
        
        return {"success": True, "message": "Integration updated successfully"}
        
    except Exception as e:
        logger.error(f"Failed to update integration {integration_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/integrations/{integration_id}")
async def delete_integration(
    integration_id: int,
    db: Session = Depends(get_db)
):
    """Delete an integration"""
    try:
        integration = db.query(Integration).filter(Integration.id == integration_id).first()
        if not integration:
            raise HTTPException(status_code=404, detail="Integration not found")
        
        db.delete(integration)
        db.commit()
        
        return {"success": True, "message": "Integration deleted successfully"}
        
    except Exception as e:
        logger.error(f"Failed to delete integration {integration_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Webhook endpoints
@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    request: Dict[str, Any],
    db: Session = Depends(get_db)
):
    """Handle WhatsApp webhook events"""
    try:
        # Process WhatsApp webhook
        logger.info(f"WhatsApp webhook received: {request}")
        
        # Extract message data and process
        if "messages" in request.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}):
            messages = request["entry"][0]["changes"][0]["value"]["messages"]
            
            for message in messages:
                # Process each message
                await process_whatsapp_message(message, db)
        
        return {"success": True}
        
    except Exception as e:
        logger.error(f"WhatsApp webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/webhooks/whatsapp")
async def whatsapp_webhook_verify(
    hub_mode: str,
    hub_verify_token: str,
    hub_challenge: str
):
    """Verify WhatsApp webhook"""
    # Verify webhook token (simplified)
    expected_token = "your_webhook_verify_token"  # Should come from config
    
    if hub_mode == "subscribe" and hub_verify_token == expected_token:
        return int(hub_challenge)
    else:
        raise HTTPException(status_code=403, detail="Forbidden")

async def process_whatsapp_message(message: Dict[str, Any], db: Session):
    """Process incoming WhatsApp message"""
    try:
        message_type = message.get("type")
        sender = message.get("from")
        
        if message_type == "text":
            text_content = message["text"]["body"]
            logger.info(f"WhatsApp text message from {sender}: {text_content}")
            
            # Create document or notification based on message content
            # Implementation would depend on business logic
            
        elif message_type == "document":
            document_info = message["document"]
            logger.info(f"WhatsApp document from {sender}: {document_info.get('filename')}")
            
            # Download and process document
            # Implementation would involve downloading file and creating document record
            
    except Exception as e:
        logger.error(f"Failed to process WhatsApp message: {e}")

# Utility functions
async def encrypt_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Encrypt sensitive configuration data"""
    # Simple encryption - in production, use proper encryption
    encrypted_config = config.copy()
    
    sensitive_fields = ["password", "access_token", "secret", "api_key"]
    for field in sensitive_fields:
        if field in encrypted_config:
            # Base64 encode as simple "encryption" - use proper encryption in production
            encrypted_config[field] = base64.b64encode(
                encrypted_config[field].encode()
            ).decode()
    
    return encrypted_config

async def decrypt_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Decrypt configuration data"""
    decrypted_config = config.copy()
    
    sensitive_fields = ["password", "access_token", "secret", "api_key"]
    for field in sensitive_fields:
        if field in decrypted_config:
            try:
                decrypted_config[field] = base64.b64decode(
                    decrypted_config[field].encode()
                ).decode()
            except Exception:
                # If decoding fails, assume it's already decrypted
                pass
    
    return decrypted_config

async def decrypt_config_for_display(config: Dict[str, Any]) -> Dict[str, Any]:
    """Decrypt config but mask sensitive fields for display"""
    display_config = config.copy()
    
    sensitive_fields = ["password", "access_token", "secret", "api_key"]
    for field in sensitive_fields:
        if field in display_config:
            display_config[field] = "***MASKED***"
    
    return display_config

@app.get("/")
async def root():
    """Service root endpoint"""
    return {
        "service": "MetroMind Integration Service",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "supported_integrations": [t.value for t in IntegrationType],
        "active_syncs": len(sync_scheduler.active_syncs)
    }

@app.get("/health")
async def health_check():
    """Service health check"""
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scheduler_running": sync_scheduler.running,
            "active_syncs": len(sync_scheduler.active_syncs),
            "capabilities": {
                "email_available": EMAIL_AVAILABLE,
                "sharepoint_available": SHAREPOINT_AVAILABLE
            }
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting Integration service on port {service_config.integration_service_port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=service_config.integration_service_port,
        log_level="info"
    )
