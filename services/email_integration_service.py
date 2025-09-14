"""
Email Integration Service
Automatically fetches documents from email attachments and processes them
"""

import asyncio
import imaplib
import email
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import ssl
from pathlib import Path
import json
import logging
from datetime import datetime, timezone
import uuid
import requests
import mimetypes
from typing import List, Dict, Any, Optional

# Import our models and config
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Document, User, Task, DocumentCategory, Priority
from config import service_config
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

class EmailIntegrationService:
    """Service for email integration and automated document processing"""
    
    def __init__(self):
        # Email configuration - these should be in environment variables
        self.email_server = os.getenv('EMAIL_SERVER', 'imap.gmail.com')
        self.email_port = int(os.getenv('EMAIL_PORT', '993'))
        self.email_username = os.getenv('EMAIL_USERNAME', 'metromind@kmrl.gov.in')
        self.email_password = os.getenv('EMAIL_PASSWORD', 'your_app_password')
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        
        # Service URLs
        self.document_service_url = "http://localhost:8003"
        self.ai_service_url = "http://localhost:8004"
        self.ocr_service_url = "http://localhost:8001"
        self.notification_service_url = "http://localhost:8006"
        
        # Directories
        self.temp_dir = Path("data/temp/email_attachments")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Email processing state
        self.processed_emails = set()
        self.last_check = None
        
    async def start_email_monitoring(self):
        """Start monitoring emails for attachments"""
        logger.info("Starting email monitoring service...")
        
        while True:
            try:
                await self.check_for_new_emails()
                # Check every 2 minutes
                await asyncio.sleep(120)
                
            except Exception as e:
                logger.error(f"Error in email monitoring: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def check_for_new_emails(self):
        """Check for new emails with attachments"""
        try:
            # Connect to email server
            mail = imaplib.IMAP4_SSL(self.email_server, self.email_port)
            mail.login(self.email_username, self.email_password)
            
            # Select inbox
            mail.select('inbox')
            
            # Search for unseen emails with attachments
            status, messages = mail.search(None, 'UNSEEN')
            
            if status == 'OK':
                email_ids = messages[0].split()
                
                for email_id in email_ids:
                    if email_id.decode() not in self.processed_emails:
                        await self.process_email(mail, email_id)
                        self.processed_emails.add(email_id.decode())
            
            mail.close()
            mail.logout()
            
        except Exception as e:
            logger.error(f"Error checking emails: {e}")
    
    async def process_email(self, mail: imaplib.IMAP4_SSL, email_id: bytes):
        """Process individual email and its attachments"""
        try:
            status, msg_data = mail.fetch(email_id, '(RFC822)')
            
            if status == 'OK':
                email_message = email.message_from_bytes(msg_data[0][1])
                
                # Extract email metadata
                sender = email_message['From']
                subject = email_message['Subject']
                date = email_message['Date']
                
                logger.info(f"Processing email from {sender}: {subject}")
                
                # Process attachments
                attachments = await self.extract_attachments(email_message)
                
                if attachments:
                    # Find or create user based on email
                    user = await self.get_or_create_user_from_email(sender)
                    
                    # Process each attachment
                    for attachment in attachments:
                        await self.process_attachment(attachment, user, email_message)
                        
                    # Send confirmation email
                    await self.send_confirmation_email(sender, subject, len(attachments))
                
        except Exception as e:
            logger.error(f"Error processing email {email_id}: {e}")
    
    async def extract_attachments(self, email_message) -> List[Dict[str, Any]]:
        """Extract attachments from email"""
        attachments = []
        
        for part in email_message.walk():
            if part.get_content_disposition() == 'attachment':
                filename = part.get_filename()
                if filename:
                    # Save attachment to temp directory
                    file_data = part.get_payload(decode=True)
                    
                    # Generate unique filename
                    unique_filename = f"{uuid.uuid4()}_{filename}"
                    file_path = self.temp_dir / unique_filename
                    
                    with open(file_path, 'wb') as f:
                        f.write(file_data)
                    
                    attachments.append({
                        'filename': filename,
                        'original_filename': filename,
                        'file_path': str(file_path),
                        'file_size': len(file_data),
                        'mime_type': part.get_content_type()
                    })
        
        return attachments
    
    async def get_or_create_user_from_email(self, email_address: str) -> Optional[str]:
        """Get existing user or create new one based on email"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            # Clean email address
            if '<' in email_address:
                email_address = email_address.split('<')[1].split('>')[0]
            
            # Try to find existing user
            user = db.query(User).filter(User.email == email_address).first()
            
            if user:
                db.close()
                return str(user.id)
            
            # Create new user with email
            # Extract name from email if possible
            name_part = email_address.split('@')[0].replace('.', ' ').replace('_', ' ')
            names = name_part.split()
            
            first_name = names[0].title() if names else "Email"
            last_name = names[1].title() if len(names) > 1 else "User"
            
            new_user = User(
                id=str(uuid.uuid4()),
                username=email_address.split('@')[0],
                email=email_address,
                first_name=first_name,
                last_name=last_name,
                department="Email Submissions",
                role="EMPLOYEE",
                status="ACTIVE",
                created_at=datetime.now(timezone.utc)
            )
            
            # Set default password
            new_user.set_password("TempPass123!")
            
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            
            user_id = str(new_user.id)
            db.close()
            
            logger.info(f"Created new user for email: {email_address}")
            return user_id
            
        except Exception as e:
            logger.error(f"Error creating user from email {email_address}: {e}")
            return None
    
    async def process_attachment(self, attachment: Dict[str, Any], user_id: str, email_message):
        """Process attachment through document pipeline"""
        try:
            file_path = attachment['file_path']
            
            # Extract content based on file type
            content = await self.extract_content_from_file(file_path, attachment['mime_type'])
            
            # Analyze with AI
            ai_analysis = await self.analyze_with_ai(content, attachment)
            
            # Create document record
            document = await self.create_document_record(attachment, content, ai_analysis, user_id, email_message)
            
            # Auto-assign and create tasks
            assignments = await self.auto_assign_document(document, ai_analysis)
            tasks = await self.create_tasks(document, assignments)
            
            # Send notifications
            await self.send_notifications(document, assignments, tasks)
            
            logger.info(f"Successfully processed email attachment: {attachment['filename']}")
            
        except Exception as e:
            logger.error(f"Error processing attachment {attachment['filename']}: {e}")
    
    async def extract_content_from_file(self, file_path: str, mime_type: str) -> str:
        """Extract content using appropriate service"""
        try:
            if mime_type.startswith('image/'):
                # Use OCR service
                with open(file_path, 'rb') as f:
                    files = {'file': f}
                    response = requests.post(f"{self.ocr_service_url}/extract", files=files, timeout=30)
                
                if response.status_code == 200:
                    return response.json().get('extracted_text', '')
            
            elif mime_type in ['application/pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document']:
                # Use document service
                with open(file_path, 'rb') as f:
                    files = {'file': f}
                    response = requests.post(f"{self.document_service_url}/extract", files=files, timeout=30)
                
                if response.status_code == 200:
                    return response.json().get('content', '')
            
            elif mime_type.startswith('text/'):
                # Read text directly
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            return ""
            
        except Exception as e:
            logger.error(f"Error extracting content from {file_path}: {e}")
            return ""
    
    async def analyze_with_ai(self, content: str, attachment: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze content with AI service"""
        try:
            if not content.strip():
                return self._get_fallback_analysis(content)
            
            payload = {
                "text": content,
                "filename": attachment['filename'],
                "analyze_all": True
            }
            
            response = requests.post(f"{self.ai_service_url}/process", json=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            
            return self._get_fallback_analysis(content)
            
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._get_fallback_analysis(content)
    
    def _get_fallback_analysis(self, content: str) -> Dict[str, Any]:
        """Fallback analysis when AI service unavailable"""
        content_lower = content.lower()
        
        if any(word in content_lower for word in ['emergency', 'urgent', 'critical', 'safety']):
            return {"category": "SAFETY", "priority": "HIGH", "summary": content[:200], "entities": {}}
        elif any(word in content_lower for word in ['budget', 'payment', 'invoice', 'finance']):
            return {"category": "FINANCE", "priority": "MEDIUM", "summary": content[:200], "entities": {}}
        elif any(word in content_lower for word in ['maintenance', 'repair', 'equipment']):
            return {"category": "MAINTENANCE", "priority": "MEDIUM", "summary": content[:200], "entities": {}}
        else:
            return {"category": "GENERAL", "priority": "LOW", "summary": content[:200], "entities": {}}
    
    async def create_document_record(self, attachment: Dict[str, Any], content: str, 
                                   ai_analysis: Dict[str, Any], user_id: str, email_message) -> Document:
        """Create document record in database"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            # Map strings to enums
            category_mapping = {
                "SAFETY": DocumentCategory.SAFETY,
                "FINANCE": DocumentCategory.FINANCE,
                "MAINTENANCE": DocumentCategory.MAINTENANCE,
                "OPERATIONS": DocumentCategory.OPERATIONS,
                "GENERAL": DocumentCategory.OTHER
            }
            
            priority_mapping = {
                "HIGH": Priority.HIGH,
                "MEDIUM": Priority.MEDIUM,
                "LOW": Priority.LOW
            }
            
            category = category_mapping.get(ai_analysis.get("category", "GENERAL"), DocumentCategory.OTHER)
            priority = priority_mapping.get(ai_analysis.get("priority", "MEDIUM"), Priority.MEDIUM)
            
            # Calculate file hash
            import hashlib
            with open(attachment['file_path'], 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            
            document = Document(
                id=str(uuid.uuid4()),
                filename=attachment['filename'],
                original_filename=attachment['original_filename'],
                file_path=attachment['file_path'],
                file_size=attachment['file_size'],
                mime_type=attachment['mime_type'],
                file_hash=file_hash,
                extracted_text=content,
                summary=ai_analysis.get('summary', ''),
                category=category,
                priority=priority,
                key_entities=ai_analysis.get('entities', {}),
                sentiment_score=ai_analysis.get('sentiment', 0.0),
                language_detected=ai_analysis.get('language', 'en'),
                uploaded_by=user_id,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                # Add email metadata
                d_metadata={
                    'source': 'email',
                    'email_subject': email_message.get('Subject', ''),
                    'email_from': email_message.get('From', ''),
                    'email_date': email_message.get('Date', '')
                }
            )
            
            db.add(document)
            db.commit()
            db.refresh(document)
            db.close()
            
            return document
            
        except Exception as e:
            logger.error(f"Error creating document record: {e}")
            raise
    
    async def auto_assign_document(self, document: Document, ai_analysis: Dict[str, Any]) -> List[str]:
        """Auto-assign document to appropriate users"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            # Assignment rules
            assignment_rules = {
                DocumentCategory.SAFETY: ["STATION_CONTROLLER", "ADMIN"],
                DocumentCategory.FINANCE: ["FINANCE_MANAGER", "ADMIN"],
                DocumentCategory.MAINTENANCE: ["MAINTENANCE_HEAD", "ADMIN"],
                DocumentCategory.OPERATIONS: ["STATION_CONTROLLER", "ADMIN"],
                DocumentCategory.OTHER: ["ADMIN"]
            }
            
            target_roles = assignment_rules.get(document.category, ["ADMIN"])
            
            # Find users with target roles
            assigned_users = []
            for role in target_roles:
                users = db.query(User).filter(User.role == role).all()
                assigned_users.extend([str(user.id) for user in users])
            
            # Remove duplicates
            assigned_users = list(set(assigned_users))
            
            db.close()
            return assigned_users
            
        except Exception as e:
            logger.error(f"Error auto-assigning document: {e}")
            return []
    
    async def create_tasks(self, document: Document, assignments: List[str]) -> List[str]:
        """Create tasks for assigned users"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            tasks_created = []
            
            task_templates = {
                DocumentCategory.SAFETY: "Review safety document received via email",
                DocumentCategory.FINANCE: "Review financial document received via email",
                DocumentCategory.MAINTENANCE: "Review maintenance document received via email",
                DocumentCategory.OPERATIONS: "Review operational document received via email",
                DocumentCategory.OTHER: "Review document received via email"
            }
            
            template = task_templates.get(document.category, "Review document")
            
            for user_id in assignments:
                task = Task(
                    id=str(uuid.uuid4()),
                    title=f"{template} - {document.filename}",
                    description=f"Document received via email\n\nSummary: {document.summary}",
                    document_id=str(document.id),
                    assigned_to=user_id,
                    priority=document.priority,
                    status="PENDING",
                    category=document.category.value,
                    created_at=datetime.now(timezone.utc),
                    metadata={
                        'source': 'email_automation',
                        'auto_generated': True
                    }
                )
                
                db.add(task)
                tasks_created.append(str(task.id))
            
            db.commit()
            db.close()
            
            return tasks_created
            
        except Exception as e:
            logger.error(f"Error creating tasks: {e}")
            return []
    
    async def send_notifications(self, document: Document, assignments: List[str], tasks: List[str]):
        """Send notifications about new document"""
        try:
            for user_id in assignments:
                payload = {
                    "user_id": user_id,
                    "title": f"New Email Document: {document.filename}",
                    "message": f"A new document has been received via email and assigned to you.\n\nSummary: {document.summary[:100]}...",
                    "notification_type": "document_processed",
                    "priority": document.priority.value.lower(),
                    "channels": ["websocket", "in_app"],
                    "metadata": {
                        "document_id": str(document.id),
                        "source": "email",
                        "auto_generated": True
                    }
                }
                
                try:
                    response = requests.post(f"{self.notification_service_url}/notifications", 
                                           json=payload, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"Notification sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send notification to user {user_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error sending notifications: {e}")
    
    async def send_confirmation_email(self, recipient: str, original_subject: str, attachment_count: int):
        """Send confirmation email to sender"""
        try:
            # Clean recipient email
            if '<' in recipient:
                recipient = recipient.split('<')[1].split('>')[0]
            
            msg = MIMEMultipart()
            msg['From'] = self.email_username
            msg['To'] = recipient
            msg['Subject'] = f"Re: {original_subject} - Documents Processed"
            
            body = f"""
            Dear User,
            
            Thank you for your email submission to MetroMind KMRL Document Management System.
            
            We have successfully received and processed {attachment_count} document(s) from your email.
            Your documents have been automatically:
            - Analyzed and categorized
            - Assigned to appropriate personnel
            - Added to our tracking system
            
            You will receive updates on the progress of your document review.
            
            Thank you for using MetroMind.
            
            Best regards,
            MetroMind System
            KMRL Document Management
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.email_username, self.email_password)
                server.send_message(msg)
            
            logger.info(f"Confirmation email sent to {recipient}")
            
        except Exception as e:
            logger.error(f"Error sending confirmation email: {e}")


# FastAPI service
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="MetroMind Email Integration Service",
    description="Email integration and automated document processing",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global service instance
email_service = EmailIntegrationService()

class EmailConfig(BaseModel):
    server: str
    port: int
    username: str
    password: str
    smtp_server: str
    smtp_port: int

@app.post("/configure")
async def configure_email(config: EmailConfig):
    """Configure email settings"""
    email_service.email_server = config.server
    email_service.email_port = config.port
    email_service.email_username = config.username
    email_service.email_password = config.password
    email_service.smtp_server = config.smtp_server
    email_service.smtp_port = config.smtp_port
    
    return {"success": True, "message": "Email configuration updated"}

@app.post("/start-monitoring")
async def start_monitoring(background_tasks: BackgroundTasks):
    """Start email monitoring"""
    background_tasks.add_task(email_service.start_email_monitoring)
    return {"success": True, "message": "Email monitoring started"}

@app.get("/status")
async def get_status():
    """Get service status"""
    return {
        "service": "email_integration",
        "status": "running",
        "email_server": email_service.email_server,
        "last_check": email_service.last_check,
        "processed_emails": len(email_service.processed_emails)
    }

@app.post("/test-connection")
async def test_email_connection():
    """Test email connection"""
    try:
        mail = imaplib.IMAP4_SSL(email_service.email_server, email_service.email_port)
        mail.login(email_service.email_username, email_service.email_password)
        mail.select('inbox')
        mail.close()
        mail.logout()
        
        return {"success": True, "message": "Email connection successful"}
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "email_integration"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)