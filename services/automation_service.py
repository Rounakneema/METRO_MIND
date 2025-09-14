"""
MetroMind Automated Processing Service
Handles background automation for document processing, task assignment, and workflow management
"""

import asyncio
import aiofiles
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import uuid
import hashlib
import mimetypes
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import requests
import time

# Import database models
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Document, User, Task, DocumentCategory, Priority, UserRole
from config import service_config
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

class DocumentProcessor:
    """Handles automated document processing pipeline"""
    
    def __init__(self):
        self.upload_dirs = [
            "data/uploads",
            "data/temp",
            "data/watch_folder"
        ]
        self.processed_files = set()
        self.ai_service_url = "http://localhost:8004"
        self.ocr_service_url = "http://localhost:8001"
        self.document_service_url = "http://localhost:8003"
        self.notification_service_url = "http://localhost:8006"
        
    async def process_document_automatically(self, file_path: str) -> Dict[str, Any]:
        """Complete automated document processing pipeline"""
        try:
            # Step 1: File validation and metadata extraction
            file_info = await self._extract_file_info(file_path)
            if not file_info:
                return {"success": False, "error": "Invalid file"}
                
            # Step 2: Content extraction (OCR if needed)
            content = await self._extract_content(file_path, file_info)
            
            # Step 3: AI Analysis
            ai_analysis = await self._analyze_with_ai(content, file_info)
            
            # Step 4: Create document record
            document = await self._create_document_record(file_path, file_info, content, ai_analysis)
            
            # Step 5: Auto-assign to appropriate users
            assignments = await self._auto_assign_document(document, ai_analysis)
            
            # Step 6: Create tasks automatically
            tasks = await self._create_automatic_tasks(document, ai_analysis, assignments)
            
            # Step 7: Send notifications
            await self._send_notifications(document, assignments, tasks)
            
            logger.info(f"Successfully processed document: {file_info['filename']}")
            return {
                "success": True,
                "document_id": document.id,
                "assignments": len(assignments),
                "tasks_created": len(tasks)
            }
            
        except Exception as e:
            logger.error(f"Error processing document {file_path}: {e}")
            return {"success": False, "error": str(e)}
    
    async def _extract_file_info(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Extract basic file information"""
        try:
            path = Path(file_path)
            if not path.exists():
                return None
                
            stats = path.stat()
            mime_type, _ = mimetypes.guess_type(file_path)
            
            # Calculate file hash
            with open(file_path, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            
            return {
                "filename": path.name,
                "original_filename": path.name,
                "file_path": str(path),
                "file_size": stats.st_size,
                "mime_type": mime_type or "application/octet-stream",
                "file_hash": file_hash,
                "created_at": datetime.fromtimestamp(stats.st_ctime, tz=timezone.utc),
                "modified_at": datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc)
            }
        except Exception as e:
            logger.error(f"Error extracting file info for {file_path}: {e}")
            return None
    
    async def _extract_content(self, file_path: str, file_info: Dict[str, Any]) -> str:
        """Extract content from file using appropriate service"""
        try:
            mime_type = file_info.get("mime_type", "")
            
            # For images, use OCR service
            if mime_type.startswith("image/"):
                return await self._extract_with_ocr(file_path)
            
            # For PDFs and documents, use document service
            elif mime_type in ["application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
                return await self._extract_with_document_service(file_path)
            
            # For text files, read directly
            elif mime_type.startswith("text/"):
                async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                    return await f.read()
            
            return ""
            
        except Exception as e:
            logger.error(f"Error extracting content from {file_path}: {e}")
            return ""
    
    async def _extract_with_ocr(self, file_path: str) -> str:
        """Extract text using OCR service"""
        try:
            with open(file_path, 'rb') as f:
                files = {'file': f}
                response = requests.post(f"{self.ocr_service_url}/extract", files=files, timeout=30)
                
            if response.status_code == 200:
                result = response.json()
                return result.get("extracted_text", "")
            return ""
        except Exception as e:
            logger.error(f"OCR extraction failed for {file_path}: {e}")
            return ""
    
    async def _extract_with_document_service(self, file_path: str) -> str:
        """Extract text using document service"""
        try:
            with open(file_path, 'rb') as f:
                files = {'file': f}
                response = requests.post(f"{self.document_service_url}/extract", files=files, timeout=30)
                
            if response.status_code == 200:
                result = response.json()
                return result.get("content", "")
            return ""
        except Exception as e:
            logger.error(f"Document extraction failed for {file_path}: {e}")
            return ""
    
    async def _analyze_with_ai(self, content: str, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze content with AI service"""
        try:
            if not content.strip():
                return {"category": "GENERAL", "priority": "MEDIUM", "summary": "", "entities": {}}
            
            payload = {
                "text": content,
                "filename": file_info.get("filename", ""),
                "analyze_all": True
            }
            
            response = requests.post(f"{self.ai_service_url}/process", json=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            
            # Fallback analysis
            return self._fallback_analysis(content)
            
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._fallback_analysis(content)
    
    def _fallback_analysis(self, content: str) -> Dict[str, Any]:
        """Fallback analysis when AI service is unavailable"""
        content_lower = content.lower()
        
        # Simple keyword-based classification
        if any(word in content_lower for word in ['emergency', 'urgent', 'critical', 'safety']):
            category = "SAFETY"
            priority = "HIGH"
        elif any(word in content_lower for word in ['budget', 'payment', 'invoice', 'finance']):
            category = "FINANCE"
            priority = "MEDIUM"
        elif any(word in content_lower for word in ['maintenance', 'repair', 'equipment']):
            category = "MAINTENANCE"
            priority = "MEDIUM"
        elif any(word in content_lower for word in ['schedule', 'operation', 'passenger']):
            category = "OPERATIONS"
            priority = "MEDIUM"
        else:
            category = "GENERAL"
            priority = "LOW"
        
        return {
            "category": category,
            "priority": priority,
            "summary": content[:200] + "..." if len(content) > 200 else content,
            "entities": {},
            "confidence": 0.7
        }
    
    async def _create_document_record(self, file_path: str, file_info: Dict[str, Any], 
                                    content: str, ai_analysis: Dict[str, Any]) -> Document:
        """Create document record in database"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            # Map category string to enum
            category_mapping = {
                "SAFETY": DocumentCategory.SAFETY,
                "FINANCE": DocumentCategory.FINANCE,
                "MAINTENANCE": DocumentCategory.MAINTENANCE,
                "OPERATIONS": DocumentCategory.OPERATIONS,
                "GENERAL": DocumentCategory.GENERAL
            }
            
            # Map priority string to enum
            priority_mapping = {
                "HIGH": Priority.HIGH,
                "MEDIUM": Priority.MEDIUM,
                "LOW": Priority.LOW
            }
            
            category = category_mapping.get(ai_analysis.get("category", "GENERAL"), DocumentCategory.GENERAL)
            priority = priority_mapping.get(ai_analysis.get("priority", "MEDIUM"), Priority.MEDIUM)
            
            document = Document(
                id=str(uuid.uuid4()),
                filename=file_info["filename"],
                original_filename=file_info["original_filename"],
                file_path=file_info["file_path"],
                file_size=file_info["file_size"],
                mime_type=file_info["mime_type"],
                file_hash=file_info["file_hash"],
                extracted_text=content,
                summary=ai_analysis.get("summary", ""),
                category=category,
                priority=priority,
                key_entities=ai_analysis.get("entities", {}),
                sentiment_score=ai_analysis.get("sentiment", 0.0),
                language_detected=ai_analysis.get("language", "en"),
                uploaded_by="system",  # System upload
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            
            db.add(document)
            db.commit()
            db.refresh(document)
            db.close()
            
            return document
            
        except Exception as e:
            logger.error(f"Error creating document record: {e}")
            raise
    
    async def _auto_assign_document(self, document: Document, ai_analysis: Dict[str, Any]) -> List[str]:
        """Automatically assign document to appropriate users based on category and content"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            # Assignment rules based on category and role
            assignment_rules = {
                DocumentCategory.SAFETY: [UserRole.STATION_CONTROLLER, UserRole.ADMIN],
                DocumentCategory.FINANCE: [UserRole.FINANCE_MANAGER, UserRole.ADMIN],
                DocumentCategory.MAINTENANCE: [UserRole.MAINTENANCE_HEAD, UserRole.ADMIN],
                DocumentCategory.OPERATIONS: [UserRole.STATION_CONTROLLER, UserRole.ADMIN],
                DocumentCategory.GENERAL: [UserRole.ADMIN]
            }
            
            # Get target roles for this document category
            target_roles = assignment_rules.get(document.category, [UserRole.ADMIN])
            
            # Find users with target roles
            assigned_users = []
            for role in target_roles:
                users = db.query(User).filter(User.role == role).all()
                assigned_users.extend([user.id for user in users])
            
            # If high priority, also assign to all admins
            if document.priority == Priority.HIGH:
                admin_users = db.query(User).filter(User.role == UserRole.ADMIN).all()
                assigned_users.extend([user.id for user in admin_users])
            
            # Remove duplicates
            assigned_users = list(set(assigned_users))
            
            db.close()
            return assigned_users
            
        except Exception as e:
            logger.error(f"Error auto-assigning document: {e}")
            return []
    
    async def _create_automatic_tasks(self, document: Document, ai_analysis: Dict[str, Any], 
                                    assignments: List[str]) -> List[str]:
        """Automatically create tasks based on document analysis"""
        try:
            from sqlalchemy.orm import Session
            db = next(get_db())
            
            tasks_created = []
            
            # Create tasks based on document category and priority
            task_templates = {
                DocumentCategory.SAFETY: [
                    "Review safety protocol document",
                    "Assess safety compliance requirements",
                    "Update safety procedures if needed"
                ],
                DocumentCategory.FINANCE: [
                    "Review financial document",
                    "Verify budget allocations",
                    "Approve financial transactions"
                ],
                DocumentCategory.MAINTENANCE: [
                    "Review maintenance requirements",
                    "Schedule maintenance activities",
                    "Update maintenance records"
                ],
                DocumentCategory.OPERATIONS: [
                    "Review operational procedures",
                    "Update schedules if required",
                    "Coordinate with relevant teams"
                ]
            }
            
            # Get task templates for this category
            templates = task_templates.get(document.category, ["Review document"])
            
            # Create tasks for each assigned user
            for user_id in assignments:
                for template in templates:
                    task = Task(
                        id=str(uuid.uuid4()),
                        title=f"{template} - {document.filename}",
                        description=f"Auto-generated task for document: {document.filename}\n\nSummary: {document.summary}",
                        document_id=document.id,
                        assigned_to=user_id,
                        priority=document.priority,
                        status="PENDING",
                        category=document.category.value,
                        created_at=datetime.now(timezone.utc),
                        due_date=datetime.now(timezone.utc) + timedelta(days=3)  # 3 days to complete
                    )
                    
                    db.add(task)
                    tasks_created.append(task.id)
            
            db.commit()
            db.close()
            
            return tasks_created
            
        except Exception as e:
            logger.error(f"Error creating automatic tasks: {e}")
            return []
    
    async def _send_notifications(self, document: Document, assignments: List[str], tasks: List[str]):
        """Send notifications about new document and tasks"""
        try:
            # Prepare notification data
            notification_data = {
                "title": f"New Document: {document.filename}",
                "message": f"A new {document.category.value} document has been processed and assigned to you.\n\nSummary: {document.summary[:100]}...",
                "type": "document_processed",
                "priority": document.priority.value.lower(),
                "document_id": document.id,
                "metadata": {
                    "document_category": document.category.value,
                    "tasks_created": len(tasks),
                    "auto_generated": True
                }
            }
            
            # Send notification to each assigned user
            for user_id in assignments:
                try:
                    payload = {
                        "user_id": user_id,
                        "title": notification_data["title"],
                        "message": notification_data["message"],
                        "notification_type": notification_data["type"],
                        "priority": notification_data["priority"],
                        "channels": ["websocket", "in_app"],
                        "metadata": notification_data["metadata"]
                    }
                    
                    response = requests.post(f"{self.notification_service_url}/notifications", 
                                           json=payload, timeout=10)
                    
                    if response.status_code == 200:
                        logger.info(f"Notification sent to user {user_id}")
                    else:
                        logger.warning(f"Failed to send notification to user {user_id}")
                        
                except Exception as e:
                    logger.error(f"Error sending notification to user {user_id}: {e}")
            
        except Exception as e:
            logger.error(f"Error sending notifications: {e}")


class FileWatcher(FileSystemEventHandler):
    """Watches directories for new files and triggers processing"""
    
    def __init__(self, processor: DocumentProcessor):
        self.processor = processor
        self.processing_queue = asyncio.Queue()
        
    def on_created(self, event):
        if not event.is_directory:
            logger.info(f"New file detected: {event.src_path}")
            # Add to processing queue
            asyncio.create_task(self.add_to_queue(event.src_path))
    
    async def add_to_queue(self, file_path: str):
        """Add file to processing queue"""
        await self.processing_queue.put(file_path)


class AutomationService:
    """Main automation service orchestrator"""
    
    def __init__(self):
        self.processor = DocumentProcessor()
        self.file_watcher = FileWatcher(self.processor)
        self.observer = Observer()
        self.running = False
        
    async def start(self):
        """Start the automation service"""
        logger.info("Starting MetroMind Automation Service...")
        
        # Create watch directories if they don't exist
        for directory in self.processor.upload_dirs:
            Path(directory).mkdir(parents=True, exist_ok=True)
            
        # Set up file system watchers
        for directory in self.processor.upload_dirs:
            self.observer.schedule(self.file_watcher, directory, recursive=True)
            logger.info(f"Watching directory: {directory}")
        
        self.observer.start()
        self.running = True
        
        # Start processing queue worker
        asyncio.create_task(self.process_queue())
        
        logger.info("Automation service started successfully")
    
    async def stop(self):
        """Stop the automation service"""
        logger.info("Stopping automation service...")
        self.running = False
        self.observer.stop()
        self.observer.join()
        logger.info("Automation service stopped")
    
    async def process_queue(self):
        """Process files from the queue"""
        while self.running:
            try:
                # Wait for new files with timeout
                file_path = await asyncio.wait_for(
                    self.file_watcher.processing_queue.get(), 
                    timeout=1.0
                )
                
                # Wait a moment for file to be fully written
                await asyncio.sleep(2)
                
                # Check if file still exists and is complete
                if Path(file_path).exists():
                    # Process the document
                    result = await self.processor.process_document_automatically(file_path)
                    
                    if result["success"]:
                        logger.info(f"Successfully processed: {file_path}")
                        # Move processed file to processed directory
                        await self.move_processed_file(file_path)
                    else:
                        logger.error(f"Failed to process: {file_path} - {result.get('error')}")
                
            except asyncio.TimeoutError:
                # No new files, continue monitoring
                continue
            except Exception as e:
                logger.error(f"Error in processing queue: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def move_processed_file(self, file_path: str):
        """Move processed file to processed directory"""
        try:
            processed_dir = Path("data/processed")
            processed_dir.mkdir(parents=True, exist_ok=True)
            
            source = Path(file_path)
            destination = processed_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{source.name}"
            
            source.rename(destination)
            logger.info(f"Moved processed file to: {destination}")
            
        except Exception as e:
            logger.error(f"Error moving processed file: {e}")


# Entry point for running the automation service
async def main():
    """Main entry point for automation service"""
    automation = AutomationService()
    
    try:
        await automation.start()
        
        # Keep the service running
        while True:
            await asyncio.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await automation.stop()


if __name__ == "__main__":
    asyncio.run(main())