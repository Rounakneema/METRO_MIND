"""
RAG Chatbot Service
Intelligent chatbot that can answer questions about documents using RAG (Retrieval-Augmented Generation)
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
import json
import logging
import re

# Import our models and config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Document, User, Task, DocumentCategory
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

app = FastAPI(
    title="MetroMind RAG Chatbot Service",
    description="Intelligent document Q&A using Retrieval-Augmented Generation",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatMessage(BaseModel):
    message: str
    user_id: str
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    sources: List[Dict[str, Any]]
    confidence: float
    suggestions: List[str]

class RAGChatbot:
    """RAG-based chatbot for document queries"""
    
    def __init__(self):
        # Service URLs
        self.search_service_url = "http://localhost:8007"
        self.ai_service_url = "http://localhost:8004"
        
        # Chat context storage
        self.chat_sessions = {}
        
        # Predefined quick responses
        self.quick_responses = {
            "greeting": "Hello! I'm your MetroMind assistant. I can help you find information from your documents, answer questions about tasks, and provide insights about your work. What would you like to know?",
            "help": "I can help you with:\n• Finding specific documents\n• Answering questions about document content\n• Getting task information\n• Providing summaries and insights\n• Searching across all your documents\n\nJust ask me anything!",
            "tasks": "I can help you with task-related queries. Try asking:\n• 'What are my current tasks?'\n• 'Show me overdue tasks'\n• 'What maintenance tasks are pending?'",
            "documents": "I can search through all your documents. Try asking:\n• 'Find documents about safety protocols'\n• 'What maintenance reports do we have?'\n• 'Show me budget documents'"
        }
        
    async def process_message(self, message: str, user_id: str, session_id: str, db: Session) -> ChatResponse:
        """Process chat message and generate response"""
        try:
            # Normalize message
            message_lower = message.lower().strip()
            
            # Check for quick responses
            if any(word in message_lower for word in ['hello', 'hi', 'hey']):
                return ChatResponse(
                    response=self.quick_responses["greeting"],
                    sources=[],
                    confidence=1.0,
                    suggestions=["What are my current tasks?", "Find safety documents", "Show me recent uploads"]
                )
            
            if any(word in message_lower for word in ['help', 'what can you do']):
                return ChatResponse(
                    response=self.quick_responses["help"],
                    sources=[],
                    confidence=1.0,
                    suggestions=["Show my tasks", "Find documents", "Get document summary"]
                )
            
            # Determine query type and process accordingly
            if self._is_task_query(message_lower):
                return await self._handle_task_query(message, user_id, db)
            elif self._is_document_search_query(message_lower):
                return await self._handle_document_search(message, user_id, db)
            elif self._is_specific_question(message_lower):
                return await self._handle_document_question(message, user_id, db)
            else:
                return await self._handle_general_query(message, user_id, db)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return ChatResponse(
                response="I apologize, but I encountered an error while processing your request. Please try again.",
                sources=[],
                confidence=0.0,
                suggestions=["Try a different question", "Check my tasks", "Search documents"]
            )
    
    def _is_task_query(self, message: str) -> bool:
        """Check if message is asking about tasks"""
        task_keywords = ['task', 'assignment', 'todo', 'work', 'assigned', 'due', 'deadline', 'overdue', 'pending', 'completed']
        return any(keyword in message for keyword in task_keywords)
    
    def _is_document_search_query(self, message: str) -> bool:
        """Check if message is searching for documents"""
        search_keywords = ['find', 'search', 'show', 'get', 'documents', 'files', 'reports']
        return any(keyword in message for keyword in search_keywords)
    
    def _is_specific_question(self, message: str) -> bool:
        """Check if message is asking specific question about content"""
        question_words = ['what', 'how', 'why', 'when', 'where', 'who', 'which', 'explain', 'tell me']
        return any(message.startswith(word) for word in question_words) or '?' in message
    
    async def _handle_task_query(self, message: str, user_id: str, db: Session) -> ChatResponse:
        """Handle task-related queries"""
        try:
            message_lower = message.lower()
            
            # Get user's tasks
            tasks_query = db.query(Task).filter(Task.assigned_to == user_id)
            
            if 'overdue' in message_lower:
                # Get overdue tasks
                now = datetime.now(timezone.utc)
                tasks = tasks_query.filter(
                    Task.due_date < now,
                    Task.status != "COMPLETED"
                ).order_by(Task.due_date.asc()).limit(10).all()
                
                if not tasks:
                    response = "Great news! You don't have any overdue tasks."
                else:
                    response = f"You have {len(tasks)} overdue tasks:\n\n"
                    for i, task in enumerate(tasks, 1):
                        days_overdue = (now - task.due_date).days
                        response += f"{i}. **{task.title}**\n   - Due: {task.due_date.strftime('%Y-%m-%d')}\n   - Overdue by: {days_overdue} days\n   - Priority: {task.priority.name}\n\n"
                
            elif 'pending' in message_lower:
                # Get pending tasks
                tasks = tasks_query.filter(Task.status == "PENDING").order_by(desc(Task.priority), Task.due_date.asc()).limit(10).all()
                
                if not tasks:
                    response = "You don't have any pending tasks."
                else:
                    response = f"You have {len(tasks)} pending tasks:\n\n"
                    for i, task in enumerate(tasks, 1):
                        due_text = f"Due: {task.due_date.strftime('%Y-%m-%d')}" if task.due_date else "No due date"
                        response += f"{i}. **{task.title}**\n   - {due_text}\n   - Priority: {task.priority.name}\n\n"
                        
            elif 'completed' in message_lower:
                # Get completed tasks
                tasks = tasks_query.filter(Task.status == "COMPLETED").order_by(desc(Task.completed_at)).limit(10).all()
                
                if not tasks:
                    response = "You haven't completed any tasks yet."
                else:
                    response = f"Your {len(tasks)} most recent completed tasks:\n\n"
                    for i, task in enumerate(tasks, 1):
                        completed_date = task.completed_at.strftime('%Y-%m-%d') if task.completed_at else "Unknown"
                        response += f"{i}. **{task.title}**\n   - Completed: {completed_date}\n\n"
                        
            else:
                # Get all current tasks
                tasks = tasks_query.filter(Task.status != "COMPLETED").order_by(desc(Task.priority), Task.due_date.asc()).limit(10).all()
                
                if not tasks:
                    response = "You don't have any current tasks assigned."
                else:
                    response = f"You have {len(tasks)} current tasks:\n\n"
                    for i, task in enumerate(tasks, 1):
                        due_text = f"Due: {task.due_date.strftime('%Y-%m-%d')}" if task.due_date else "No due date"
                        response += f"{i}. **{task.title}**\n   - Status: {task.status}\n   - {due_text}\n   - Priority: {task.priority.name}\n\n"
            
            return ChatResponse(
                response=response,
                sources=[],
                confidence=0.95,
                suggestions=["Show overdue tasks", "Mark task as completed", "Get task details"]
            )
            
        except Exception as e:
            logger.error(f"Error handling task query: {e}")
            return ChatResponse(
                response="I couldn't retrieve your tasks at the moment. Please try again.",
                sources=[],
                confidence=0.0,
                suggestions=["Try again", "Check task status"]
            )
    
    async def _handle_document_search(self, message: str, user_id: str, db: Session) -> ChatResponse:
        """Handle document search queries"""
        try:
            # Extract search terms from message
            search_terms = self._extract_search_terms(message)
            
            if not search_terms:
                return ChatResponse(
                    response="Please specify what documents you're looking for. For example: 'Find safety documents' or 'Show me maintenance reports'.",
                    sources=[],
                    confidence=0.0,
                    suggestions=["Find safety documents", "Show maintenance reports", "Search budget files"]
                )
            
            # Search documents using multiple approaches
            results = []
            
            # 1. Search by category
            category_results = await self._search_by_category(search_terms, db)
            results.extend(category_results)
            
            # 2. Search by filename
            filename_results = await self._search_by_filename(search_terms, db)
            results.extend(filename_results)
            
            # 3. Search by content
            content_results = await self._search_by_content(search_terms, db)
            results.extend(content_results)
            
            # Remove duplicates and limit results
            unique_results = []
            seen_ids = set()
            for result in results:
                if result['id'] not in seen_ids:
                    unique_results.append(result)
                    seen_ids.add(result['id'])
                    if len(unique_results) >= 10:
                        break
            
            if not unique_results:
                response = f"I couldn't find any documents matching '{' '.join(search_terms)}'. Try using different keywords or check the document upload section."
            else:
                response = f"I found {len(unique_results)} documents matching your search:\n\n"
                for i, doc in enumerate(unique_results, 1):
                    response += f"{i}. **{doc['filename']}**\n"
                    response += f"   - Category: {doc['category']}\n"
                    response += f"   - Uploaded: {doc['created_at']}\n"
                    if doc.get('summary'):
                        response += f"   - Summary: {doc['summary'][:100]}...\n"
                    response += "\n"
            
            return ChatResponse(
                response=response,
                sources=unique_results,
                confidence=0.8 if unique_results else 0.2,
                suggestions=["Get document summary", "Open document", "Search related documents"]
            )
            
        except Exception as e:
            logger.error(f"Error handling document search: {e}")
            return ChatResponse(
                response="I encountered an error while searching documents. Please try again.",
                sources=[],
                confidence=0.0,
                suggestions=["Try a different search", "Check document list"]
            )
    
    async def _handle_document_question(self, message: str, user_id: str, db: Session) -> ChatResponse:
        """Handle specific questions about document content"""
        try:
            # Use vector search service for semantic search
            search_payload = {
                "query": message,
                "limit": 5,
                "user_id": user_id
            }
            
            try:
                response = requests.post(f"{self.search_service_url}/search/semantic", 
                                       json=search_payload, timeout=30)
                
                if response.status_code == 200:
                    search_results = response.json().get('results', [])
                else:
                    search_results = []
            except:
                search_results = []
            
            if not search_results:
                # Fallback to simple keyword search
                keywords = self._extract_search_terms(message)
                search_results = await self._search_by_content(keywords, db)
            
            # Generate answer using AI service
            if search_results:
                context = "\n\n".join([
                    f"Document: {doc['filename']}\nContent: {doc.get('content', doc.get('summary', ''))[:500]}"
                    for doc in search_results[:3]
                ])
                
                ai_payload = {
                    "prompt": f"Based on the following document content, answer this question: {message}\n\nContext:\n{context}",
                    "max_length": 300
                }
                
                try:
                    ai_response = requests.post(f"{self.ai_service_url}/generate", 
                                              json=ai_payload, timeout=30)
                    
                    if ai_response.status_code == 200:
                        answer = ai_response.json().get('response', '')
                    else:
                        answer = ""
                except:
                    answer = ""
                
                if answer:
                    response_text = f"{answer}\n\n**Sources:**\n"
                    for i, doc in enumerate(search_results[:3], 1):
                        response_text += f"{i}. {doc['filename']} ({doc['category']})\n"
                else:
                    response_text = "Based on the documents I found, here are the relevant documents that might contain your answer:\n\n"
                    for i, doc in enumerate(search_results[:3], 1):
                        response_text += f"{i}. **{doc['filename']}**\n"
                        if doc.get('summary'):
                            response_text += f"   Summary: {doc['summary'][:150]}...\n"
                        response_text += "\n"
            else:
                response_text = "I couldn't find relevant documents to answer your question. Please try rephrasing or check if the documents containing this information have been uploaded."
            
            return ChatResponse(
                response=response_text,
                sources=search_results[:5],
                confidence=0.7 if search_results else 0.2,
                suggestions=["Ask another question", "Search for documents", "Get more details"]
            )
            
        except Exception as e:
            logger.error(f"Error handling document question: {e}")
            return ChatResponse(
                response="I encountered an error while searching for an answer. Please try asking your question differently.",
                sources=[],
                confidence=0.0,
                suggestions=["Rephrase question", "Search documents", "Try simpler query"]
            )
    
    async def _handle_general_query(self, message: str, user_id: str, db: Session) -> ChatResponse:
        """Handle general queries"""
        return ChatResponse(
            response="I can help you with tasks and document-related questions. Try asking:\n• 'What are my current tasks?'\n• 'Find documents about [topic]'\n• 'What does this document say about [topic]?'\n• 'Show me overdue tasks'",
            sources=[],
            confidence=0.5,
            suggestions=["Show my tasks", "Find documents", "Ask about document content"]
        )
    
    def _extract_search_terms(self, message: str) -> List[str]:
        """Extract meaningful search terms from message"""
        # Remove common words and extract keywords
        stop_words = {'find', 'show', 'get', 'me', 'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'about', 'all', 'any', 'can', 'could', 'do', 'does', 'did', 'will', 'would', 'should', 'may', 'might', 'must', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'this', 'that', 'these', 'those'}
        
        words = re.findall(r'\b\w+\b', message.lower())
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        
        return keywords
    
    async def _search_by_category(self, search_terms: List[str], db: Session) -> List[Dict[str, Any]]:
        """Search documents by category"""
        category_mapping = {
            'safety': DocumentCategory.SAFETY,
            'finance': DocumentCategory.FINANCE,
            'maintenance': DocumentCategory.MAINTENANCE,
            'operations': DocumentCategory.OPERATIONS,
            'budget': DocumentCategory.FINANCE,
            'financial': DocumentCategory.FINANCE,
            'repair': DocumentCategory.MAINTENANCE,
            'equipment': DocumentCategory.MAINTENANCE,
        }
        
        results = []
        for term in search_terms:
            if term in category_mapping:
                docs = db.query(Document).filter(Document.category == category_mapping[term]).limit(5).all()
                for doc in docs:
                    results.append({
                        'id': str(doc.id),
                        'filename': doc.filename,
                        'category': doc.category.value,
                        'summary': doc.summary,
                        'created_at': doc.created_at.strftime('%Y-%m-%d')
                    })
        
        return results
    
    async def _search_by_filename(self, search_terms: List[str], db: Session) -> List[Dict[str, Any]]:
        """Search documents by filename"""
        results = []
        for term in search_terms:
            docs = db.query(Document).filter(Document.filename.ilike(f'%{term}%')).limit(5).all()
            for doc in docs:
                results.append({
                    'id': str(doc.id),
                    'filename': doc.filename,
                    'category': doc.category.value,
                    'summary': doc.summary,
                    'created_at': doc.created_at.strftime('%Y-%m-%d')
                })
        
        return results
    
    async def _search_by_content(self, search_terms: List[str], db: Session) -> List[Dict[str, Any]]:
        """Search documents by content"""
        results = []
        for term in search_terms:
            docs = db.query(Document).filter(
                Document.extracted_text.ilike(f'%{term}%') |
                Document.summary.ilike(f'%{term}%')
            ).limit(5).all()
            
            for doc in docs:
                results.append({
                    'id': str(doc.id),
                    'filename': doc.filename,
                    'category': doc.category.value,
                    'summary': doc.summary,
                    'content': doc.extracted_text[:200] if doc.extracted_text else '',
                    'created_at': doc.created_at.strftime('%Y-%m-%d')
                })
        
        return results

# Global chatbot instance
chatbot = RAGChatbot()

@app.post("/chat", response_model=ChatResponse)
async def chat(message_data: ChatMessage, db: Session = Depends(get_db)):
    """Main chat endpoint"""
    try:
        response = await chatbot.process_message(
            message_data.message,
            message_data.user_id,
            message_data.session_id or "default",
            db
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(status_code=500, detail="Chat service error")

@app.get("/chat/suggestions")
async def get_suggestions(user_id: str, db: Session = Depends(get_db)):
    """Get suggested questions for user"""
    try:
        # Get some context about user's data
        task_count = db.query(Task).filter(Task.assigned_to == user_id).count()
        doc_count = db.query(Document).filter(Document.uploaded_by == user_id).count()
        
        suggestions = [
            "What are my current tasks?",
            "Show me overdue tasks",
            "Find safety documents",
            "What maintenance tasks are pending?",
            "Search for budget reports"
        ]
        
        if task_count > 0:
            suggestions.insert(0, f"I have {task_count} tasks assigned")
        
        if doc_count > 0:
            suggestions.append("Show me my uploaded documents")
        
        return {"suggestions": suggestions[:6]}
        
    except Exception as e:
        logger.error(f"Error getting suggestions: {e}")
        return {"suggestions": ["What can you help me with?"]}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "rag_chatbot"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8020)