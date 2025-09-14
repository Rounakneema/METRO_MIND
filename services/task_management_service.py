"""
Task Management Service
Provides API endpoints for task operations to support the dashboard
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
import uuid
import logging

# Import our models and config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Task, User, Document, Priority, DocumentCategory
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

app = FastAPI(
    title="MetroMind Task Management Service",
    description="Task management and assignment system",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    document_id: Optional[str] = None
    assigned_to: str
    priority: str = "MEDIUM"
    category: Optional[str] = None
    due_date: Optional[datetime] = None
    estimated_hours: Optional[float] = None
    tags: Optional[List[str]] = []

class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    progress_percentage: Optional[int] = None
    actual_hours: Optional[float] = None
    tags: Optional[List[str]] = None

class TaskComment(BaseModel):
    task_id: str
    user_id: str
    comment: str

class TaskResponse(BaseModel):
    id: str
    title: str
    description: Optional[str]
    document_id: Optional[str]
    assigned_to: str
    assigned_by: Optional[str]
    priority: str
    status: str
    category: Optional[str]
    progress_percentage: int
    due_date: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    
    # Additional fields for UI
    document_filename: Optional[str] = None
    assigned_user_name: Optional[str] = None
    is_overdue: bool = False
    days_remaining: Optional[int] = None

class DashboardStats(BaseModel):
    total_tasks: int
    pending_tasks: int
    in_progress_tasks: int
    completed_tasks: int
    overdue_tasks: int
    my_tasks: int
    high_priority_tasks: int

@app.get("/tasks", response_model=List[TaskResponse])
async def get_tasks(
    user_id: Optional[str] = Query(None, description="Filter by assigned user"),
    status: Optional[str] = Query(None, description="Filter by status"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, description="Number of tasks to return"),
    offset: int = Query(0, description="Offset for pagination"),
    db: Session = Depends(get_db)
):
    """Get tasks with filtering options"""
    try:
        query = db.query(Task)
        
        # Apply filters
        if user_id:
            query = query.filter(Task.assigned_to == user_id)
        if status:
            query = query.filter(Task.status == status)
        if priority:
            query = query.filter(Task.priority == priority)
        if category:
            query = query.filter(Task.category == category)
        
        # Order by priority and due date
        query = query.order_by(desc(Task.priority), Task.due_date.asc())
        
        # Apply pagination
        tasks = query.offset(offset).limit(limit).all()
        
        # Format response with additional info
        task_responses = []
        for task in tasks:
            # Get document filename if exists
            document_filename = None
            if task.document_id:
                document = db.query(Document).filter(Document.id == task.document_id).first()
                if document:
                    document_filename = document.filename
            
            # Get assigned user name
            assigned_user_name = None
            if task.assigned_to:
                user = db.query(User).filter(User.id == task.assigned_to).first()
                if user:
                    assigned_user_name = f"{user.first_name} {user.last_name}"
            
            # Calculate overdue and days remaining
            is_overdue = False
            days_remaining = None
            if task.due_date:
                now = datetime.now(timezone.utc)
                if task.due_date < now and task.status != "COMPLETED":
                    is_overdue = True
                    days_remaining = (now - task.due_date).days
                else:
                    days_remaining = (task.due_date - now).days
            
            task_response = TaskResponse(
                id=str(task.id),
                title=task.title,
                description=task.description,
                document_id=str(task.document_id) if task.document_id else None,
                assigned_to=str(task.assigned_to),
                assigned_by=str(task.assigned_by) if task.assigned_by else None,
                priority=task.priority.name if hasattr(task.priority, 'name') else str(task.priority),
                status=task.status,
                category=task.category,
                progress_percentage=task.progress_percentage,
                due_date=task.due_date,
                created_at=task.created_at,
                updated_at=task.updated_at,
                document_filename=document_filename,
                assigned_user_name=assigned_user_name,
                is_overdue=is_overdue,
                days_remaining=days_remaining
            )
            
            task_responses.append(task_response)
        
        return task_responses
        
    except Exception as e:
        logger.error(f"Error getting tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, db: Session = Depends(get_db)):
    """Get specific task by ID"""
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # Get additional info
        document_filename = None
        if task.document_id:
            document = db.query(Document).filter(Document.id == task.document_id).first()
            if document:
                document_filename = document.filename
        
        assigned_user_name = None
        if task.assigned_to:
            user = db.query(User).filter(User.id == task.assigned_to).first()
            if user:
                assigned_user_name = f"{user.first_name} {user.last_name}"
        
        # Calculate overdue status
        is_overdue = False
        days_remaining = None
        if task.due_date:
            now = datetime.now(timezone.utc)
            if task.due_date < now and task.status != "COMPLETED":
                is_overdue = True
                days_remaining = (now - task.due_date).days
            else:
                days_remaining = (task.due_date - now).days
        
        return TaskResponse(
            id=str(task.id),
            title=task.title,
            description=task.description,
            document_id=str(task.document_id) if task.document_id else None,
            assigned_to=str(task.assigned_to),
            assigned_by=str(task.assigned_by) if task.assigned_by else None,
            priority=task.priority.name if hasattr(task.priority, 'name') else str(task.priority),
            status=task.status,
            category=task.category,
            progress_percentage=task.progress_percentage,
            due_date=task.due_date,
            created_at=task.created_at,
            updated_at=task.updated_at,
            document_filename=document_filename,
            assigned_user_name=assigned_user_name,
            is_overdue=is_overdue,
            days_remaining=days_remaining
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tasks", response_model=TaskResponse)
async def create_task(task_data: TaskCreate, db: Session = Depends(get_db)):
    """Create new task"""
    try:
        # Map priority string to enum
        priority_mapping = {
            "HIGH": Priority.HIGH,
            "MEDIUM": Priority.MEDIUM,
            "LOW": Priority.LOW,
            "CRITICAL": Priority.CRITICAL
        }
        
        priority = priority_mapping.get(task_data.priority.upper(), Priority.MEDIUM)
        
        task = Task(
            id=str(uuid.uuid4()),
            title=task_data.title,
            description=task_data.description,
            document_id=task_data.document_id,
            assigned_to=task_data.assigned_to,
            priority=priority,
            status="PENDING",
            category=task_data.category,
            due_date=task_data.due_date,
            estimated_hours=task_data.estimated_hours,
            tags=task_data.tags or [],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        db.add(task)
        db.commit()
        db.refresh(task)
        
        return TaskResponse(
            id=str(task.id),
            title=task.title,
            description=task.description,
            document_id=str(task.document_id) if task.document_id else None,
            assigned_to=str(task.assigned_to),
            assigned_by=str(task.assigned_by) if task.assigned_by else None,
            priority=task.priority.name,
            status=task.status,
            category=task.category,
            progress_percentage=task.progress_percentage,
            due_date=task.due_date,
            created_at=task.created_at,
            updated_at=task.updated_at
        )
        
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/tasks/{task_id}", response_model=TaskResponse)
async def update_task(task_id: str, task_data: TaskUpdate, db: Session = Depends(get_db)):
    """Update existing task"""
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # Update fields that are provided
        if task_data.title is not None:
            task.title = task_data.title
        if task_data.description is not None:
            task.description = task_data.description
        if task_data.status is not None:
            task.status = task_data.status
            if task_data.status == "COMPLETED":
                task.completed_at = datetime.now(timezone.utc)
                task.progress_percentage = 100
        if task_data.priority is not None:
            priority_mapping = {
                "HIGH": Priority.HIGH,
                "MEDIUM": Priority.MEDIUM,
                "LOW": Priority.LOW,
                "CRITICAL": Priority.CRITICAL
            }
            task.priority = priority_mapping.get(task_data.priority.upper(), task.priority)
        if task_data.progress_percentage is not None:
            task.progress_percentage = task_data.progress_percentage
        if task_data.actual_hours is not None:
            task.actual_hours = task_data.actual_hours
        if task_data.tags is not None:
            task.tags = task_data.tags
        
        task.updated_at = datetime.now(timezone.utc)
        
        db.commit()
        db.refresh(task)
        
        return TaskResponse(
            id=str(task.id),
            title=task.title,
            description=task.description,
            document_id=str(task.document_id) if task.document_id else None,
            assigned_to=str(task.assigned_to),
            assigned_by=str(task.assigned_by) if task.assigned_by else None,
            priority=task.priority.name,
            status=task.status,
            category=task.category,
            progress_percentage=task.progress_percentage,
            due_date=task.due_date,
            created_at=task.created_at,
            updated_at=task.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str, db: Session = Depends(get_db)):
    """Delete task"""
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        db.delete(task)
        db.commit()
        
        return {"success": True, "message": "Task deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats(
    user_id: Optional[str] = Query(None, description="Get stats for specific user"),
    db: Session = Depends(get_db)
):
    """Get dashboard statistics"""
    try:
        base_query = db.query(Task)
        
        # Total tasks
        total_tasks = base_query.count()
        
        # Status counts
        pending_tasks = base_query.filter(Task.status == "PENDING").count()
        in_progress_tasks = base_query.filter(Task.status == "IN_PROGRESS").count()
        completed_tasks = base_query.filter(Task.status == "COMPLETED").count()
        
        # Overdue tasks
        now = datetime.now(timezone.utc)
        overdue_tasks = base_query.filter(
            and_(
                Task.due_date < now,
                Task.status != "COMPLETED"
            )
        ).count()
        
        # High priority tasks
        high_priority_tasks = base_query.filter(Task.priority == Priority.HIGH).count()
        
        # User-specific tasks
        my_tasks = 0
        if user_id:
            my_tasks = base_query.filter(Task.assigned_to == user_id).count()
        
        return DashboardStats(
            total_tasks=total_tasks,
            pending_tasks=pending_tasks,
            in_progress_tasks=in_progress_tasks,
            completed_tasks=completed_tasks,
            overdue_tasks=overdue_tasks,
            my_tasks=my_tasks,
            high_priority_tasks=high_priority_tasks
        )
        
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tasks/{task_id}/comments")
async def add_task_comment(task_id: str, comment_data: TaskComment, db: Session = Depends(get_db)):
    """Add comment to task"""
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # Get user name
        user = db.query(User).filter(User.id == comment_data.user_id).first()
        user_name = f"{user.first_name} {user.last_name}" if user else "Unknown User"
        
        # Add comment to task
        if not task.comments:
            task.comments = []
        
        new_comment = {
            "id": str(uuid.uuid4()),
            "user_id": comment_data.user_id,
            "user_name": user_name,
            "comment": comment_data.comment,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        task.comments.append(new_comment)
        task.updated_at = datetime.now(timezone.utc)
        
        db.commit()
        
        return {"success": True, "comment": new_comment}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding comment to task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tasks/{task_id}/comments")
async def get_task_comments(task_id: str, db: Session = Depends(get_db)):
    """Get comments for task"""
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return {"comments": task.comments or []}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting comments for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "task_management"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8021)