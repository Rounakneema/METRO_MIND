"""
MetroMind Analytics Service
Comprehensive analytics, reporting, and insights dashboard
User activity tracking, document metrics, system performance analysis
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc, asc
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple
import asyncio
import json
import logging
from enum import Enum
import pandas as pd
import io
import csv
from collections import defaultdict, Counter

# Analytics and visualization imports
try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import seaborn as sns
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly.utils
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Import our models and config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import (
    get_db, User, Document, AnalyticsRecord, Session as DBSession,
    DocumentEmbedding, Notification, Integration
)
from config import service_config, analytics_config
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)

# Analytics enums and models
class MetricType(str, Enum):
    DOCUMENT_UPLOAD = "document_upload"
    DOCUMENT_VIEW = "document_view"
    DOCUMENT_DOWNLOAD = "document_download"
    SEARCH_QUERY = "search_query"
    USER_LOGIN = "user_login"
    USER_REGISTRATION = "user_registration"
    AI_ANALYSIS = "ai_analysis"
    INTEGRATION_SYNC = "integration_sync"
    NOTIFICATION_SENT = "notification_sent"
    SYSTEM_ERROR = "system_error"

class ReportType(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    CUSTOM = "custom"

class ChartType(str, Enum):
    LINE = "line"
    BAR = "bar"
    PIE = "pie"
    HEATMAP = "heatmap"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"

# Pydantic models
class AnalyticsQuery(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    user_ids: Optional[List[int]] = None
    metric_types: Optional[List[MetricType]] = None
    group_by: Optional[str] = "day"  # day, week, month, user, category
    limit: int = 100
    
class DashboardRequest(BaseModel):
    date_range: int = 30  # days
    include_charts: bool = True
    include_tables: bool = True
    user_specific: Optional[int] = None

class ReportRequest(BaseModel):
    report_type: ReportType
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    format: str = "json"  # json, csv, excel
    include_charts: bool = False

class MetricData(BaseModel):
    timestamp: datetime
    value: float
    metadata: Dict[str, Any] = {}

class AnalyticsResponse(BaseModel):
    metrics: Dict[str, List[MetricData]]
    summary: Dict[str, Any]
    charts: Optional[Dict[str, Any]] = None

# FastAPI app
app = FastAPI(
    title="MetroMind Analytics Service",
    description="Comprehensive analytics, reporting, and insights dashboard",
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

# Analytics engine
class AnalyticsEngine:
    """Core analytics processing engine"""
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        
    async def get_dashboard_data(self, request: DashboardRequest, db: Session) -> Dict[str, Any]:
        """Get comprehensive dashboard data"""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=request.date_range)
        
        # Get basic metrics
        metrics = await self._get_basic_metrics(start_date, end_date, request.user_specific, db)
        
        # Get user activity
        user_activity = await self._get_user_activity(start_date, end_date, request.user_specific, db)
        
        # Get document metrics
        document_metrics = await self._get_document_metrics(start_date, end_date, request.user_specific, db)
        
        # Get system health
        system_health = await self._get_system_health(start_date, end_date, db)
        
        # Get integration status
        integration_stats = await self._get_integration_stats(request.user_specific, db)
        
        dashboard = {
            "overview": metrics,
            "user_activity": user_activity,
            "document_metrics": document_metrics,
            "system_health": system_health,
            "integrations": integration_stats,
            "time_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": request.date_range
            }
        }
        
        # Add charts if requested
        if request.include_charts:
            dashboard["charts"] = await self._generate_dashboard_charts(dashboard, db)
        
        return dashboard
    
    async def _get_basic_metrics(self, start_date: datetime, end_date: datetime, 
                                user_id: Optional[int], db: Session) -> Dict[str, Any]:
        """Get basic system metrics"""
        try:
            # Base queries
            user_query = db.query(User)
            doc_query = db.query(Document).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date
            )
            analytics_query = db.query(AnalyticsRecord).filter(
                AnalyticsRecord.timestamp >= start_date,
                AnalyticsRecord.timestamp <= end_date
            )
            
            # Apply user filter if specified
            if user_id:
                doc_query = doc_query.filter(Document.uploaded_by == user_id)
                analytics_query = analytics_query.filter(AnalyticsRecord.user_id == user_id)
            
            # Get counts
            total_users = user_query.count()
            active_users = user_query.filter(
                User.last_login >= start_date
            ).count() if not user_id else (1 if user_query.filter(User.id == user_id).first() else 0)
            
            documents_uploaded = doc_query.count()
            total_documents = db.query(Document).count()
            
            # Analytics events
            total_events = analytics_query.count()
            search_queries = analytics_query.filter(
                AnalyticsRecord.event_type == "search"
            ).count()
            
            # Document categories
            category_stats = db.query(
                Document.category,
                func.count(Document.id).label('count')
            ).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date
            )
            
            if user_id:
                category_stats = category_stats.filter(Document.uploaded_by == user_id)
            
            category_stats = category_stats.group_by(Document.category).all()
            
            # File type distribution
            file_type_stats = db.query(
                Document.mime_type,
                func.count(Document.id).label('count')
            ).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date
            )
            
            if user_id:
                file_type_stats = file_type_stats.filter(Document.uploaded_by == user_id)
            
            file_type_stats = file_type_stats.group_by(Document.mime_type).all()
            
            return {
                "total_users": total_users,
                "active_users": active_users,
                "documents_uploaded": documents_uploaded,
                "total_documents": total_documents,
                "total_events": total_events,
                "search_queries": search_queries,
                "category_distribution": [
                    {"category": cat.value if hasattr(cat, 'value') else str(cat), "count": count}
                    for cat, count in category_stats
                ],
                "file_type_distribution": [
                    {"file_type": ft, "count": count}
                    for ft, count in file_type_stats
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting basic metrics: {e}")
            return {}
    
    async def _get_user_activity(self, start_date: datetime, end_date: datetime,
                                user_id: Optional[int], db: Session) -> Dict[str, Any]:
        """Get user activity metrics"""
        try:
            # Daily activity
            daily_activity = db.query(
                func.date(AnalyticsRecord.timestamp).label('date'),
                func.count(AnalyticsRecord.id).label('events')
            ).filter(
                AnalyticsRecord.timestamp >= start_date,
                AnalyticsRecord.timestamp <= end_date
            )
            
            if user_id:
                daily_activity = daily_activity.filter(AnalyticsRecord.user_id == user_id)
            
            daily_activity = daily_activity.group_by(
                func.date(AnalyticsRecord.timestamp)
            ).all()
            
            # Event type distribution
            event_types = db.query(
                AnalyticsRecord.event_type,
                func.count(AnalyticsRecord.id).label('count')
            ).filter(
                AnalyticsRecord.timestamp >= start_date,
                AnalyticsRecord.timestamp <= end_date
            )
            
            if user_id:
                event_types = event_types.filter(AnalyticsRecord.user_id == user_id)
            
            event_types = event_types.group_by(AnalyticsRecord.event_type).all()
            
            # Top active users (if not user-specific)
            top_users = []
            if not user_id:
                top_users = db.query(
                    User.username,
                    func.count(AnalyticsRecord.id).label('events')
                ).join(
                    AnalyticsRecord, User.id == AnalyticsRecord.user_id
                ).filter(
                    AnalyticsRecord.timestamp >= start_date,
                    AnalyticsRecord.timestamp <= end_date
                ).group_by(User.id, User.username).order_by(
                    desc('events')
                ).limit(10).all()
            
            return {
                "daily_activity": [
                    {"date": str(date), "events": events}
                    for date, events in daily_activity
                ],
                "event_types": [
                    {"event_type": event_type, "count": count}
                    for event_type, count in event_types
                ],
                "top_users": [
                    {"username": username, "events": events}
                    for username, events in top_users
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting user activity: {e}")
            return {}
    
    async def _get_document_metrics(self, start_date: datetime, end_date: datetime,
                                  user_id: Optional[int], db: Session) -> Dict[str, Any]:
        """Get document-related metrics"""
        try:
            # Document uploads over time
            daily_uploads = db.query(
                func.date(Document.created_at).label('date'),
                func.count(Document.id).label('uploads')
            ).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date
            )
            
            if user_id:
                daily_uploads = daily_uploads.filter(Document.uploaded_by == user_id)
            
            daily_uploads = daily_uploads.group_by(
                func.date(Document.created_at)
            ).all()
            
            # File size statistics
            size_stats = db.query(
                func.avg(Document.file_size).label('avg_size'),
                func.sum(Document.file_size).label('total_size'),
                func.max(Document.file_size).label('max_size'),
                func.min(Document.file_size).label('min_size')
            ).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date
            )
            
            if user_id:
                size_stats = size_stats.filter(Document.uploaded_by == user_id)
            
            size_stats = size_stats.first()
            
            # Processing statistics
            processed_docs = db.query(Document).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date,
                Document.extracted_text.isnot(None)
            )
            
            if user_id:
                processed_docs = processed_docs.filter(Document.uploaded_by == user_id)
            
            processed_count = processed_docs.count()
            
            # Language distribution
            lang_stats = db.query(
                Document.language_detected,
                func.count(Document.id).label('count')
            ).filter(
                Document.created_at >= start_date,
                Document.created_at <= end_date,
                Document.language_detected.isnot(None)
            )
            
            if user_id:
                lang_stats = lang_stats.filter(Document.uploaded_by == user_id)
            
            lang_stats = lang_stats.group_by(Document.language_detected).all()
            
            return {
                "daily_uploads": [
                    {"date": str(date), "uploads": uploads}
                    for date, uploads in daily_uploads
                ],
                "file_size_stats": {
                    "average_size": float(size_stats.avg_size or 0),
                    "total_size": int(size_stats.total_size or 0),
                    "max_size": int(size_stats.max_size or 0),
                    "min_size": int(size_stats.min_size or 0)
                } if size_stats else {},
                "processed_documents": processed_count,
                "language_distribution": [
                    {"language": lang, "count": count}
                    for lang, count in lang_stats
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting document metrics: {e}")
            return {}
    
    async def _get_system_health(self, start_date: datetime, end_date: datetime, 
                                db: Session) -> Dict[str, Any]:
        """Get system health metrics"""
        try:
            # Error events
            error_count = db.query(AnalyticsRecord).filter(
                AnalyticsRecord.timestamp >= start_date,
                AnalyticsRecord.timestamp <= end_date,
                AnalyticsRecord.event_type == "error"
            ).count()
            
            # Response times (from search analytics)
            search_times = db.query(AnalyticsRecord.event_data).filter(
                AnalyticsRecord.timestamp >= start_date,
                AnalyticsRecord.timestamp <= end_date,
                AnalyticsRecord.event_type == "search"
            ).all()
            
            # Extract response times
            response_times = []
            for record in search_times:
                if record.event_data and isinstance(record.event_data, dict):
                    if 'search_time_ms' in record.event_data:
                        response_times.append(record.event_data['search_time_ms'])
            
            avg_response_time = sum(response_times) / len(response_times) if response_times else 0
            
            # Service uptime (simplified)
            uptime_percentage = 99.5  # Would be calculated from actual service monitoring
            
            return {
                "error_count": error_count,
                "average_response_time_ms": avg_response_time,
                "uptime_percentage": uptime_percentage,
                "health_status": "healthy" if error_count < 10 else "degraded"
            }
            
        except Exception as e:
            logger.error(f"Error getting system health: {e}")
            return {}
    
    async def _get_integration_stats(self, user_id: Optional[int], db: Session) -> Dict[str, Any]:
        """Get integration statistics"""
        try:
            query = db.query(Integration)
            if user_id:
                query = query.filter(
                    (Integration.user_id == user_id) | (Integration.is_global == True)
                )
            
            integrations = query.all()
            
            total_integrations = len(integrations)
            active_integrations = len([i for i in integrations if i.status == "active"])
            
            # Integration type distribution
            type_distribution = Counter([i.integration_type for i in integrations])
            
            # Sync statistics
            total_syncs = sum([i.sync_count for i in integrations])
            total_errors = sum([i.error_count for i in integrations])
            
            return {
                "total_integrations": total_integrations,
                "active_integrations": active_integrations,
                "type_distribution": [
                    {"type": int_type, "count": count}
                    for int_type, count in type_distribution.items()
                ],
                "sync_stats": {
                    "total_syncs": total_syncs,
                    "total_errors": total_errors,
                    "success_rate": ((total_syncs - total_errors) / total_syncs * 100) if total_syncs > 0 else 100
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting integration stats: {e}")
            return {}
    
    async def _generate_dashboard_charts(self, dashboard_data: Dict[str, Any], 
                                       db: Session) -> Dict[str, Any]:
        """Generate charts for dashboard data"""
        charts = {}
        
        try:
            if PLOTLY_AVAILABLE:
                # User activity chart
                if "user_activity" in dashboard_data:
                    daily_data = dashboard_data["user_activity"]["daily_activity"]
                    if daily_data:
                        fig = px.line(
                            daily_data,
                            x="date",
                            y="events",
                            title="Daily User Activity"
                        )
                        charts["user_activity"] = json.loads(fig.to_json())
                
                # Document uploads chart
                if "document_metrics" in dashboard_data:
                    upload_data = dashboard_data["document_metrics"]["daily_uploads"]
                    if upload_data:
                        fig = px.bar(
                            upload_data,
                            x="date",
                            y="uploads",
                            title="Daily Document Uploads"
                        )
                        charts["document_uploads"] = json.loads(fig.to_json())
                
                # Category distribution pie chart
                if "overview" in dashboard_data:
                    cat_data = dashboard_data["overview"]["category_distribution"]
                    if cat_data:
                        fig = px.pie(
                            cat_data,
                            values="count",
                            names="category",
                            title="Document Categories"
                        )
                        charts["category_distribution"] = json.loads(fig.to_json())
            
        except Exception as e:
            logger.error(f"Error generating charts: {e}")
        
        return charts

    async def generate_report(self, request: ReportRequest, db: Session) -> Any:
        """Generate comprehensive reports"""
        try:
            # Determine date range
            if request.report_type == ReportType.DAILY:
                start_date = datetime.now(timezone.utc) - timedelta(days=1)
                end_date = datetime.now(timezone.utc)
            elif request.report_type == ReportType.WEEKLY:
                start_date = datetime.now(timezone.utc) - timedelta(weeks=1)
                end_date = datetime.now(timezone.utc)
            elif request.report_type == ReportType.MONTHLY:
                start_date = datetime.now(timezone.utc) - timedelta(days=30)
                end_date = datetime.now(timezone.utc)
            elif request.report_type == ReportType.QUARTERLY:
                start_date = datetime.now(timezone.utc) - timedelta(days=90)
                end_date = datetime.now(timezone.utc)
            elif request.report_type == ReportType.YEARLY:
                start_date = datetime.now(timezone.utc) - timedelta(days=365)
                end_date = datetime.now(timezone.utc)
            else:  # CUSTOM
                start_date = request.start_date or (datetime.now(timezone.utc) - timedelta(days=30))
                end_date = request.end_date or datetime.now(timezone.utc)
            
            # Get comprehensive data
            dashboard_request = DashboardRequest(
                date_range=(end_date - start_date).days,
                include_charts=request.include_charts
            )
            
            report_data = await self.get_dashboard_data(dashboard_request, db)
            
            # Add report metadata
            report_data["report_info"] = {
                "type": request.report_type,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "format": request.format
            }
            
            return report_data
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            raise

    async def track_event(self, user_id: int, event_type: str, 
                         event_data: Dict[str, Any], db: Session):
        """Track analytics event"""
        try:
            record = AnalyticsRecord(
                user_id=user_id,
                event_type=event_type,
                event_data=event_data,
                timestamp=datetime.now(timezone.utc)
            )
            
            db.add(record)
            db.commit()
            
        except Exception as e:
            logger.error(f"Error tracking event: {e}")

# Global analytics engine
analytics_engine = AnalyticsEngine()

@app.on_event("startup")
async def startup_event():
    """Initialize analytics service"""
    logger.info("Analytics service started")

# API Endpoints
@app.get("/dashboard")
async def get_dashboard(
    date_range: int = Query(30, description="Number of days to analyze"),
    user_id: Optional[int] = Query(None, description="Filter by specific user"),
    include_charts: bool = Query(True, description="Include chart data"),
    db: Session = Depends(get_db)
):
    """Get comprehensive dashboard data"""
    try:
        request = DashboardRequest(
            date_range=date_range,
            include_charts=include_charts,
            user_specific=user_id
        )
        
        dashboard_data = await analytics_engine.get_dashboard_data(request, db)
        
        # Transform data to match frontend expectations
        overview = dashboard_data.get("overview", {})
        
        # Get department stats from category distribution
        department_stats = []
        category_dist = overview.get("category_distribution", [])
        for cat in category_dist:
            department_stats.append({
                "department": cat.get("category", "Unknown"),
                "documentCount": cat.get("count", 0)
            })
        
        frontend_data = {
            "totalDocuments": overview.get("total_documents", 0),
            "processedToday": overview.get("documents_uploaded", 0),
            "pendingTasks": 0,  # This would need to be calculated based on processing status
            "departmentStats": department_stats
        }
        
        return frontend_data
        
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        # Return empty data structure on error instead of raising exception
        return {
            "totalDocuments": 0,
            "processedToday": 0,
            "pendingTasks": 0,
            "departmentStats": []
        }

@app.get("/metrics/{metric_type}")
async def get_specific_metrics(
    metric_type: MetricType,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    user_id: Optional[int] = Query(None),
    group_by: str = Query("day", regex="^(day|week|month|user)$"),
    db: Session = Depends(get_db)
):
    """Get specific metric data"""
    try:
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=30)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        query = db.query(AnalyticsRecord).filter(
            AnalyticsRecord.timestamp >= start_date,
            AnalyticsRecord.timestamp <= end_date,
            AnalyticsRecord.event_type == metric_type
        )
        
        if user_id:
            query = query.filter(AnalyticsRecord.user_id == user_id)
        
        records = query.all()
        
        # Group data based on group_by parameter
        grouped_data = defaultdict(int)
        for record in records:
            if group_by == "day":
                key = record.timestamp.date().isoformat()
            elif group_by == "week":
                week_start = record.timestamp - timedelta(days=record.timestamp.weekday())
                key = week_start.date().isoformat()
            elif group_by == "month":
                key = record.timestamp.strftime("%Y-%m")
            elif group_by == "user":
                key = str(record.user_id)
            
            grouped_data[key] += 1
        
        metrics = [
            MetricData(
                timestamp=datetime.fromisoformat(key) if group_by != "user" else datetime.now(),
                value=value,
                metadata={"group_by": group_by, "key": key}
            )
            for key, value in grouped_data.items()
        ]
        
        return {
            "metric_type": metric_type,
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "group_by": group_by,
            "data": metrics,
            "total": len(records)
        }
        
    except Exception as e:
        logger.error(f"Metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/{report_type}")
async def generate_report(
    report_type: ReportType,
    format: str = Query("json", regex="^(json|csv|excel)$"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    include_charts: bool = Query(False),
    db: Session = Depends(get_db)
):
    """Generate comprehensive reports"""
    try:
        request = ReportRequest(
            report_type=report_type,
            start_date=start_date,
            end_date=end_date,
            format=format,
            include_charts=include_charts
        )
        
        report_data = await analytics_engine.generate_report(request, db)
        
        if format == "json":
            return report_data
        elif format == "csv":
            return await _export_csv(report_data)
        elif format == "excel":
            return await _export_excel(report_data)
        
    except Exception as e:
        logger.error(f"Report generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/events")
async def track_event(
    user_id: int,
    event_type: str,
    event_data: Dict[str, Any] = {},
    db: Session = Depends(get_db)
):
    """Track custom analytics event"""
    try:
        await analytics_engine.track_event(user_id, event_type, event_data, db)
        
        return {"success": True, "message": "Event tracked successfully"}
        
    except Exception as e:
        logger.error(f"Event tracking error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{user_id}/activity")
async def get_user_activity(
    user_id: int,
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Get detailed user activity"""
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        end_date = datetime.now(timezone.utc)
        
        # Get user info
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get activity records
        activities = db.query(AnalyticsRecord).filter(
            AnalyticsRecord.user_id == user_id,
            AnalyticsRecord.timestamp >= start_date,
            AnalyticsRecord.timestamp <= end_date
        ).order_by(desc(AnalyticsRecord.timestamp)).all()
        
        # Get document stats for user
        doc_stats = db.query(
            func.count(Document.id).label('total_docs'),
            func.sum(Document.file_size).label('total_size')
        ).filter(
            Document.uploaded_by == user_id
        ).first()
        
        return {
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "role": user.role,
                "created_at": user.created_at,
                "last_login": user.last_login
            },
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": days
            },
            "activity_summary": {
                "total_events": len(activities),
                "event_types": dict(Counter([a.event_type for a in activities])),
                "documents_uploaded": doc_stats.total_docs if doc_stats else 0,
                "total_storage_used": doc_stats.total_size if doc_stats and doc_stats.total_size else 0
            },
            "recent_activities": [
                {
                    "timestamp": activity.timestamp.isoformat(),
                    "event_type": activity.event_type,
                    "event_data": activity.event_data
                }
                for activity in activities[:50]  # Last 50 activities
            ]
        }
        
    except Exception as e:
        logger.error(f"User activity error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/analytics")
async def get_search_analytics(
    days: int = Query(30),
    db: Session = Depends(get_db)
):
    """Get search analytics and insights"""
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        end_date = datetime.now(timezone.utc)
        
        # Get search events
        search_events = db.query(AnalyticsRecord).filter(
            AnalyticsRecord.event_type == "search",
            AnalyticsRecord.timestamp >= start_date,
            AnalyticsRecord.timestamp <= end_date
        ).all()
        
        # Analyze search patterns
        search_queries = []
        search_times = []
        search_types = Counter()
        
        for event in search_events:
            if event.event_data:
                if 'query' in event.event_data:
                    search_queries.append(event.event_data['query'])
                if 'search_time_ms' in event.event_data:
                    search_times.append(event.event_data['search_time_ms'])
                if 'search_type' in event.event_data:
                    search_types[event.event_data['search_type']] += 1
        
        # Top search terms
        query_words = []
        for query in search_queries:
            query_words.extend(query.lower().split())
        
        top_terms = Counter(query_words).most_common(20)
        
        # Performance metrics
        avg_search_time = sum(search_times) / len(search_times) if search_times else 0
        
        return {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": days
            },
            "summary": {
                "total_searches": len(search_events),
                "unique_queries": len(set(search_queries)),
                "average_search_time_ms": avg_search_time
            },
            "search_types": dict(search_types),
            "top_search_terms": [
                {"term": term, "count": count}
                for term, count in top_terms
            ],
            "performance": {
                "fastest_search_ms": min(search_times) if search_times else 0,
                "slowest_search_ms": max(search_times) if search_times else 0,
                "median_search_time_ms": sorted(search_times)[len(search_times)//2] if search_times else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Search analytics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Export functions
async def _export_csv(data: Dict[str, Any]) -> StreamingResponse:
    """Export data as CSV"""
    try:
        output = io.StringIO()
        
        # Write basic metrics
        writer = csv.writer(output)
        writer.writerow(["Metric", "Value"])
        
        if "overview" in data:
            overview = data["overview"]
            for key, value in overview.items():
                if isinstance(value, (int, float, str)):
                    writer.writerow([key, value])
        
        # Write daily activity if available
        if "user_activity" in data and "daily_activity" in data["user_activity"]:
            writer.writerow([])  # Empty row
            writer.writerow(["Date", "Events"])
            
            for item in data["user_activity"]["daily_activity"]:
                writer.writerow([item["date"], item["events"]])
        
        output.seek(0)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=analytics_report.csv"}
        )
        
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        raise HTTPException(status_code=500, detail="Failed to export CSV")

async def _export_excel(data: Dict[str, Any]) -> StreamingResponse:
    """Export data as Excel"""
    try:
        # Create Excel file in memory
        output = io.BytesIO()
        
        # Simple Excel export using pandas if available
        try:
            import pandas as pd
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Overview sheet
                if "overview" in data:
                    overview_df = pd.DataFrame([
                        {"Metric": k, "Value": v}
                        for k, v in data["overview"].items()
                        if isinstance(v, (int, float, str))
                    ])
                    overview_df.to_excel(writer, sheet_name="Overview", index=False)
                
                # Activity sheet
                if "user_activity" in data and "daily_activity" in data["user_activity"]:
                    activity_df = pd.DataFrame(data["user_activity"]["daily_activity"])
                    activity_df.to_excel(writer, sheet_name="Daily Activity", index=False)
            
            output.seek(0)
            
            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": "attachment; filename=analytics_report.xlsx"}
            )
            
        except ImportError:
            # Fallback to CSV if pandas/openpyxl not available
            return await _export_csv(data)
        
    except Exception as e:
        logger.error(f"Excel export error: {e}")
        raise HTTPException(status_code=500, detail="Failed to export Excel")

@app.get("/")
async def root():
    """Service root endpoint"""
    return {
        "service": "MetroMind Analytics Service",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "capabilities": {
            "matplotlib_available": MATPLOTLIB_AVAILABLE,
            "plotly_available": PLOTLY_AVAILABLE
        }
    }

@app.get("/health")
async def health_check():
    """Service health check"""
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "capabilities": {
                "chart_generation": PLOTLY_AVAILABLE or MATPLOTLIB_AVAILABLE,
                "data_export": True
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
    
    logger.info(f"Starting Analytics service on port {service_config.analytics_service_port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=service_config.analytics_service_port,
        log_level="info"
    )
