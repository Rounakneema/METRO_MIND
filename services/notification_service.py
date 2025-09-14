"""
MetroMind Notification Service
Real-time notifications, alerts, and WebSocket communications
"""

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Set
import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from enum import Enum

# WebSocket and async imports
import redis.asyncio as redis
from contextlib import asynccontextmanager

# Import our models and config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, User, Notification, NotificationChannel, Document, AnalyticsRecord
from config import service_config, notification_config
from utils.logging_utils import setup_logger
from utils.email_service import EmailService

# Setup
logger = setup_logger(__name__)

# Notification types and priorities
class NotificationType(str, Enum):
    DOCUMENT_UPLOADED = "document_uploaded"
    DOCUMENT_PROCESSED = "document_processed"
    DOCUMENT_SHARED = "document_shared"
    SYSTEM_ALERT = "system_alert"
    USER_MENTION = "user_mention"
    APPROVAL_REQUEST = "approval_request"
    DEADLINE_REMINDER = "deadline_reminder"
    SECURITY_ALERT = "security_alert"
    INTEGRATION_STATUS = "integration_status"

class NotificationPriority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    URGENT = "urgent"

class DeliveryChannel(str, Enum):
    WEBSOCKET = "websocket"
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    IN_APP = "in_app"

# Pydantic models
class NotificationCreateRequest(BaseModel):
    user_id: str
    title: str
    message: str
    notification_type: NotificationType
    priority: NotificationPriority = NotificationPriority.MEDIUM
    channels: List[DeliveryChannel] = [DeliveryChannel.IN_APP]
    metadata: Optional[Dict[str, Any]] = {}
    scheduled_time: Optional[datetime] = None
    expires_at: Optional[datetime] = None

class NotificationResponse(BaseModel):
    id: str
    user_id: str
    title: str
    message: str
    notification_type: str
    priority: str
    channels: List[str]
    is_read: bool
    created_at: datetime
    metadata: Dict[str, Any]

class WebSocketMessage(BaseModel):
    type: str  # notification, alert, system_message, etc.
    data: Dict[str, Any]
    timestamp: datetime
    priority: str = "medium"

class SubscriptionRequest(BaseModel):
    user_id: str
    notification_types: List[NotificationType] = []
    channels: List[DeliveryChannel] = [DeliveryChannel.IN_APP]

# WebSocket Connection Manager
class WebSocketManager:
    """Manages WebSocket connections and message broadcasting"""
    
    def __init__(self):
        # Active connections: user_id -> set of websockets
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # Channel subscriptions: channel -> set of user_ids
        self.channel_subscriptions: Dict[str, Set[str]] = {}
        # User subscriptions: user_id -> set of notification_types
        self.user_subscriptions: Dict[str, Set[str]] = {}
        
    async def connect(self, websocket: WebSocket, user_id: str):
        """Connect a WebSocket for a user"""
        await websocket.accept()
        
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        
        self.active_connections[user_id].add(websocket)
        logger.info(f"WebSocket connected for user {user_id}")
        
        # Send connection confirmation
        await self.send_to_user(user_id, {
            "type": "connection_established",
            "data": {
                "message": "WebSocket connection established",
                "user_id": user_id,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "priority": "low"
        })
    
    async def disconnect(self, websocket: WebSocket, user_id: str):
        """Disconnect a WebSocket"""
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            
            # Remove user if no active connections
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
                
        logger.info(f"WebSocket disconnected for user {user_id}")
    
    async def send_to_user(self, user_id: str, message: Dict[str, Any]):
        """Send message to all WebSocket connections of a user"""
        if user_id not in self.active_connections:
            return False
        
        message_json = json.dumps(message, default=str)
        disconnected_sockets = set()
        
        for websocket in self.active_connections[user_id].copy():
            try:
                await websocket.send_text(message_json)
            except Exception as e:
                logger.warning(f"Failed to send message to user {user_id}: {e}")
                disconnected_sockets.add(websocket)
        
        # Remove disconnected sockets
        for websocket in disconnected_sockets:
            self.active_connections[user_id].discard(websocket)
            
        return len(self.active_connections[user_id]) > 0
    
    async def broadcast_to_channel(self, channel: str, message: Dict[str, Any]):
        """Broadcast message to all users subscribed to a channel"""
        if channel not in self.channel_subscriptions:
            return 0
        
        count = 0
        for user_id in self.channel_subscriptions[channel].copy():
            if await self.send_to_user(user_id, message):
                count += 1
        
        return count
    
    async def broadcast_to_all(self, message: Dict[str, Any]):
        """Broadcast message to all connected users"""
        count = 0
        for user_id in list(self.active_connections.keys()):
            if await self.send_to_user(user_id, message):
                count += 1
        
        return count
    
    def subscribe_user_to_channel(self, user_id: str, channel: str):
        """Subscribe user to a notification channel"""
        if channel not in self.channel_subscriptions:
            self.channel_subscriptions[channel] = set()
        
        self.channel_subscriptions[channel].add(user_id)
        
        if user_id not in self.user_subscriptions:
            self.user_subscriptions[user_id] = set()
        
        self.user_subscriptions[user_id].add(channel)
    
    def unsubscribe_user_from_channel(self, user_id: str, channel: str):
        """Unsubscribe user from a notification channel"""
        if channel in self.channel_subscriptions:
            self.channel_subscriptions[channel].discard(user_id)
            
            if not self.channel_subscriptions[channel]:
                del self.channel_subscriptions[channel]
        
        if user_id in self.user_subscriptions:
            self.user_subscriptions[user_id].discard(channel)
    
    def get_connected_users(self) -> List[str]:
        """Get list of currently connected users"""
        return list(self.active_connections.keys())
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get WebSocket connection statistics"""
        total_connections = sum(len(sockets) for sockets in self.active_connections.values())
        
        return {
            "connected_users": len(self.active_connections),
            "total_connections": total_connections,
            "channels": len(self.channel_subscriptions),
            "channel_subscriptions": {
                channel: len(users) for channel, users in self.channel_subscriptions.items()
            }
        }

# Initialize WebSocket manager
websocket_manager = WebSocketManager()

# Redis client for pub/sub (optional)
redis_client = None

# FastAPI app
app = FastAPI(
    title="MetroMind Notification Service",
    description="Real-time notifications and WebSocket communications",
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

# Background notification processor
class NotificationProcessor:
    """Processes and delivers notifications through various channels"""
    
    def __init__(self):
        self.email_service = EmailService()
        self.processing_queue = asyncio.Queue()
        
    async def start_processing(self):
        """Start background notification processing"""
        asyncio.create_task(self._process_notifications())
        logger.info("Notification processor started")
    
    async def _process_notifications(self):
        """Background task to process notification queue"""
        while True:
            try:
                notification_data = await self.processing_queue.get()
                await self._deliver_notification(notification_data)
                self.processing_queue.task_done()
                
            except Exception as e:
                logger.error(f"Notification processing error: {e}")
                await asyncio.sleep(1)
    
    async def queue_notification(self, notification: Dict[str, Any]):
        """Add notification to processing queue"""
        await self.processing_queue.put(notification)
    
    async def _deliver_notification(self, notification: Dict[str, Any]):
        """Deliver notification through specified channels"""
        try:
            channels = notification.get('channels', [DeliveryChannel.IN_APP])
            
            for channel in channels:
                if channel == DeliveryChannel.WEBSOCKET or channel == DeliveryChannel.IN_APP:
                    await self._deliver_websocket(notification)
                elif channel == DeliveryChannel.EMAIL:
                    await self._deliver_email(notification)
                elif channel == DeliveryChannel.SMS:
                    await self._deliver_sms(notification)
                elif channel == DeliveryChannel.PUSH:
                    await self._deliver_push(notification)
            
            logger.info(f"Notification delivered to user {notification['user_id']} via {channels}")
            
        except Exception as e:
            logger.error(f"Failed to deliver notification: {e}")
    
    async def _deliver_websocket(self, notification: Dict[str, Any]):
        """Deliver notification via WebSocket"""
        message = {
            "type": "notification",
            "data": notification,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "priority": notification.get('priority', 'medium')
        }
        
        success = await websocket_manager.send_to_user(
            notification['user_id'], 
            message
        )
        
        if not success:
            logger.warning(f"Failed to deliver WebSocket notification to user {notification['user_id']}")
    
    async def _deliver_email(self, notification: Dict[str, Any]):
        """Deliver notification via email"""
        try:
            # Get user email from database
            # This would need database session - simplified for now
            user_email = notification.get('user_email')
            if not user_email:
                logger.warning(f"No email address for user {notification['user_id']}")
                return
            
            await self.email_service.send_notification_email(
                user_email,
                notification['title'],
                notification['message'],
                notification.get('metadata', {})
            )
            
        except Exception as e:
            logger.error(f"Email delivery failed: {e}")
    
    async def _deliver_sms(self, notification: Dict[str, Any]):
        """Deliver notification via SMS"""
        # SMS delivery implementation would go here
        logger.info(f"SMS delivery not implemented for user {notification['user_id']}")
    
    async def _deliver_push(self, notification: Dict[str, Any]):
        """Deliver push notification"""
        # Push notification implementation would go here
        logger.info(f"Push notification delivery not implemented for user {notification['user_id']}")

# Initialize notification processor
notification_processor = NotificationProcessor()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global redis_client
    
    try:
        # Initialize Redis if available
        try:
            redis_client = redis.Redis(
                host=notification_config.redis_host,
                port=notification_config.redis_port,
                db=notification_config.redis_db,
                decode_responses=True
            )
            await redis_client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis not available: {e}")
            redis_client = None
        
        # Start notification processor
        await notification_processor.start_processing()
        
        logger.info("Notification service started")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")

# API Endpoints
@app.post("/notifications", response_model=NotificationResponse)
async def create_notification(
    request: NotificationCreateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Create and send a notification"""
    try:
        # Create notification in database
        notification = Notification(
            user_id=request.user_id,
            title=request.title,
            message=request.message,
            notification_type=request.notification_type,
            priority=request.priority,
            channels=request.channels,
            metadata=request.metadata or {},
            scheduled_time=request.scheduled_time,
            expires_at=request.expires_at,
            created_at=datetime.now(timezone.utc)
        )
        
        db.add(notification)
        db.commit()
        db.refresh(notification)
        
        # Queue for delivery if not scheduled
        if not request.scheduled_time or request.scheduled_time <= datetime.now(timezone.utc):
            notification_data = {
                "id": notification.id,
                "user_id": notification.user_id,
                "title": notification.title,
                "message": notification.message,
                "notification_type": notification.notification_type,
                "priority": notification.priority,
                "channels": notification.channels,
                "metadata": notification.metadata,
                "created_at": notification.created_at.isoformat()
            }
            
            await notification_processor.queue_notification(notification_data)
        
        return NotificationResponse(
            id=notification.id,
            user_id=notification.user_id,
            title=notification.title,
            message=notification.message,
            notification_type=notification.notification_type,
            priority=notification.priority,
            channels=[channel.value for channel in notification.channels],
            is_read=notification.is_read,
            created_at=notification.created_at,
            metadata=notification.metadata
        )
        
    except Exception as e:
        logger.error(f"Failed to create notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/notifications/{user_id}")
async def get_user_notifications(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
    unread_only: bool = False,
    db: Session = Depends(get_db)
):
    """Get notifications for a user"""
    try:
        query = db.query(Notification).filter(Notification.user_id == user_id)
        
        if unread_only:
            query = query.filter(Notification.is_read == False)
        
        # Filter out expired notifications
        query = query.filter(
            (Notification.expires_at.is_(None)) | 
            (Notification.expires_at > datetime.now(timezone.utc))
        )
        
        notifications = query.order_by(
            Notification.created_at.desc()
        ).offset(offset).limit(limit).all()
        
        return {
            "notifications": [
                NotificationResponse(
                    id=n.id,
                    user_id=n.user_id,
                    title=n.title,
                    message=n.message,
                    notification_type=n.notification_type,
                    priority=n.priority,
                    channels=[channel.value if hasattr(channel, 'value') else channel for channel in n.channels],
                    is_read=n.is_read,
                    created_at=n.created_at,
                    metadata=n.metadata
                ) for n in notifications
            ],
            "total": len(notifications),
            "unread_count": db.query(Notification).filter(
                Notification.user_id == user_id,
                Notification.is_read == False
            ).count()
        }
        
    except Exception as e:
        logger.error(f"Failed to get notifications for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/notifications/{notification_id}/read")
async def mark_notification_read(
    notification_id: int,
    db: Session = Depends(get_db)
):
    """Mark notification as read"""
    try:
        notification = db.query(Notification).filter(Notification.id == notification_id).first()
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        
        notification.is_read = True
        notification.read_at = datetime.now(timezone.utc)
        
        db.commit()
        
        return {"success": True, "message": "Notification marked as read"}
        
    except Exception as e:
        logger.error(f"Failed to mark notification {notification_id} as read: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/notifications/{user_id}/read-all")
async def mark_all_notifications_read(
    user_id: str,
    db: Session = Depends(get_db)
):
    """Mark all notifications as read for a user"""
    try:
        db.query(Notification).filter(
            Notification.user_id == user_id,
            Notification.is_read == False
        ).update({
            "is_read": True,
            "read_at": datetime.now(timezone.utc)
        })
        
        db.commit()
        
        return {"success": True, "message": "All notifications marked as read"}
        
    except Exception as e:
        logger.error(f"Failed to mark all notifications as read for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/broadcast")
async def broadcast_notification(
    title: str,
    message: str,
    notification_type: NotificationType = NotificationType.SYSTEM_ALERT,
    priority: NotificationPriority = NotificationPriority.MEDIUM,
    channels: List[DeliveryChannel] = [DeliveryChannel.WEBSOCKET],
    db: Session = Depends(get_db)
):
    """Broadcast notification to all connected users"""
    try:
        # Get all active users
        connected_users = websocket_manager.get_connected_users()
        
        if not connected_users:
            return {"success": True, "message": "No connected users", "recipients": 0}
        
        # Create notifications for all users
        notifications = []
        for user_id in connected_users:
            notification = Notification(
                user_id=user_id,
                title=title,
                message=message,
                notification_type=notification_type,
                priority=priority,
                channels=channels,
                created_at=datetime.now(timezone.utc)
            )
            notifications.append(notification)
        
        db.add_all(notifications)
        db.commit()
        
        # Broadcast via WebSocket
        broadcast_message = {
            "type": "broadcast",
            "data": {
                "title": title,
                "message": message,
                "notification_type": notification_type,
                "priority": priority
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "priority": priority
        }
        
        recipients = await websocket_manager.broadcast_to_all(broadcast_message)
        
        return {
            "success": True,
            "message": f"Broadcast sent to {recipients} users",
            "recipients": recipients
        }
        
    except Exception as e:
        logger.error(f"Failed to broadcast notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """WebSocket endpoint for real-time notifications"""
    try:
        # Validate user_id as UUID and convert if needed
        import uuid
        try:
            user_uuid = uuid.UUID(user_id)
        except ValueError:
            await websocket.close(code=1008, reason="Invalid user ID format")
            return
        
        await websocket_manager.connect(websocket, user_id)
        
        while True:
            # Listen for client messages (heartbeat, subscriptions, etc.)
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle different message types
                if message.get("type") == "subscribe":
                    # Subscribe to notification channels
                    channels = message.get("channels", [])
                    for channel in channels:
                        websocket_manager.subscribe_user_to_channel(user_id, channel)
                    
                    await websocket.send_text(json.dumps({
                        "type": "subscription_confirmed",
                        "channels": channels,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }))
                
                elif message.get("type") == "ping":
                    # Heartbeat response
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }))
                
            except asyncio.TimeoutError:
                # Send periodic heartbeat
                await websocket.send_text(json.dumps({
                    "type": "heartbeat",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }))
                
    except WebSocketDisconnect:
        await websocket_manager.disconnect(websocket, user_id)
    except Exception as e:
        logger.error(f"WebSocket error for user {user_id}: {e}")
        await websocket_manager.disconnect(websocket, user_id)

@app.get("/ws/stats")
async def get_websocket_stats():
    """Get WebSocket connection statistics"""
    return websocket_manager.get_connection_stats()

# System alerts and monitoring
@app.post("/alerts/system")
async def send_system_alert(
    title: str,
    message: str,
    priority: NotificationPriority = NotificationPriority.HIGH,
    target_roles: List[str] = ["admin"],
    db: Session = Depends(get_db)
):
    """Send system alert to specific user roles"""
    try:
        # Get users with target roles
        target_users = db.query(User).filter(User.role.in_(target_roles)).all()
        
        if not target_users:
            return {"success": True, "message": "No target users found", "recipients": 0}
        
        # Create and send notifications
        count = 0
        for user in target_users:
            notification_data = {
                "user_id": user.id,
                "title": title,
                "message": message,
                "notification_type": NotificationType.SYSTEM_ALERT,
                "priority": priority,
                "channels": [DeliveryChannel.WEBSOCKET, DeliveryChannel.EMAIL],
                "metadata": {"roles": target_roles}
            }
            
            # Create in database
            notification = Notification(
                user_id=user.id,
                title=title,
                message=message,
                notification_type=NotificationType.SYSTEM_ALERT,
                priority=priority,
                channels=[DeliveryChannel.WEBSOCKET, DeliveryChannel.EMAIL],
                metadata={"roles": target_roles},
                created_at=datetime.now(timezone.utc)
            )
            
            db.add(notification)
            
            # Queue for delivery
            await notification_processor.queue_notification(notification_data)
            count += 1
        
        db.commit()
        
        return {
            "success": True,
            "message": f"System alert sent to {count} users",
            "recipients": count
        }
        
    except Exception as e:
        logger.error(f"Failed to send system alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    """Service root endpoint"""
    return {
        "service": "MetroMind Notification Service",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "websocket_endpoint": "/ws/{user_id}",
        "connected_users": len(websocket_manager.get_connected_users())
    }

@app.get("/health")
async def health_check():
    """Service health check"""
    try:
        stats = websocket_manager.get_connection_stats()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "websocket_connections": stats,
            "redis_available": redis_client is not None,
            "notification_processor_active": True
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }

# Background tasks for scheduled notifications
async def process_scheduled_notifications():
    """Background task to process scheduled notifications"""
    while True:
        try:
            # This would check database for scheduled notifications
            # and queue them when their time arrives
            await asyncio.sleep(60)  # Check every minute
            
        except Exception as e:
            logger.error(f"Scheduled notification processing error: {e}")
            await asyncio.sleep(60)

# Start background tasks
@app.on_event("startup")
async def start_background_tasks():
    """Start background tasks"""
    asyncio.create_task(process_scheduled_notifications())

if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting Notification service on port {service_config.notification_service_port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=service_config.notification_service_port,
        log_level="info"
    )
