"""
MetroMind Database Models
Comprehensive database schema for document management system
"""
from sqlalchemy import text
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean, 
    Float, JSON, ForeignKey, Index, UniqueConstraint, CheckConstraint,
    LargeBinary, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timezone
import uuid
import enum
from typing import Optional, List, Dict, Any
import bcrypt
import logging

from config import get_database_url

logger = logging.getLogger(__name__)

Base = declarative_base()

# Enums
class Permission(enum.Enum):
    # Document Permissions
    CREATE_DOCUMENT = "create_document"
    READ_DOCUMENT = "read_document"
    UPDATE_DOCUMENT = "update_document"
    DELETE_DOCUMENT = "delete_document"
    SHARE_DOCUMENT = "share_document"
    
    # User Management Permissions
    CREATE_USER = "create_user"
    READ_USER = "read_user"
    UPDATE_USER = "update_user"
    DELETE_USER = "delete_user"
    
    # Department Permissions
    MANAGE_DEPARTMENT = "manage_department"
    VIEW_DEPARTMENT_STATS = "view_department_stats"
    
    # System Permissions
    MANAGE_SYSTEM = "manage_system"
    VIEW_AUDIT_LOGS = "view_audit_logs"

class UserRole(enum.Enum):
    ADMIN = "admin"  # Full system access
    MANAGER = "manager"  # Department-level access
    SUPERVISOR = "supervisor"  # Team-level access
    EMPLOYEE = "employee"  # Basic access
    AUDITOR = "auditor"  # Read-only system-wide access
    STATION_CONTROLLER = "station_controller"  # Station operations
    FINANCE_MANAGER = "finance_manager"  # Finance department
    MAINTENANCE_HEAD = "maintenance_head"  # Maintenance department

class UserStatus(enum.Enum):
    PENDING = "pending"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DEACTIVATED = "deactivated"

class NotificationChannel(enum.Enum):
    EMAIL = "email"
    WEBAPP = "webapp"
    SMS = "sms"
    WHATSAPP = "whatsapp"

class IntegrationType(enum.Enum):
    SHAREPOINT = "sharepoint"
    ONEDRIVE = "onedrive"
    GDRIVE = "gdrive"
    EMAIL = "email"
    WHATSAPP = "whatsapp"

class DocumentCategory(enum.Enum):
    SAFETY = "safety"
    MAINTENANCE = "maintenance"
    FINANCE = "finance"
    OPERATIONS = "operations"
    HR = "hr"
    LEGAL = "legal"
    REGULATORY = "regulatory"
    ENVIRONMENTAL = "environmental"
    GENERAL = "general"
    OTHER = "other"

class DocumentStatus(enum.Enum):
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    ARCHIVED = "archived"

class Priority(enum.Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4

class NotificationType(enum.Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"
    DOCUMENT_SHARED = "document_shared"
    DOCUMENT_UNSHARED = "document_unshared"
    DOCUMENT_ACCESS_EXPIRED = "document_access_expired"
    DOCUMENT_UPDATED = "document_updated"

class IntegrationStatus(enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    TESTING = "testing"
    PENDING_APPROVAL = "pending_approval"

# Models
class RolePermission(Base):
    __tablename__ = "role_permissions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    role = Column(Enum(UserRole), nullable=False)
    permission = Column(Enum(Permission), nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    
    __table_args__ = (
        UniqueConstraint('role', 'permission', name='unique_role_permission'),
    )

    def __repr__(self):
        return f"<RolePermission {self.role.value}:{self.permission.value}>"

class AnalyticsRecord(Base):
    __tablename__ = "analytics_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String(50), nullable=False)
    event_data = Column(JSON)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"))
    timestamp = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    d_metadata = Column(JSON)

    # Relationships
    user = relationship("User", back_populates="analytics")
    document = relationship("Document", back_populates="analytics")

class Integration(Base):
    __tablename__ = "integrations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), nullable=False)
    type = Column(Enum(IntegrationType), nullable=False)
    config = Column(JSON, nullable=False)
    status = Column(Enum(IntegrationStatus), default=IntegrationStatus.INACTIVE)
    last_sync = Column(DateTime(timezone=True))
    next_sync = Column(DateTime(timezone=True))
    sync_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_error = Column(Text)
    auto_sync = Column(Boolean, default=True)
    sync_interval_minutes = Column(Integer, default=30)
    is_global = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), onupdate=datetime.now(timezone.utc))
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))

    # Relationships
    user = relationship("User", back_populates="integrations")

class User(Base):
    __tablename__ = "users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    department = Column(String(100), nullable=False)
    role = Column(Enum(UserRole), nullable=False, default=UserRole.EMPLOYEE)
    status = Column(Enum(UserStatus), nullable=False, default=UserStatus.PENDING)
    phone = Column(String(20))
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)

    # Relationships
    analytics = relationship("AnalyticsRecord", back_populates="user", cascade="all, delete-orphan")
    integrations = relationship("Integration", back_populates="user", cascade="all, delete-orphan")
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    last_login = Column(DateTime(timezone=True))
    login_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime(timezone=True))
    preferences = Column(JSON, default={})
    avatar_url = Column(String(500))
    
    # Relationships
    documents = relationship("Document", back_populates="uploaded_by_user", foreign_keys="Document.uploaded_by")
    notifications = relationship("Notification", back_populates="user")
    audit_logs = relationship("AuditLog", back_populates="user")
    data_integrations = relationship("DataIntegration", back_populates="user")
    integrations = relationship("Integration", back_populates="user")
    sessions = relationship("UserSession", back_populates="user")
    assigned_tasks = relationship("Task", foreign_keys="Task.assigned_to", back_populates="assigned_user")
    created_tasks = relationship("Task", foreign_keys="Task.assigned_by", back_populates="assigner_user")
    
    def set_password(self, password: str):
        """Set password with bcrypt hashing"""
        salt = bcrypt.gensalt()
        self.password_hash = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def verify_password(self, password: str) -> bool:
        """Verify password against hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), self.password_hash.encode('utf-8'))
        except Exception:
            return False
    
    def is_locked(self) -> bool:
        """Check if user account is locked"""
        if self.locked_until:
            return datetime.now(timezone.utc) < self.locked_until
        return False
    
    def can_access_document(self, document: 'Document') -> bool:
        """Check if user can access a specific document"""
        if self.role == UserRole.ADMIN:
            return True
        if self.role == UserRole.MANAGER and document.category in [DocumentCategory.OPERATIONS, DocumentCategory.MAINTENANCE]:
            return True
        if document.uploaded_by == self.id:
            return True
        return False
    
    def __repr__(self):
        return f"<User {self.username} ({self.email})>"

class Notification(Base):
    __tablename__ = "notifications"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(Enum(NotificationType), nullable=False)
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    channel = Column(Enum(NotificationChannel), nullable=False, default=NotificationChannel.WEBAPP)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"))
    read = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    read_at = Column(DateTime(timezone=True))
    d_metadata = Column(JSON)

    # Relationships
    user = relationship("User", back_populates="notifications")
    document = relationship("Document", back_populates="notifications")

class UserSession(Base):
    __tablename__ = "user_sessions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    session_token = Column(String(500), unique=True, nullable=False, index=True)
    refresh_token = Column(String(500), unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    last_accessed = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    ip_address = Column(String(45))
    user_agent = Column(Text)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    user = relationship("User", back_populates="sessions")
    
    def is_expired(self) -> bool:
        """Check if session is expired"""
        return datetime.now(timezone.utc) > self.expires_at
    
    def __repr__(self):
        return f"<UserSession {self.user_id} expires {self.expires_at}>"

class Document(Base):
    __tablename__ = "documents"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename = Column(String(500), nullable=False)
    original_filename = Column(String(500), nullable=False)
    file_path = Column(String(1000), nullable=False)
    file_size = Column(Integer, nullable=False)
    mime_type = Column(String(100), nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)  # SHA-256 hash
    
    # Document metadata
    title = Column(String(500))
    description = Column(Text)
    category = Column(Enum(DocumentCategory), nullable=False, index=True)
    priority = Column(Enum(Priority), default=Priority.MEDIUM, index=True)
    tags = Column(JSON, default=[])
    d_metadata = Column(JSON, default={})
    
    # Content
    extracted_text = Column(Text)
    ocr_text = Column(Text)
    summary = Column(Text)
    key_entities = Column(JSON, default={})
    sentiment_score = Column(Float)
    language_detected = Column(String(10))
    
    # Processing status
    status = Column(Enum(DocumentStatus), default=DocumentStatus.PROCESSING, index=True)
    processing_progress = Column(Integer, default=0)
    processing_error = Column(Text)
    ocr_confidence = Column(Float)
    
    # Audit fields
    uploaded_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    processed_at = Column(DateTime(timezone=True))
    
    # Access control
    is_confidential = Column(Boolean, default=False)
    access_level = Column(Integer, default=1)  # 1=Public, 2=Department, 3=Manager, 4=Admin
    
    # Relationships
    uploaded_by_user = relationship("User", back_populates="documents", foreign_keys=[uploaded_by])
    embeddings = relationship("DocumentEmbedding", back_populates="document", cascade="all, delete-orphan")
    notifications = relationship("Notification", back_populates="document")
    audit_logs = relationship("AuditLog", back_populates="document")
    analytics = relationship("AnalyticsRecord", back_populates="document", cascade="all, delete-orphan")
    shares = relationship("SharedDocument", back_populates="document", cascade="all, delete-orphan")
    versions = relationship("DocumentVersion", back_populates="document", cascade="all, delete-orphan", order_by="DocumentVersion.version_number")
    tasks = relationship("Task", back_populates="document", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_document_category_priority', 'category', 'priority'),
        Index('idx_document_created_at', 'created_at'),
        Index('idx_document_status_category', 'status', 'category'),
    )
    
    def __repr__(self):
        return f"<Document {self.filename} ({self.category.value})>"

class DocumentEmbedding(Base):
    __tablename__ = "document_embeddings"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), nullable=False, index=True)
    embedding_model = Column(String(100), nullable=False)
    embedding_vector = Column(LargeBinary, nullable=False)  # Serialized numpy array
    chunk_index = Column(Integer, default=0)
    chunk_text = Column(Text)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    
    # Relationships
    document = relationship("Document", back_populates="embeddings")
    
    __table_args__ = (
        Index('idx_embedding_document_model', 'document_id', 'embedding_model'),
    )

class DataIntegration(Base):
    __tablename__ = "data_integrations"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    integration_type = Column(String(50), nullable=False)  # email, whatsapp, sharepoint, etc.
    config = Column(JSON, nullable=False)  # Integration-specific configuration
    status = Column(Enum(IntegrationStatus), default=IntegrationStatus.INACTIVE)
    last_sync = Column(DateTime(timezone=True))
    last_error = Column(Text)
    documents_synced = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Relationships
    user = relationship("User", back_populates="data_integrations")
    
    def __repr__(self):
        return f"<DataIntegration {self.name} ({self.integration_type})>"


class AuditLog(Base):
    __tablename__ = "audit_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), index=True)
    action = Column(String(100), nullable=False, index=True)
    entity_type = Column(String(50), nullable=False)  # user, document, system, etc.
    entity_id = Column(String(100))
    details = Column(JSON, default={})
    ip_address = Column(String(45))
    user_agent = Column(Text)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False, index=True)
    
    # Relationships
    user = relationship("User", back_populates="audit_logs")
    document = relationship("Document", back_populates="audit_logs")
    
    __table_args__ = (
        Index('idx_audit_action_entity', 'action', 'entity_type'),
        Index('idx_audit_created_at', 'created_at'),
    )
    
    def __repr__(self):
        return f"<AuditLog {self.action} on {self.entity_type}>"

class DocumentVersion(Base):
    __tablename__ = "document_versions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), index=True, nullable=False)
    version_number = Column(Integer, nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer, nullable=False)
    file_hash = Column(String(128), nullable=False)
    modified_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    changes_description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    
    # Relationships
    document = relationship("Document", back_populates="versions")
    modified_by_user = relationship("User", foreign_keys=[modified_by])
    
    __table_args__ = (
        UniqueConstraint('document_id', 'version_number', name='uix_doc_version'),
    )

class SharedDocument(Base):
    __tablename__ = "shared_documents"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), index=True, nullable=False)
    shared_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True, nullable=False)
    shared_with_user = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True)
    shared_with_department = Column(String(100))  # NULL if shared with specific user
    can_edit = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    expires_at = Column(DateTime(timezone=True))
    
    # Relationships
    document = relationship("Document", back_populates="shares")
    shared_by_user = relationship("User", foreign_keys=[shared_by])
    shared_with = relationship("User", foreign_keys=[shared_with_user])
    
    __table_args__ = (
        # Ensure document can't be shared multiple times with same user/department
        UniqueConstraint('document_id', 'shared_with_user', name='uix_shared_doc_user'),
        UniqueConstraint('document_id', 'shared_with_department', name='uix_shared_doc_dept'),
        # Ensure either user or department is set, not both
        CheckConstraint('(shared_with_user IS NULL) != (shared_with_department IS NULL)', 
                       name='chk_shared_with_xor'),
    )
    
    def __repr__(self):
        target = self.shared_with_user if self.shared_with_user else self.shared_with_department
        return f"<SharedDocument doc={self.document_id} with={target}>"

class SystemHealth(Base):
    __tablename__ = "system_health"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_name = Column(String(100), nullable=False, index=True)
    status = Column(String(20), nullable=False)  # healthy, degraded, unhealthy
    response_time_ms = Column(Float)
    memory_usage_mb = Column(Float)
    cpu_usage_percent = Column(Float)
    error_count = Column(Integer, default=0)
    last_error = Column(Text)
    checked_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False, index=True)
    d_metadata = Column(JSON, default={})
    
    __table_args__ = (
        Index('idx_health_service_time', 'service_name', 'checked_at'),
    )

class Analytics(Base):
    __tablename__ = "analytics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    metric_name = Column(String(100), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    dimensions = Column(JSON, default={})  # Additional dimensions for filtering
    timestamp = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False, index=True)
    
    __table_args__ = (
        Index('idx_analytics_metric_time', 'metric_name', 'timestamp'),
    )

class Task(Base):
    __tablename__ = "tasks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String(500), nullable=False)
    description = Column(Text)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), index=True)
    assigned_to = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True, nullable=False)
    assigned_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True)
    priority = Column(Enum(Priority), default=Priority.MEDIUM, index=True)
    status = Column(String(50), default="PENDING", index=True)  # PENDING, IN_PROGRESS, COMPLETED, CANCELLED
    category = Column(String(100), index=True)
    tags = Column(JSON, default=[])
    due_date = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    estimated_hours = Column(Float)
    actual_hours = Column(Float)
    progress_percentage = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Additional metadata
    task_metadata = Column(JSON, default={})
    attachments = Column(JSON, default=[])
    comments = Column(JSON, default=[])
    
    # Relationships
    document = relationship("Document", back_populates="tasks")
    assigned_user = relationship("User", foreign_keys=[assigned_to], back_populates="assigned_tasks")
    assigner_user = relationship("User", foreign_keys=[assigned_by], back_populates="created_tasks")
    
    __table_args__ = (
        Index('idx_task_status_priority', 'status', 'priority'),
        Index('idx_task_assigned_status', 'assigned_to', 'status'),
        Index('idx_task_due_date', 'due_date'),
        Index('idx_task_created_at', 'created_at'),
    )
    
    def __repr__(self):
        return f"<Task {self.title} ({self.status})>"

# Database management
class DatabaseManager:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or get_database_url()
        self.engine = create_engine(
            self.database_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=False
        )
        self.SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
    
    def create_tables(self):
        """Create all database tables"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            raise
    
    def get_session(self) -> Session:
        """Get database session"""
        session = self.SessionLocal()
        try:
            return session
        except Exception:
            session.close()
            raise
    
    def create_admin_user(self, username: str = "admin", email: str = "admin@kmrl.gov.in", 
                         password: str = "MetroAdmin@2024") -> User:
        """Create default admin user"""
        with self.get_session() as session:
            try:
                # Check if admin exists
                admin = session.query(User).filter_by(username=username).first()
                if admin:
                    logger.info(f"Admin user {username} already exists")
                    return admin
                
                # Create admin user
                admin = User(
                    username=username,
                    email=email,
                    first_name="System",
                    last_name="Administrator",
                    department="IT",
                    role=UserRole.ADMIN,
                    status=UserStatus.ACTIVE
                )
                admin.set_password(password)
                
                session.add(admin)
                session.commit()
                session.refresh(admin)
                
                logger.info(f"Admin user {username} created successfully")
                return admin
            
            except Exception as e:
                session.rollback()
                logger.error(f"Error creating admin user: {e}")
                raise
    
    def health_check(self) -> Dict[str, Any]:
        """Check database health"""
        try:
            with self.get_session() as session:
                # Simple query to check connection
                result = session.execute(text("SELECT 1"))
                result.fetchone()
                
                # Get basic stats
                user_count = session.query(User).count()
                document_count = session.query(Document).count()
                
                return {
                    "status": "healthy",
                    "users": user_count,
                    "documents": document_count,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

# Global database manager instance
db_manager = DatabaseManager()

# Dependency to get database session
def get_db() -> Session:
    """FastAPI dependency to get database session"""
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()

# Utility functions
def create_sample_data(session: Session):
    """Create sample data for testing"""
    try:
        # Create sample users
        users = [
            User(
                username="station.controller",
                email="station@kmrl.gov.in",
                first_name="Rajesh",
                last_name="Kumar",
                department="Operations",
                role=UserRole.EMPLOYEE,
                status=UserStatus.ACTIVE
            ),
            User(
                username="finance.manager",
                email="finance@kmrl.gov.in",
                first_name="Priya",
                last_name="Nair",
                department="Finance",
                role=UserRole.MANAGER,
                status=UserStatus.ACTIVE
            ),
            User(
                username="maintenance.head",
                email="maintenance@kmrl.gov.in",
                first_name="Suresh",
                last_name="Menon",
                department="Maintenance",
                role=UserRole.MANAGER,
                status=UserStatus.ACTIVE
            )
        ]
        
        for user in users:
            user.set_password("Metro@123")
        
        session.add_all(users)
        session.commit()
        
        logger.info("Sample data created successfully")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating sample data: {e}")
        raise

if __name__ == "__main__":
    # Initialize database
    logging.basicConfig(level=logging.INFO)
    
    print("Creating database tables...")
    db_manager.create_tables()
    
    print("Creating admin user...")
    admin = db_manager.create_admin_user()
    
    with db_manager.get_session() as session:
        print("Creating sample data...")
        create_sample_data(session)
    
    print("Database initialization completed!")
    health = db_manager.health_check()
    print(f"Database health: {health}")
