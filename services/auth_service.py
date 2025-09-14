"""
MetroMind Authentication Service
JWT-based authentication with admin approval workflow
"""
from sqlalchemy import text
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, validator
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
import jwt
import logging
import uuid
from sqlalchemy.orm import Session
import redis
import hashlib
import secrets

# Import our models and config
import sys
import os
from functools import wraps
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import (get_db, User, UserSession, AuditLog, UserRole, UserStatus, 
                     NotificationType, Permission, RolePermission)
from config import security_config, service_config, get_redis_url
from utils.logging_utils import setup_logger
from utils.email_service import EmailService

# Setup
logger = setup_logger(__name__)
app = FastAPI(
    title="MetroMind Authentication Service",
    description="JWT-based authentication with admin approval workflow",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis connection for session management
try:
    redis_client = redis.from_url(get_redis_url(), decode_responses=True)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_client = None

# Initialize role permissions
def initialize_role_permissions(db: Session):
    """Initialize default role permissions"""
    # Clear existing permissions
    db.query(RolePermission).delete()
    
    # Define default permissions per role
    role_permissions = {
        UserRole.ADMIN: [p for p in Permission],  # All permissions
        UserRole.MANAGER: [
            Permission.CREATE_DOCUMENT,
            Permission.READ_DOCUMENT,
            Permission.UPDATE_DOCUMENT,
            Permission.DELETE_DOCUMENT,
            Permission.SHARE_DOCUMENT,
            Permission.READ_USER,
            Permission.UPDATE_USER,
            Permission.MANAGE_DEPARTMENT,
            Permission.VIEW_DEPARTMENT_STATS,
        ],
        UserRole.SUPERVISOR: [
            Permission.CREATE_DOCUMENT,
            Permission.READ_DOCUMENT,
            Permission.UPDATE_DOCUMENT,
            Permission.SHARE_DOCUMENT,
            Permission.READ_USER,
            Permission.VIEW_DEPARTMENT_STATS,
        ],
        UserRole.EMPLOYEE: [
            Permission.CREATE_DOCUMENT,
            Permission.READ_DOCUMENT,
            Permission.UPDATE_DOCUMENT,
        ],
        UserRole.AUDITOR: [
            Permission.READ_DOCUMENT,
            Permission.READ_USER,
            Permission.VIEW_AUDIT_LOGS,
            Permission.VIEW_DEPARTMENT_STATS,
        ]
    }
    
    # Create permission entries
    for role, permissions in role_permissions.items():
        for permission in permissions:
            role_permission = RolePermission(role=role, permission=permission)
            db.add(role_permission)
    
    try:
        db.commit()
        logger.info("Role permissions initialized successfully")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to initialize role permissions: {e}")
        raise

# Security
security = HTTPBearer()
email_service = EmailService()

# Permission management
def get_role_permissions(db: Session, role: UserRole) -> List[Permission]:
    """Get list of permissions for a role"""
    permissions = db.query(RolePermission.permission).filter(RolePermission.role == role).all()
    return [p[0] for p in permissions]

def has_permission(db: Session, user: User, required_permission: Permission) -> bool:
    """Check if user has the required permission"""
    if user.role == UserRole.ADMIN:
        return True
        
    permissions = get_role_permissions(db, user.role)
    return required_permission in permissions

def require_permission(required_permission: Permission):
    """Decorator to check if user has required permission"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get('request')
            db = kwargs.get('db')
            current_user = kwargs.get('current_user')
            
            if not current_user or not db:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Not authenticated"
                )
                
            if not has_permission(db, current_user, required_permission):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission denied: {required_permission.value} required"
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def require_department_access(func):
    """Decorator to check if user has access to department"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request = kwargs.get('request')
        db = kwargs.get('db')
        current_user = kwargs.get('current_user')
        department = kwargs.get('department')
        
        if not current_user or not db:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated"
            )
            
        # Admin has access to all departments
        if current_user.role == UserRole.ADMIN:
            return await func(*args, **kwargs)
            
        # Manager can only access their department
        if current_user.role == UserRole.MANAGER and current_user.department != department:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this department"
            )
            
        # Employees can only access their own department's data
        if current_user.role == UserRole.EMPLOYEE and current_user.department != department:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this department"
            )
            
        return await func(*args, **kwargs)
    return wrapper

# Pydantic models
class RolePermissionUpdate(BaseModel):
    role: UserRole
    permissions: List[Permission]

class UserRegistration(BaseModel):
    username: str
    email: EmailStr
    password: str
    first_name: str
    last_name: str
    department: str
    phone: Optional[str] = None
    
    @validator('username')
    def validate_username(cls, v):
        if len(v) < 3 or len(v) > 50:
            raise ValueError('Username must be between 3 and 50 characters')
        if not v.replace('.', '').replace('_', '').replace('-', '').isalnum():
            raise ValueError('Username can only contain letters, numbers, dots, underscores, and hyphens')
        return v.lower()
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        if not any(c in '!@#$%^&*()_+-=[]{}|;:,.<>?' for c in v):
            raise ValueError('Password must contain at least one special character')
        return v
    
    @validator('department')
    def validate_department(cls, v):
        allowed_departments = [
            'Operations', 'Maintenance', 'Finance', 'HR', 'Safety', 
            'Legal', 'IT', 'Security', 'Customer Service', 'Engineering'
        ]
        if v not in allowed_departments:
            raise ValueError(f'Department must be one of: {", ".join(allowed_departments)}')
        return v

class UserLogin(BaseModel):
    username: str
    password: str
    remember_me: bool = False

class UserApproval(BaseModel):
    user_id: str
    approved: bool
    role: Optional[UserRole] = UserRole.EMPLOYEE
    notes: Optional[str] = None

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: Dict[str, Any]

class UserProfile(BaseModel):
    id: str
    username: str
    email: str
    first_name: str
    last_name: str
    department: str
    role: str
    status: str
    phone: Optional[str]
    created_at: datetime
    last_login: Optional[datetime]
    preferences: Dict[str, Any] = {}

class PasswordChange(BaseModel):
    current_password: str
    new_password: str
    
    @validator('new_password')
    def validate_new_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        if not any(c in '!@#$%^&*()_+-=[]{}|;:,.<>?' for c in v):
            raise ValueError('Password must contain at least one special character')
        return v

class PasswordReset(BaseModel):
    email: EmailStr

class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str

# Utility functions
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(hours=security_config.jwt_expiration_hours)
    
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, security_config.jwt_secret_key, algorithm=security_config.jwt_algorithm)
    return encoded_jwt

def create_refresh_token(data: dict):
    """Create JWT refresh token"""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=30)  # Refresh tokens last 30 days
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, security_config.jwt_secret_key, algorithm=security_config.jwt_algorithm)
    return encoded_jwt

def verify_token(token: str, token_type: str = "access") -> Dict[str, Any]:
    """Verify JWT token"""
    try:
        payload = jwt.decode(token, security_config.jwt_secret_key, algorithms=[security_config.jwt_algorithm])
        
        if payload.get("type") != token_type:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type"
            )
        
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)) -> User:
    """Get current user from JWT token"""
    payload = verify_token(credentials.credentials)
    user_id = payload.get("sub")
    
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload"
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    
    if user.status != UserStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"User account is {user.status.value}"
        )
    
    if user.is_locked():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is locked"
        )
    
    return user

def get_admin_user(current_user: User = Depends(get_current_user)) -> User:
    """Require admin role"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user

def log_audit_event(db: Session, user_id: Optional[str], action: str, entity_type: str, 
                   entity_id: Optional[str] = None, details: Optional[Dict] = None,
                   ip_address: Optional[str] = None, user_agent: Optional[str] = None):
    """Log audit event"""
    try:
        audit_log = AuditLog(
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent
        )
        db.add(audit_log)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log audit event: {e}")

def handle_failed_login(db: Session, user: User, ip_address: str):
    """Handle failed login attempt"""
    user.login_attempts += 1
    
    if user.login_attempts >= security_config.max_login_attempts:
        lockout_until = datetime.now(timezone.utc) + timedelta(minutes=security_config.lockout_duration_minutes)
        user.locked_until = lockout_until
        logger.warning(f"User {user.username} locked until {lockout_until}")
    
    db.commit()
    
    log_audit_event(
        db, str(user.id), "login_failed", "user", str(user.id),
        {"attempts": user.login_attempts}, ip_address
    )

def create_user_session(db: Session, user: User, remember_me: bool, ip_address: str, user_agent: str) -> UserSession:
    """Create user session"""
    expires_delta = timedelta(days=30) if remember_me else timedelta(hours=security_config.jwt_expiration_hours)
    expires_at = datetime.now(timezone.utc) + expires_delta
    
    session = UserSession(
        user_id=user.id,
        session_token=secrets.token_urlsafe(32),
        refresh_token=secrets.token_urlsafe(32),
        expires_at=expires_at,
        ip_address=ip_address,
        user_agent=user_agent
    )
    
    db.add(session)
    db.commit()
    db.refresh(session)
    
    return session

# API Endpoints
@app.post("/register", response_model=Dict[str, str])
async def register_user(user_data: UserRegistration, request: Request, db: Session = Depends(get_db)):
    """Register new user - requires admin approval"""
    
    # Check if username or email already exists
    existing_user = db.query(User).filter(
        (User.username == user_data.username) | (User.email == user_data.email)
    ).first()
    
    if existing_user:
        if existing_user.username == user_data.username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
    
    # Create user with pending status
    user = User(
        username=user_data.username,
        email=user_data.email,
        first_name=user_data.first_name,
        last_name=user_data.last_name,
        department=user_data.department,
        phone=user_data.phone,
        role=UserRole.EMPLOYEE,
        status=UserStatus.PENDING
    )
    user.set_password(user_data.password)
    
    db.add(user)
    db.commit()
    db.refresh(user)
    
    # Log registration
    log_audit_event(
        db, str(user.id), "user_registered", "user", str(user.id),
        {"department": user_data.department}, 
        request.client.host, request.headers.get("user-agent")
    )
    
    # Notify admins about new user registration
    try:
        await notify_admins_new_registration(db, user)
    except Exception as e:
        logger.error(f"Failed to notify admins: {e}")
    
    logger.info(f"New user registered: {user.username}")
    
    return {
        "message": "Registration successful. Your account is pending admin approval.",
        "user_id": str(user.id)
    }

@app.post("/login", response_model=TokenResponse)
async def login_user(login_data: UserLogin, request: Request, db: Session = Depends(get_db)):
    """User login"""
    
    user = db.query(User).filter(User.username == login_data.username).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    if user.is_locked():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is locked. Please contact administrator."
        )
    
    if not user.verify_password(login_data.password):
        handle_failed_login(db, user, request.client.host)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    if user.status == UserStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is pending approval"
        )
    elif user.status != UserStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Account is {user.status.value}"
        )
    
    # Reset failed login attempts
    user.login_attempts = 0
    user.locked_until = None
    user.last_login = datetime.now(timezone.utc)
    db.commit()
    
    # Create session
    session = create_user_session(
        db, user, login_data.remember_me, 
        request.client.host, request.headers.get("user-agent", "")
    )
    
    # Create tokens
    token_data = {"sub": str(user.id), "username": user.username, "role": user.role.value}
    access_token = create_access_token(token_data)
    refresh_token = create_refresh_token(token_data)
    
    # Store in Redis if available
    if redis_client:
        try:
            redis_client.setex(f"session:{session.session_token}", 
                             int((session.expires_at - datetime.now(timezone.utc)).total_seconds()),
                             str(user.id))
        except Exception as e:
            logger.error(f"Failed to store session in Redis: {e}")
    
    # Log successful login
    log_audit_event(
        db, str(user.id), "login_success", "user", str(user.id),
        {"remember_me": login_data.remember_me}, 
        request.client.host, request.headers.get("user-agent")
    )
    
    logger.info(f"User logged in: {user.username}")
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=security_config.jwt_expiration_hours * 3600,
        user={
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "department": user.department,
            "role": user.role.value,
            "status": user.status.value
        }
    )

# Role permission management endpoints
@app.post("/admin/permissions/initialize")
@require_permission(Permission.MANAGE_SYSTEM)
async def init_role_permissions(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Initialize default role permissions"""
    try:
        initialize_role_permissions(db)
        return {"message": "Role permissions initialized successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/admin/permissions/{role}")
@require_permission(Permission.MANAGE_SYSTEM)
async def get_role_permissions_endpoint(
    role: UserRole,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get permissions for a specific role"""
    permissions = get_role_permissions(db, role)
    return {"role": role.value, "permissions": [p.value for p in permissions]}

@app.post("/admin/permissions/update")
@require_permission(Permission.MANAGE_SYSTEM)
async def update_role_permissions(
    permission_update: RolePermissionUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update permissions for a role"""
    try:
        # Delete existing permissions for the role
        db.query(RolePermission).filter(RolePermission.role == permission_update.role).delete()
        
        # Add new permissions
        for permission in permission_update.permissions:
            role_permission = RolePermission(role=permission_update.role, permission=permission)
            db.add(role_permission)
        
        db.commit()
        return {"message": f"Permissions updated for role {permission_update.role.value}"}
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/logout")
async def logout_user(request: Request, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """User logout"""
    
    # Get authorization header to extract token
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:]
        
        # Invalidate session in database
        try:
            payload = verify_token(token)
            sessions = db.query(UserSession).filter(
                UserSession.user_id == current_user.id,
                UserSession.is_active == True
            ).all()
            
            for session in sessions:
                session.is_active = False
                
                # Remove from Redis
                if redis_client:
                    try:
                        redis_client.delete(f"session:{session.session_token}")
                    except Exception as e:
                        logger.error(f"Failed to remove session from Redis: {e}")
            
            db.commit()
        except Exception as e:
            logger.error(f"Error during logout: {e}")
    
    # Log logout
    log_audit_event(
        db, str(current_user.id), "logout", "user", str(current_user.id),
        {}, request.client.host, request.headers.get("user-agent")
    )
    
    logger.info(f"User logged out: {current_user.username}")
    
    return {"message": "Logged out successfully"}

@app.get("/profile", response_model=UserProfile)
async def get_user_profile(current_user: User = Depends(get_current_user)):
    """Get current user profile"""
    return UserProfile(
        id=str(current_user.id),
        username=current_user.username,
        email=current_user.email,
        first_name=current_user.first_name,
        last_name=current_user.last_name,
        department=current_user.department,
        role=current_user.role.value,
        status=current_user.status.value,
        phone=current_user.phone,
        created_at=current_user.created_at,
        last_login=current_user.last_login,
        preferences=current_user.preferences
    )

@app.put("/profile", response_model=UserProfile)
async def update_user_profile(
    profile_data: Dict[str, Any], 
    current_user: User = Depends(get_current_user), 
    db: Session = Depends(get_db)
):
    """Update user profile"""
    
    updatable_fields = ['first_name', 'last_name', 'phone', 'preferences']
    
    for field, value in profile_data.items():
        if field in updatable_fields and hasattr(current_user, field):
            setattr(current_user, field, value)
    
    db.commit()
    db.refresh(current_user)
    
    return UserProfile(
        id=str(current_user.id),
        username=current_user.username,
        email=current_user.email,
        first_name=current_user.first_name,
        last_name=current_user.last_name,
        department=current_user.department,
        role=current_user.role.value,
        status=current_user.status.value,
        phone=current_user.phone,
        created_at=current_user.created_at,
        last_login=current_user.last_login,
        preferences=current_user.preferences
    )

@app.post("/change-password")
async def change_password(
    password_data: PasswordChange,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Change user password"""
    
    if not current_user.verify_password(password_data.current_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    
    current_user.set_password(password_data.new_password)
    db.commit()
    
    # Invalidate all sessions
    sessions = db.query(UserSession).filter(
        UserSession.user_id == current_user.id,
        UserSession.is_active == True
    ).all()
    
    for session in sessions:
        session.is_active = False
    
    db.commit()
    
    logger.info(f"Password changed for user: {current_user.username}")
    
    return {"message": "Password changed successfully. Please log in again."}

# Admin endpoints
@app.get("/admin/pending-users")
async def get_pending_users(admin_user: User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """Get users pending approval"""
    
    pending_users = db.query(User).filter(User.status == UserStatus.PENDING).all()
    
    return [
        {
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "department": user.department,
            "created_at": user.created_at.isoformat(),
            "phone": user.phone
        }
        for user in pending_users
    ]

@app.post("/admin/approve-user")
async def approve_user(
    approval_data: UserApproval,
    admin_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Approve or reject user registration"""
    
    user = db.query(User).filter(User.id == approval_data.user_id).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if user.status != UserStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is not in pending status"
        )
    
    if approval_data.approved:
        user.status = UserStatus.ACTIVE
        user.role = approval_data.role
        message = "User approved successfully"
        
        # Send approval email
        try:
            await email_service.send_approval_email(user.email, user.first_name, True)
        except Exception as e:
            logger.error(f"Failed to send approval email: {e}")
    else:
        user.status = UserStatus.DEACTIVATED
        message = "User registration rejected"
        
        # Send rejection email
        try:
            await email_service.send_approval_email(user.email, user.first_name, False, approval_data.notes)
        except Exception as e:
            logger.error(f"Failed to send rejection email: {e}")
    
    db.commit()
    
    # Log approval action
    log_audit_event(
        db, str(admin_user.id), "user_approval", "user", str(user.id),
        {
            "approved": approval_data.approved,
            "role": approval_data.role.value if approval_data.role else None,
            "notes": approval_data.notes
        }
    )
    
    logger.info(f"User {user.username} {'approved' if approval_data.approved else 'rejected'} by {admin_user.username}")
    
    return {"message": message}

@app.get("/admin/users")
async def get_all_users(
    skip: int = 0,
    limit: int = 100,
    department: Optional[str] = None,
    role: Optional[UserRole] = None,
    status: Optional[UserStatus] = None,
    admin_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Get all users with filtering"""
    
    query = db.query(User)
    
    if department:
        query = query.filter(User.department == department)
    if role:
        query = query.filter(User.role == role)
    if status:
        query = query.filter(User.status == status)
    
    users = query.offset(skip).limit(limit).all()
    total = query.count()
    
    return {
        "users": [
            {
                "id": str(user.id),
                "username": user.username,
                "email": user.email,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "department": user.department,
                "role": user.role.value,
                "status": user.status.value,
                "created_at": user.created_at.isoformat(),
                "last_login": user.last_login.isoformat() if user.last_login else None
            }
            for user in users
        ],
        "total": total,
        "skip": skip,
        "limit": limit
    }

@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    
    try:
        # Check database
        db.execute(text("SELECT 1"))
        
        # Check Redis
        redis_status = "unknown"
        if redis_client:
            try:
                redis_client.ping()
                redis_status = "healthy"
            except:
                redis_status = "unhealthy"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": "healthy",
            "redis": redis_status,
            "version": "1.0.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }

# Helper functions for notifications
async def notify_admins_new_registration(db: Session, user: User):
    """Notify admins about new user registration"""
    
    admins = db.query(User).filter(
        User.role == UserRole.ADMIN,
        User.status == UserStatus.ACTIVE
    ).all()
    
    for admin in admins:
        try:
            await email_service.send_admin_notification(
                admin.email,
                f"New User Registration - {user.first_name} {user.last_name}",
                f"A new user {user.username} from {user.department} department has registered and is pending approval."
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin.username}: {e}")

if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting Authentication Service on port {service_config.auth_service_port}")
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=service_config.auth_service_port,
        log_level="info"
    )
