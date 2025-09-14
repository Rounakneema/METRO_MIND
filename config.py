"""
MetroMind Configuration
Centralized configuration management with non-default ports
"""
import os
from urllib.parse import quote_plus
from typing import Dict, Any
from dataclasses import dataclass
from dataclasses import field

@dataclass
class DatabaseConfig:
    """Database configuration with non-default ports"""
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))  # Non-default port
    postgres_db: str = os.getenv("POSTGRES_DB", "metromind_db")
    postgres_user: str = os.getenv("POSTGRES_USER", "metromind_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "MetroMind@2025")
    
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6380"))  # Non-default port
    redis_password: str = os.getenv("REDIS_PASSWORD", "MetroRedis@2024")

@dataclass
class ServiceConfig:
    """Service configuration with non-default ports"""
    api_gateway_port: int = int(os.getenv("API_GATEWAY_PORT", "8010"))
    auth_service_port: int = int(os.getenv("AUTH_SERVICE_PORT", "8011"))
    document_service_port: int = int(os.getenv("DOCUMENT_SERVICE_PORT", "8012"))
    ocr_service_port: int = int(os.getenv("OCR_SERVICE_PORT", "8013"))
    ai_ml_service_port: int = int(os.getenv("AI_ML_SERVICE_PORT", "8014"))
    search_service_port: int = int(os.getenv("SEARCH_SERVICE_PORT", "8015"))
    notification_service_port: int = int(os.getenv("NOTIFICATION_SERVICE_PORT", "8016"))
    integration_service_port: int = int(os.getenv("INTEGRATION_SERVICE_PORT", "8017"))
    analytics_service_port: int = int(os.getenv("ANALYTICS_SERVICE_PORT", "8018"))
    
    web_frontend_port: int = int(os.getenv("WEB_FRONTEND_PORT", "3001"))  # Non-default port
    model_downloader_port: int = int(os.getenv("MODEL_DOWNLOADER_PORT", "8019"))

@dataclass
class NotificationConfig:
    """Notification service configuration"""
    websocket_ping_interval: int = 30
    notification_cleanup_days: int = 30
    max_notifications_per_user: int = 100
    enable_email_notifications: bool = True
    enable_web_notifications: bool = True
    enable_sms_notifications: bool = False
    enable_whatsapp_notifications: bool = False

@dataclass
class AnalyticsConfig:
    """Analytics service configuration"""
    retention_days: int = 90
    enable_usage_tracking: bool = True
    enable_performance_metrics: bool = True
    enable_error_tracking: bool = True
    batch_size: int = 100
    flush_interval_seconds: int = 300

@dataclass
class SecurityConfig:
    """Security configuration"""
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "MetroMind_JWT_Secret_2024_KMRL")
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24
    bcrypt_rounds: int = 12
    max_login_attempts: int = 5
    lockout_duration_minutes: int = 30

@dataclass
class AIConfig:
    """AI/ML service configuration"""
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")  # Optional for local models
    huggingface_cache_dir: str = os.getenv("HF_CACHE_DIR", "./models/huggingface")
    vector_db_path: str = os.getenv("VECTOR_DB_PATH", "./data/vector_db")
    max_document_size_mb: int = 50
    supported_languages: list = None
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    sentiment_model: str = os.getenv("SENTIMENT_MODEL", "cardiffnlp/twitter-roberta-base-sentiment-latest")
    summarization_model: str = os.getenv("SUMMARIZATION_MODEL", "facebook/bart-large-cnn")
    
    def __post_init__(self):
        if self.supported_languages is None:
            self.supported_languages = ["en", "ml", "hi", "ta", "kn", "te"]

# (Moved AI_MODEL_CONFIG and vector_search_config definitions below globals)

@dataclass
class IntegrationConfig:
    """External integration configuration"""
    email_imap_server: str = os.getenv("EMAIL_IMAP_SERVER", "")
    email_imap_port: int = int(os.getenv("EMAIL_IMAP_PORT", "993"))
    email_username: str = os.getenv("EMAIL_USERNAME", "")
    email_password: str = os.getenv("EMAIL_PASSWORD", "")
    
    smtp_server: str = os.getenv("SMTP_SERVER", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: str = os.getenv("SMTP_USERNAME", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    
    whatsapp_business_token: str = os.getenv("WHATSAPP_BUSINESS_TOKEN", "")
    sharepoint_client_id: str = os.getenv("SHAREPOINT_CLIENT_ID", "")
    sharepoint_client_secret: str = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
    sharepoint_tenant_id: str = os.getenv("SHAREPOINT_TENANT_ID", "")
    
    # Google Drive configuration
    google_drive_credentials_file: str = os.getenv("GOOGLE_DRIVE_CREDENTIALS_FILE", "./data/google_drive_credentials.json")
    google_drive_token_file: str = os.getenv("GOOGLE_DRIVE_TOKEN_FILE", "./data/google_drive_token.json")

@dataclass
class AppConfig:
    """Main application configuration"""
    debug: bool = os.getenv("DEBUG", "true").lower() == "true"
    testing: bool = os.getenv("TESTING", "false").lower() == "true"
    environment: str = os.getenv("ENVIRONMENT", "development")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    data_directory: str = os.getenv("DATA_DIRECTORY", "./data")
    upload_directory: str = os.getenv("UPLOAD_DIRECTORY", "./data/uploads")
    temp_directory: str = os.getenv("TEMP_DIRECTORY", "./data/temp")
    
    # Create directories if they don't exist
    def __post_init__(self):
        import pathlib
        pathlib.Path(self.data_directory).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.upload_directory).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.temp_directory).mkdir(parents=True, exist_ok=True)

# Global configuration instances
db_config = DatabaseConfig()
service_config = ServiceConfig()
security_config = SecurityConfig()
ai_config = AIConfig()
integration_config = IntegrationConfig()
app_config = AppConfig()
notification_config = NotificationConfig()
analytics_config = AnalyticsConfig()

# Backward-compatible configs for services expecting dict-like configs
# AI model configuration used by model_downloader and ai_ml_service
AI_MODEL_CONFIG: Dict[str, Any] = {
    "sentence_transformer": {
        "name": "sentence_transformer",
        "model_id": getattr(ai_config, "embedding_model", "all-MiniLM-L6-v2"),
        "model_type": "sentence_transformer",
        "version": None,
        "priority": "medium",
        "auto_update": True,
        "fallback_models": [],
        "metadata": {},
    },
    "sentiment": {
        "name": "sentiment",
        "model_id": getattr(ai_config, "sentiment_model", "cardiffnlp/twitter-roberta-base-sentiment-latest"),
        "model_type": "transformers_model",
        "version": None,
        "priority": "low",
        "auto_update": True,
        "fallback_models": [],
        "metadata": {},
    },
    "summarization": {
        "name": "summarization",
        "model_id": getattr(ai_config, "summarization_model", "facebook/bart-large-cnn"),
        "model_type": "transformers_model",
        "version": None,
        "priority": "low",
        "auto_update": True,
        "fallback_models": [],
        "metadata": {},
    },
}

# Vector search configuration expected by search_service
vector_search_config: Dict[str, Any] = {
    "embedding_model": getattr(ai_config, "embedding_model", "all-MiniLM-L6-v2"),
    "vector_db_path": ai_config.vector_db_path,
    "index_type": os.getenv("VECTOR_INDEX_TYPE", "faiss"),
    "use_chromadb": os.getenv("USE_CHROMADB", "false").lower() == "true",
}

def get_database_url() -> str:
    """Get PostgreSQL database URL with proper URL encoding"""
    password = quote_plus(db_config.postgres_password)
    return f"postgresql://{db_config.postgres_user}:{password}@{db_config.postgres_host}:{db_config.postgres_port}/{db_config.postgres_db}"

def get_redis_url() -> str:
    """Get Redis URL"""
    if db_config.redis_password:
        return f"redis://:{db_config.redis_password}@{db_config.redis_host}:{db_config.redis_port}/0"
    return f"redis://{db_config.redis_host}:{db_config.redis_port}/0"

def get_service_urls() -> Dict[str, str]:
    """Get all service URLs"""
    return {
        "api_gateway": f"http://localhost:{service_config.api_gateway_port}",
        "auth_service": f"http://localhost:{service_config.auth_service_port}",
        "document_service": f"http://localhost:{service_config.document_service_port}",
        "ocr_service": f"http://localhost:{service_config.ocr_service_port}",
        "ai_ml_service": f"http://localhost:{service_config.ai_ml_service_port}",
        "search_service": f"http://localhost:{service_config.search_service_port}",
        "notification_service": f"http://localhost:{service_config.notification_service_port}",
        "integration_service": f"http://localhost:{service_config.integration_service_port}",
        "analytics_service": f"http://localhost:{service_config.analytics_service_port}",
        "web_frontend": f"http://localhost:{service_config.web_frontend_port}"
    }

# Environment validation
def validate_config():
    """Validate configuration and warn about missing required settings"""
    warnings = []
    
    if not integration_config.email_username:
        warnings.append("Email integration not configured - set EMAIL_USERNAME and EMAIL_PASSWORD")
    
    if not integration_config.whatsapp_business_token:
        warnings.append("WhatsApp integration not configured - set WHATSAPP_BUSINESS_TOKEN")
    
    if not integration_config.sharepoint_client_id:
        warnings.append("SharePoint integration not configured - set SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET, and SHAREPOINT_TENANT_ID")
    
    if not os.path.exists(integration_config.google_drive_credentials_file):
        warnings.append("Google Drive integration not configured - set GOOGLE_DRIVE_CREDENTIALS_FILE to valid path")
    
    return warnings

def print_config_summary():
    """Print configuration summary"""
    print("=" * 60)
    print("METROMIND CONFIGURATION SUMMARY")
    print("=" * 60)
    print(f"Environment: {app_config.environment}")
    print(f"Debug Mode: {app_config.debug}")
    print(f"Log Level: {app_config.log_level}")
    print()
    print("SERVICE PORTS:")
    for service, url in get_service_urls().items():
        print(f"  {service}: {url}")
    print()
    print("DATABASE:")
    print(f"  PostgreSQL: {db_config.postgres_host}:{db_config.postgres_port}")
    print(f"  Redis: {db_config.redis_host}:{db_config.redis_port}")
    print()
    
    warnings = validate_config()
    if warnings:
        print("CONFIGURATION WARNINGS:")
        for warning in warnings:
            print(f"  ⚠️  {warning}")
        print()
    
    print("=" * 60)

if __name__ == "__main__":
    print_config_summary()
