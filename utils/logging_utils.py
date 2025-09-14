"""
MetroMind Logging Utilities
Centralized logging configuration and utilities
"""

import logging
import logging.handlers
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import app_config

def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Set up a logger with consistent formatting and handlers
    
    Args:
        name: Logger name (usually __name__)
        level: Log level override
        
    Returns:
        Configured logger
    """
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Set level
    log_level = level or app_config.log_level
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Don't add handlers if they already exist
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if not in testing mode)
    if not app_config.testing:
        # Create logs directory
        log_dir = Path(app_config.data_directory) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Service-specific log file
        service_name = name.split('.')[-1] if '.' in name else name
        log_file = log_dir / f"{service_name}.log"
        
        # Rotating file handler (10MB max, keep 5 files)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def log_request(logger: logging.Logger, request_id: str, method: str, 
               url: str, user_id: Optional[str] = None):
    """Log incoming HTTP request"""
    logger.info(f"[{request_id}] {method} {url} - User: {user_id or 'anonymous'}")

def log_response(logger: logging.Logger, request_id: str, status_code: int, 
                response_time_ms: float):
    """Log HTTP response"""
    logger.info(f"[{request_id}] Response: {status_code} - {response_time_ms:.2f}ms")

def log_error(logger: logging.Logger, request_id: str, error: Exception, 
              context: Optional[dict] = None):
    """Log error with context"""
    context_str = f" - Context: {context}" if context else ""
    logger.error(f"[{request_id}] Error: {str(error)}{context_str}", exc_info=True)

class LoggerMixin:
    """Mixin to add logging capabilities to any class"""
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class"""
        if not hasattr(self, '_logger'):
            self._logger = setup_logger(self.__class__.__name__)
        return self._logger

# Global loggers for common use
auth_logger = setup_logger('auth_service')
document_logger = setup_logger('document_service')
ocr_logger = setup_logger('ocr_service')
ai_logger = setup_logger('ai_service')
search_logger = setup_logger('search_service')
notification_logger = setup_logger('notification_service')
integration_logger = setup_logger('integration_service')
analytics_logger = setup_logger('analytics_service')
