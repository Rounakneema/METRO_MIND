"""
MetroMind Model Downloader Service
Automatic downloading and management of AI/ML models with offline support
Handles model versioning, caching, and fallback strategies
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple
import asyncio
import aiohttp
import aiofiles
import hashlib
import json
import os
import shutil
import logging
from pathlib import Path
from enum import Enum
import tarfile
import zipfile
import tempfile

# Model-specific imports
try:
    import huggingface_hub
    from huggingface_hub import hf_hub_download, snapshot_download
    HUGGINGFACE_AVAILABLE = True
except ImportError:
    HUGGINGFACE_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import transformers
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

# Import our config
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import service_config, AI_MODEL_CONFIG
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)

# Model types and status
class ModelType(str, Enum):
    SENTENCE_TRANSFORMER = "sentence_transformer"
    TRANSFORMERS_MODEL = "transformers_model"
    TOKENIZER = "tokenizer"
    OCR_MODEL = "ocr_model"
    CUSTOM_MODEL = "custom_model"
    SPACY_MODEL = "spacy_model"

class ModelStatus(str, Enum):
    NOT_DOWNLOADED = "not_downloaded"
    DOWNLOADING = "downloading"
    AVAILABLE = "available"
    UPDATING = "updating"
    ERROR = "error"
    CORRUPTED = "corrupted"

class DownloadPriority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

# Pydantic models
class ModelConfig(BaseModel):
    name: str
    model_id: str  # HuggingFace model ID or URL
    model_type: ModelType
    version: Optional[str] = None
    priority: DownloadPriority = DownloadPriority.MEDIUM
    auto_update: bool = True
    fallback_models: List[str] = []
    metadata: Dict[str, Any] = {}

class ModelInfo(BaseModel):
    name: str
    model_id: str
    model_type: str
    version: Optional[str]
    status: str
    file_size: int
    download_date: Optional[datetime]
    last_used: Optional[datetime]
    checksum: Optional[str]
    local_path: str
    fallback_available: bool

class DownloadRequest(BaseModel):
    model_name: str
    force_download: bool = False
    check_updates: bool = True

class DownloadStatus(BaseModel):
    model_name: str
    status: str
    progress: float  # 0-100
    downloaded_bytes: int
    total_bytes: int
    eta_seconds: Optional[float]
    error_message: Optional[str]

# FastAPI app
app = FastAPI(
    title="MetroMind Model Downloader",
    description="Automatic AI/ML model downloading and management",
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

# Model configuration
DEFAULT_MODELS = {
    "sentence_transformer": ModelConfig(
        name="sentence_transformer",
        model_id="all-MiniLM-L6-v2",
        model_type=ModelType.SENTENCE_TRANSFORMER,
        priority=DownloadPriority.HIGH,
        auto_update=True,
        fallback_models=["all-mpnet-base-v2", "all-distilroberta-v1"]
    ),
    "sentiment_analyzer": ModelConfig(
        name="sentiment_analyzer",
        model_id="cardiffnlp/twitter-roberta-base-sentiment-latest",
        model_type=ModelType.TRANSFORMERS_MODEL,
        priority=DownloadPriority.MEDIUM,
        auto_update=False,
        fallback_models=["distilbert-base-uncased-finetuned-sst-2-english"]
    ),
    "summarizer": ModelConfig(
        name="summarizer",
        model_id="facebook/bart-large-cnn",
        model_type=ModelType.TRANSFORMERS_MODEL,
        priority=DownloadPriority.MEDIUM,
        auto_update=False,
        fallback_models=["sshleifer/distilbart-cnn-12-6"]
    ),
    "multilingual_embeddings": ModelConfig(
        name="multilingual_embeddings",
        model_id="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        model_type=ModelType.SENTENCE_TRANSFORMER,
        priority=DownloadPriority.LOW,
        auto_update=True,
        fallback_models=["sentence-transformers/distiluse-base-multilingual-cased"]
    )
}

# Model downloader engine
class ModelDownloader:
    """Core model downloading and management engine"""
    
    def __init__(self):
        self.models_dir = Path("models")
        self.models_dir.mkdir(exist_ok=True)
        self.cache_dir = self.models_dir / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        self.model_configs = DEFAULT_MODELS.copy()
        self.download_status = {}
        self.model_registry = {}
        
        # Load model registry
        self.registry_file = self.models_dir / "registry.json"
        self._load_registry()
    
    def _load_registry(self):
        """Load model registry from disk"""
        try:
            if self.registry_file.exists():
                with open(self.registry_file, 'r') as f:
                    registry_data = json.load(f)
                    
                # Convert datetime strings back to datetime objects
                for model_name, info in registry_data.items():
                    if info.get('download_date'):
                        info['download_date'] = datetime.fromisoformat(info['download_date'])
                    if info.get('last_used'):
                        info['last_used'] = datetime.fromisoformat(info['last_used'])
                    
                    self.model_registry[model_name] = info
                
                logger.info(f"Loaded model registry with {len(self.model_registry)} models")
        
        except Exception as e:
            logger.error(f"Failed to load model registry: {e}")
            self.model_registry = {}
    
    def _save_registry(self):
        """Save model registry to disk"""
        try:
            registry_data = {}
            for model_name, info in self.model_registry.items():
                # Convert datetime objects to strings for JSON serialization
                info_copy = info.copy()
                if info_copy.get('download_date'):
                    info_copy['download_date'] = info_copy['download_date'].isoformat()
                if info_copy.get('last_used'):
                    info_copy['last_used'] = info_copy['last_used'].isoformat()
                
                registry_data[model_name] = info_copy
            
            with open(self.registry_file, 'w') as f:
                json.dump(registry_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save model registry: {e}")
    
    async def initialize(self):
        """Initialize the model downloader"""
        logger.info("Initializing model downloader...")
        
        # Check existing models
        await self._scan_existing_models()
        
        # Start background tasks
        asyncio.create_task(self._periodic_update_check())
        asyncio.create_task(self._cleanup_old_models())
        
        logger.info("Model downloader initialized")
    
    async def _scan_existing_models(self):
        """Scan for existing downloaded models"""
        for model_name, config in self.model_configs.items():
            model_path = self._get_model_path(model_name)
            
            if model_path.exists():
                # Model exists, update registry
                model_info = {
                    "name": model_name,
                    "model_id": config.model_id,
                    "model_type": config.model_type.value,
                    "status": ModelStatus.AVAILABLE.value,
                    "local_path": str(model_path),
                    "file_size": self._get_directory_size(model_path),
                    "checksum": await self._calculate_checksum(model_path),
                    "last_used": datetime.now(timezone.utc)
                }
                
                # Preserve existing download date if available
                if model_name in self.model_registry:
                    existing_info = self.model_registry[model_name]
                    model_info["download_date"] = existing_info.get("download_date")
                
                self.model_registry[model_name] = model_info
            else:
                # Model not downloaded yet
                self.model_registry[model_name] = {
                    "name": model_name,
                    "model_id": config.model_id,
                    "model_type": config.model_type.value,
                    "status": ModelStatus.NOT_DOWNLOADED.value,
                    "local_path": str(model_path),
                    "file_size": 0,
                    "checksum": None
                }
        
        self._save_registry()
    
    def _get_model_path(self, model_name: str) -> Path:
        """Get local path for a model"""
        return self.models_dir / model_name
    
    def _get_directory_size(self, directory: Path) -> int:
        """Get total size of directory in bytes"""
        total_size = 0
        try:
            for file_path in directory.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        except Exception as e:
            logger.warning(f"Failed to calculate directory size for {directory}: {e}")
        
        return total_size
    
    async def _calculate_checksum(self, path: Path) -> str:
        """Calculate MD5 checksum of model directory"""
        try:
            hasher = hashlib.md5()
            
            if path.is_file():
                async with aiofiles.open(path, 'rb') as f:
                    async for chunk in f:
                        hasher.update(chunk)
            elif path.is_dir():
                # Calculate checksum of all files in directory
                for file_path in sorted(path.rglob('*')):
                    if file_path.is_file():
                        async with aiofiles.open(file_path, 'rb') as f:
                            async for chunk in f:
                                hasher.update(chunk)
            
            return hasher.hexdigest()
            
        except Exception as e:
            logger.warning(f"Failed to calculate checksum for {path}: {e}")
            return ""
    
    async def download_model(self, model_name: str, force_download: bool = False) -> bool:
        """Download a specific model"""
        if model_name not in self.model_configs:
            logger.error(f"Unknown model: {model_name}")
            return False
        
        config = self.model_configs[model_name]
        model_path = self._get_model_path(model_name)
        
        # Check if already downloaded and not forcing
        if not force_download and model_path.exists() and self._is_model_valid(model_name):
            logger.info(f"Model {model_name} already available")
            return True
        
        # Start download
        self.download_status[model_name] = DownloadStatus(
            model_name=model_name,
            status="starting",
            progress=0.0,
            downloaded_bytes=0,
            total_bytes=0
        )
        
        try:
            logger.info(f"Starting download of model: {model_name}")
            
            # Update registry
            self.model_registry[model_name]["status"] = ModelStatus.DOWNLOADING.value
            self._save_registry()
            
            # Download based on model type
            success = False
            if config.model_type == ModelType.SENTENCE_TRANSFORMER:
                success = await self._download_sentence_transformer(model_name, config)
            elif config.model_type == ModelType.TRANSFORMERS_MODEL:
                success = await self._download_transformers_model(model_name, config)
            elif config.model_type == ModelType.SPACY_MODEL:
                success = await self._download_spacy_model(model_name, config)
            else:
                success = await self._download_generic_model(model_name, config)
            
            if success:
                # Update registry
                self.model_registry[model_name].update({
                    "status": ModelStatus.AVAILABLE.value,
                    "download_date": datetime.now(timezone.utc),
                    "file_size": self._get_directory_size(model_path),
                    "checksum": await self._calculate_checksum(model_path),
                    "last_used": datetime.now(timezone.utc)
                })
                
                self.download_status[model_name].status = "completed"
                self.download_status[model_name].progress = 100.0
                
                logger.info(f"Successfully downloaded model: {model_name}")
            else:
                self.model_registry[model_name]["status"] = ModelStatus.ERROR.value
                self.download_status[model_name].status = "error"
                self.download_status[model_name].error_message = "Download failed"
                
                logger.error(f"Failed to download model: {model_name}")
            
            self._save_registry()
            return success
            
        except Exception as e:
            logger.error(f"Error downloading model {model_name}: {e}")
            self.model_registry[model_name]["status"] = ModelStatus.ERROR.value
            self.download_status[model_name].status = "error"
            self.download_status[model_name].error_message = str(e)
            self._save_registry()
            return False
    
    async def _download_sentence_transformer(self, model_name: str, config: ModelConfig) -> bool:
        """Download SentenceTransformer model"""
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            logger.error("sentence-transformers not available")
            return False
        
        try:
            model_path = self._get_model_path(model_name)
            
            # Remove existing directory if it exists
            if model_path.exists():
                shutil.rmtree(model_path)
            
            # Download model
            model = SentenceTransformer(config.model_id)
            model.save(str(model_path))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to download sentence transformer {model_name}: {e}")
            return False
    
    async def _download_transformers_model(self, model_name: str, config: ModelConfig) -> bool:
        """Download Transformers model"""
        if not TRANSFORMERS_AVAILABLE:
            logger.error("transformers not available")
            return False
        
        try:
            model_path = self._get_model_path(model_name)
            
            # Remove existing directory if it exists
            if model_path.exists():
                shutil.rmtree(model_path)
            
            model_path.mkdir(parents=True)
            
            # Download model and tokenizer
            if HUGGINGFACE_AVAILABLE:
                # Use huggingface_hub for better control
                snapshot_download(
                    repo_id=config.model_id,
                    local_dir=str(model_path),
                    local_dir_use_symlinks=False
                )
            else:
                # Fallback to transformers library
                from transformers import AutoModel, AutoTokenizer
                
                model = AutoModel.from_pretrained(config.model_id)
                tokenizer = AutoTokenizer.from_pretrained(config.model_id)
                
                model.save_pretrained(str(model_path))
                tokenizer.save_pretrained(str(model_path))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to download transformers model {model_name}: {e}")
            return False
    
    async def _download_spacy_model(self, model_name: str, config: ModelConfig) -> bool:
        """Download spaCy model"""
        try:
            # spaCy models are typically installed via pip/conda
            # This is a placeholder for spaCy model downloads
            logger.warning(f"spaCy model download not implemented: {model_name}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to download spaCy model {model_name}: {e}")
            return False
    
    async def _download_generic_model(self, model_name: str, config: ModelConfig) -> bool:
        """Download generic model from URL"""
        try:
            model_path = self._get_model_path(model_name)
            
            # This would implement generic HTTP download
            # For now, return False as not implemented
            logger.warning(f"Generic model download not implemented: {model_name}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to download generic model {model_name}: {e}")
            return False
    
    def _is_model_valid(self, model_name: str) -> bool:
        """Check if downloaded model is valid"""
        if model_name not in self.model_registry:
            return False
        
        info = self.model_registry[model_name]
        model_path = Path(info["local_path"])
        
        # Check if path exists
        if not model_path.exists():
            return False
        
        # Check if not empty
        if model_path.is_dir() and not any(model_path.iterdir()):
            return False
        
        # Check file size
        current_size = self._get_directory_size(model_path)
        if current_size == 0:
            return False
        
        return True
    
    async def get_model_for_use(self, model_name: str) -> Optional[str]:
        """Get model path for use, downloading if necessary"""
        # Check if model is available
        if self._is_model_valid(model_name):
            # Update last used timestamp
            self.model_registry[model_name]["last_used"] = datetime.now(timezone.utc)
            self._save_registry()
            return self.model_registry[model_name]["local_path"]
        
        # Try to download model
        if await self.download_model(model_name):
            return self.model_registry[model_name]["local_path"]
        
        # Try fallback models
        if model_name in self.model_configs:
            config = self.model_configs[model_name]
            for fallback_model in config.fallback_models:
                if await self.download_model(fallback_model):
                    logger.info(f"Using fallback model {fallback_model} for {model_name}")
                    return self.model_registry[fallback_model]["local_path"]
        
        logger.error(f"No available model for {model_name}")
        return None
    
    async def _periodic_update_check(self):
        """Periodically check for model updates"""
        while True:
            try:
                await asyncio.sleep(3600)  # Check every hour
                
                # Check for updates for models with auto_update enabled
                for model_name, config in self.model_configs.items():
                    if config.auto_update and model_name in self.model_registry:
                        info = self.model_registry[model_name]
                        
                        # Check if model was downloaded more than 24 hours ago
                        if info.get("download_date"):
                            time_since_download = datetime.now(timezone.utc) - info["download_date"]
                            if time_since_download > timedelta(hours=24):
                                await self._check_for_updates(model_name)
                
            except Exception as e:
                logger.error(f"Error in periodic update check: {e}")
    
    async def _check_for_updates(self, model_name: str):
        """Check if a model has updates available"""
        # This would implement version checking logic
        # For now, it's a placeholder
        logger.info(f"Checking for updates for model: {model_name}")
    
    async def _cleanup_old_models(self):
        """Clean up old or unused models to save space"""
        while True:
            try:
                await asyncio.sleep(86400)  # Run daily
                
                # Get disk usage
                total_size = sum(
                    info.get("file_size", 0) 
                    for info in self.model_registry.values()
                )
                
                # If total size exceeds threshold, clean up
                max_total_size = 10 * 1024 * 1024 * 1024  # 10GB threshold
                
                if total_size > max_total_size:
                    await self._cleanup_least_used_models()
                
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
    
    async def _cleanup_least_used_models(self):
        """Remove least recently used models"""
        try:
            # Sort models by last used date
            models_by_usage = sorted(
                [(name, info) for name, info in self.model_registry.items()
                 if info.get("status") == ModelStatus.AVAILABLE.value],
                key=lambda x: x[1].get("last_used", datetime.min),
                reverse=False  # Least recently used first
            )
            
            # Remove oldest 20% of models
            models_to_remove = models_by_usage[:len(models_by_usage) // 5]
            
            for model_name, info in models_to_remove:
                # Don't remove high priority models
                if (model_name in self.model_configs and 
                    self.model_configs[model_name].priority == DownloadPriority.HIGH):
                    continue
                
                await self.remove_model(model_name)
                logger.info(f"Cleaned up old model: {model_name}")
            
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")
    
    async def remove_model(self, model_name: str) -> bool:
        """Remove a downloaded model"""
        try:
            if model_name not in self.model_registry:
                return False
            
            info = self.model_registry[model_name]
            model_path = Path(info["local_path"])
            
            if model_path.exists():
                shutil.rmtree(model_path)
            
            # Update registry
            self.model_registry[model_name]["status"] = ModelStatus.NOT_DOWNLOADED.value
            self.model_registry[model_name]["file_size"] = 0
            self.model_registry[model_name]["checksum"] = None
            self.model_registry[model_name]["download_date"] = None
            
            self._save_registry()
            
            logger.info(f"Removed model: {model_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove model {model_name}: {e}")
            return False
    
    def get_model_info(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a model"""
        if model_name not in self.model_registry:
            return None
        
        return self.model_registry[model_name]
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all available models"""
        return list(self.model_registry.values())
    
    def get_download_status(self, model_name: str) -> Optional[DownloadStatus]:
        """Get download status for a model"""
        return self.download_status.get(model_name)

# Global model downloader
model_downloader = ModelDownloader()

@app.on_event("startup")
async def startup_event():
    """Initialize model downloader on startup"""
    await model_downloader.initialize()
    logger.info("Model downloader service started")

# API Endpoints
@app.get("/models")
async def list_models():
    """List all models and their status"""
    models = model_downloader.list_models()
    
    model_info_list = []
    for model_data in models:
        model_info = ModelInfo(
            name=model_data["name"],
            model_id=model_data["model_id"],
            model_type=model_data["model_type"],
            version=model_data.get("version"),
            status=model_data["status"],
            file_size=model_data.get("file_size", 0),
            download_date=model_data.get("download_date"),
            last_used=model_data.get("last_used"),
            checksum=model_data.get("checksum"),
            local_path=model_data["local_path"],
            fallback_available=len(model_downloader.model_configs.get(
                model_data["name"], ModelConfig(name="", model_id="", model_type=ModelType.CUSTOM_MODEL)
            ).fallback_models) > 0
        )
        model_info_list.append(model_info)
    
    return {
        "models": model_info_list,
        "total_models": len(model_info_list),
        "total_size_mb": sum(m.file_size for m in model_info_list) / (1024 * 1024)
    }

@app.get("/models/{model_name}")
async def get_model_info(model_name: str):
    """Get information about a specific model"""
    model_data = model_downloader.get_model_info(model_name)
    
    if not model_data:
        raise HTTPException(status_code=404, detail="Model not found")
    
    return ModelInfo(
        name=model_data["name"],
        model_id=model_data["model_id"],
        model_type=model_data["model_type"],
        version=model_data.get("version"),
        status=model_data["status"],
        file_size=model_data.get("file_size", 0),
        download_date=model_data.get("download_date"),
        last_used=model_data.get("last_used"),
        checksum=model_data.get("checksum"),
        local_path=model_data["local_path"],
        fallback_available=len(model_downloader.model_configs.get(
            model_name, ModelConfig(name="", model_id="", model_type=ModelType.CUSTOM_MODEL)
        ).fallback_models) > 0
    )

@app.post("/models/{model_name}/download")
async def download_model(
    model_name: str,
    background_tasks: BackgroundTasks,
    force_download: bool = False
):
    """Download a specific model"""
    if model_name not in model_downloader.model_configs:
        raise HTTPException(status_code=404, detail="Model not found")
    
    # Start download in background
    background_tasks.add_task(
        model_downloader.download_model,
        model_name,
        force_download
    )
    
    return {
        "success": True,
        "message": f"Download started for model: {model_name}",
        "model_name": model_name
    }

@app.get("/models/{model_name}/download-status")
async def get_download_status(model_name: str):
    """Get download status for a model"""
    status = model_downloader.get_download_status(model_name)
    
    if not status:
        # Check if model exists and is available
        model_info = model_downloader.get_model_info(model_name)
        if model_info and model_info["status"] == ModelStatus.AVAILABLE.value:
            return DownloadStatus(
                model_name=model_name,
                status="completed",
                progress=100.0,
                downloaded_bytes=model_info.get("file_size", 0),
                total_bytes=model_info.get("file_size", 0)
            )
        else:
            return DownloadStatus(
                model_name=model_name,
                status="not_started",
                progress=0.0,
                downloaded_bytes=0,
                total_bytes=0
            )
    
    return status

@app.delete("/models/{model_name}")
async def remove_model(model_name: str):
    """Remove a downloaded model"""
    success = await model_downloader.remove_model(model_name)
    
    if success:
        return {
            "success": True,
            "message": f"Model {model_name} removed successfully"
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to remove model")

@app.post("/models/download-all")
async def download_all_models(background_tasks: BackgroundTasks):
    """Download all configured models"""
    models_to_download = []
    
    for model_name in model_downloader.model_configs.keys():
        model_info = model_downloader.get_model_info(model_name)
        if not model_info or model_info["status"] != ModelStatus.AVAILABLE.value:
            models_to_download.append(model_name)
            background_tasks.add_task(
                model_downloader.download_model,
                model_name,
                False
            )
    
    return {
        "success": True,
        "message": f"Started downloading {len(models_to_download)} models",
        "models": models_to_download
    }

@app.post("/models/{model_name}/use")
async def get_model_for_use(model_name: str):
    """Get model path for use, downloading if necessary"""
    model_path = await model_downloader.get_model_for_use(model_name)
    
    if model_path:
        return {
            "success": True,
            "model_name": model_name,
            "model_path": model_path,
            "status": "available"
        }
    else:
        raise HTTPException(
            status_code=503,
            detail=f"Model {model_name} is not available and could not be downloaded"
        )

@app.get("/storage")
async def get_storage_info():
    """Get storage information"""
    models = model_downloader.list_models()
    
    total_size = sum(model.get("file_size", 0) for model in models)
    available_models = len([m for m in models if m["status"] == ModelStatus.AVAILABLE.value])
    
    # Get disk space info
    statvfs = os.statvfs(model_downloader.models_dir)
    disk_free = statvfs.f_frsize * statvfs.f_bavail
    disk_total = statvfs.f_frsize * statvfs.f_blocks
    
    return {
        "models_total_size_bytes": total_size,
        "models_total_size_mb": total_size / (1024 * 1024),
        "models_total_size_gb": total_size / (1024 * 1024 * 1024),
        "available_models": available_models,
        "total_models": len(models),
        "disk_free_bytes": disk_free,
        "disk_free_gb": disk_free / (1024 * 1024 * 1024),
        "disk_total_gb": disk_total / (1024 * 1024 * 1024),
        "models_directory": str(model_downloader.models_dir)
    }

@app.post("/cleanup")
async def cleanup_models():
    """Manually trigger model cleanup"""
    await model_downloader._cleanup_least_used_models()
    
    return {
        "success": True,
        "message": "Model cleanup completed"
    }

@app.get("/")
async def root():
    """Service root endpoint"""
    return {
        "service": "MetroMind Model Downloader",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "capabilities": {
            "huggingface_available": HUGGINGFACE_AVAILABLE,
            "torch_available": TORCH_AVAILABLE,
            "transformers_available": TRANSFORMERS_AVAILABLE,
            "sentence_transformers_available": SENTENCE_TRANSFORMERS_AVAILABLE
        }
    }

@app.get("/health")
async def health_check():
    """Service health check"""
    try:
        models = model_downloader.list_models()
        available_models = len([m for m in models if m["status"] == ModelStatus.AVAILABLE.value])
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "models_available": available_models,
            "total_models": len(models),
            "storage_directory": str(model_downloader.models_dir),
            "capabilities": {
                "auto_download": True,
                "fallback_support": True,
                "cleanup": True
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
    
    logger.info(f"Starting Model Downloader service on port {service_config.model_downloader_port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=service_config.model_downloader_port,
        log_level="info"
    )
