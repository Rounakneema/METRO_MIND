"""
MetroMind OCR Service
Multi-language OCR and text extraction from images and documents
"""

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import os
import cv2
import numpy as np
from PIL import Image, ImageEnhance, ImageFilter
import asyncio
import tempfile
from pathlib import Path
import logging
import uuid

# OCR libraries
import pytesseract
try:
    import easyocr
    EASYOCR_AVAILABLE = True
except ImportError:
    EASYOCR_AVAILABLE = False
    print("EasyOCR not available - falling back to Tesseract only")

# Import our models and config
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Document, User
from config import service_config, ai_config
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)
app = FastAPI(
    title="MetroMind OCR Service",
    description="Multi-language OCR and text extraction",
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

# Initialize EasyOCR reader if available
EASYOCR_READER = None
if EASYOCR_AVAILABLE:
    try:
        # Initialize with supported languages
        EASYOCR_READER = easyocr.Reader(['en', 'hi', 'ml', 'ta', 'kn', 'te'], gpu=False)
        logger.info("EasyOCR initialized with multi-language support")
    except Exception as e:
        logger.warning(f"Failed to initialize EasyOCR: {e}")
        EASYOCR_READER = None

# Pydantic models
class OCRRequest(BaseModel):
    languages: List[str] = ['en', 'ml']  # Default to English and Malayalam
    preprocess: bool = True
    confidence_threshold: float = 0.5
    output_format: str = 'text'  # 'text' or 'structured'

class OCRResult(BaseModel):
    success: bool
    extracted_text: str
    confidence_score: float
    language_detected: Optional[str]
    processing_time_ms: float
    word_count: int
    lines: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}

class DocumentOCRRequest(BaseModel):
    document_id: str
    languages: List[str] = ['en', 'ml']
    preprocess: bool = True
    pages: Optional[List[int]] = None  # Specific pages to process

# Configuration
SUPPORTED_LANGUAGES = {
    'en': 'eng',        # English
    'ml': 'mal',        # Malayalam
    'hi': 'hin',        # Hindi
    'ta': 'tam',        # Tamil
    'kn': 'kan',        # Kannada
    'te': 'tel',        # Telugu
    'bn': 'ben',        # Bengali
    'gu': 'guj',        # Gujarati
    'mr': 'mar',        # Marathi
    'pa': 'pan',        # Punjabi
    'or': 'ori',        # Oriya
    'as': 'asm',        # Assamese
}

ALLOWED_IMAGE_FORMATS = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif', '.gif', '.webp'}
MAX_IMAGE_SIZE_MB = 20

class OCRProcessor:
    """Main OCR processing class"""
    
    def __init__(self):
        self.tesseract_available = self._check_tesseract()
        self.easyocr_available = EASYOCR_AVAILABLE and EASYOCR_READER is not None
        
        # Configure Tesseract path if needed (Windows)
        if os.name == 'nt':  # Windows
            tesseract_paths = [
                r'C:\Program Files\Tesseract-OCR\tesseract.exe',
                r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
                r'C:\Users\Asus\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
            ]
            for path in tesseract_paths:
                if os.path.exists(path):
                    pytesseract.pytesseract.tesseract_cmd = path
                    break
        
        logger.info(f"OCR Processor initialized - Tesseract: {self.tesseract_available}, EasyOCR: {self.easyocr_available}")
    
    def _check_tesseract(self) -> bool:
        """Check if Tesseract is available"""
        try:
            pytesseract.get_tesseract_version()
            return True
        except Exception as e:
            logger.warning(f"Tesseract not available: {e}")
            return False
    
    async def preprocess_image(self, image: np.ndarray, enhance: bool = True) -> np.ndarray:
        """Preprocess image for better OCR results"""
        try:
            # Convert to PIL Image for enhancement
            pil_image = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
            
            if enhance:
                # Enhance contrast
                enhancer = ImageEnhance.Contrast(pil_image)
                pil_image = enhancer.enhance(1.5)
                
                # Enhance sharpness
                enhancer = ImageEnhance.Sharpness(pil_image)
                pil_image = enhancer.enhance(1.2)
                
                # Apply slight blur to reduce noise
                pil_image = pil_image.filter(ImageFilter.MedianFilter())
            
            # Convert back to OpenCV format
            processed = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
            
            # Convert to grayscale
            if len(processed.shape) == 3:
                processed = cv2.cvtColor(processed, cv2.COLOR_BGR2GRAY)
            
            # Apply adaptive thresholding
            processed = cv2.adaptiveThreshold(
                processed, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
            )
            
            # Morphological operations to clean up
            kernel = np.ones((1, 1), np.uint8)
            processed = cv2.morphologyEx(processed, cv2.MORPH_CLOSE, kernel)
            processed = cv2.morphologyEx(processed, cv2.MORPH_OPEN, kernel)
            
            return processed
            
        except Exception as e:
            logger.error(f"Image preprocessing failed: {e}")
            # Return original image as grayscale if preprocessing fails
            if len(image.shape) == 3:
                return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            return image
    
    async def extract_text_tesseract(self, image: np.ndarray, languages: List[str], 
                                   config: str = '') -> Tuple[str, float, List[Dict]]:
        """Extract text using Tesseract OCR"""
        if not self.tesseract_available:
            raise Exception("Tesseract OCR is not available")
        
        try:
            # Convert language codes
            tesseract_langs = []
            for lang in languages:
                if lang in SUPPORTED_LANGUAGES:
                    tesseract_langs.append(SUPPORTED_LANGUAGES[lang])
                else:
                    tesseract_langs.append('eng')  # Default to English
            
            lang_string = '+'.join(tesseract_langs)
            
            # Default config for better results
            if not config:
                config = '--oem 3 --psm 6'
            
            # Extract text
            text = pytesseract.image_to_string(
                image, 
                lang=lang_string, 
                config=config
            ).strip()
            
            # Get detailed data for confidence scores
            data = pytesseract.image_to_data(
                image, 
                lang=lang_string, 
                config=config,
                output_type=pytesseract.Output.DICT
            )
            
            # Calculate average confidence
            confidences = [int(conf) for conf in data['conf'] if int(conf) > 0]
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0
            
            # Extract line-level information
            lines = []
            current_line = []
            current_line_num = -1
            
            for i in range(len(data['text'])):
                if int(data['conf'][i]) > 0:
                    word_info = {
                        'text': data['text'][i],
                        'confidence': int(data['conf'][i]),
                        'bbox': {
                            'left': int(data['left'][i]),
                            'top': int(data['top'][i]),
                            'width': int(data['width'][i]),
                            'height': int(data['height'][i])
                        }
                    }
                    
                    if data['line_num'][i] != current_line_num:
                        if current_line:
                            lines.append({
                                'line_text': ' '.join([w['text'] for w in current_line]),
                                'words': current_line,
                                'avg_confidence': sum([w['confidence'] for w in current_line]) / len(current_line)
                            })
                        current_line = [word_info]
                        current_line_num = data['line_num'][i]
                    else:
                        current_line.append(word_info)
            
            # Add the last line
            if current_line:
                lines.append({
                    'line_text': ' '.join([w['text'] for w in current_line]),
                    'words': current_line,
                    'avg_confidence': sum([w['confidence'] for w in current_line]) / len(current_line)
                })
            
            return text, avg_confidence / 100.0, lines
            
        except Exception as e:
            logger.error(f"Tesseract OCR failed: {e}")
            raise
    
    async def extract_text_easyocr(self, image: np.ndarray, languages: List[str]) -> Tuple[str, float, List[Dict]]:
        """Extract text using EasyOCR"""
        if not self.easyocr_available:
            raise Exception("EasyOCR is not available")
        
        try:
            # EasyOCR expects language codes in its format
            easyocr_langs = []
            for lang in languages:
                if lang in ['en', 'hi', 'ml', 'ta', 'kn', 'te']:  # Supported by our EasyOCR reader
                    easyocr_langs.append(lang)
            
            if not easyocr_langs:
                easyocr_langs = ['en']  # Default to English
            
            # Perform OCR
            results = EASYOCR_READER.readtext(image, detail=1)
            
            # Process results
            text_parts = []
            lines = []
            total_confidence = 0
            
            for (bbox, text, confidence) in results:
                if confidence > 0.3:  # Filter low-confidence results
                    text_parts.append(text)
                    total_confidence += confidence
                    
                    # Convert bbox to our format
                    x_coords = [point[0] for point in bbox]
                    y_coords = [point[1] for point in bbox]
                    
                    lines.append({
                        'line_text': text,
                        'words': [{
                            'text': text,
                            'confidence': int(confidence * 100),
                            'bbox': {
                                'left': int(min(x_coords)),
                                'top': int(min(y_coords)),
                                'width': int(max(x_coords) - min(x_coords)),
                                'height': int(max(y_coords) - min(y_coords))
                            }
                        }],
                        'avg_confidence': confidence * 100
                    })
            
            extracted_text = '\n'.join(text_parts)
            avg_confidence = total_confidence / len(results) if results else 0
            
            return extracted_text, avg_confidence, lines
            
        except Exception as e:
            logger.error(f"EasyOCR failed: {e}")
            raise
    
    async def extract_text_hybrid(self, image: np.ndarray, languages: List[str]) -> Tuple[str, float, List[Dict]]:
        """Use both Tesseract and EasyOCR and combine results"""
        tesseract_text = tesseract_conf = tesseract_lines = ""
        easyocr_text = easyocr_conf = easyocr_lines = ""
        
        # Try Tesseract first
        if self.tesseract_available:
            try:
                tesseract_text, tesseract_conf, tesseract_lines = await self.extract_text_tesseract(image, languages)
            except Exception as e:
                logger.warning(f"Tesseract failed: {e}")
        
        # Try EasyOCR
        if self.easyocr_available:
            try:
                easyocr_text, easyocr_conf, easyocr_lines = await self.extract_text_easyocr(image, languages)
            except Exception as e:
                logger.warning(f"EasyOCR failed: {e}")
        
        # Choose the better result or combine
        if tesseract_conf > easyocr_conf and len(tesseract_text.strip()) > len(easyocr_text.strip()) / 2:
            return tesseract_text, tesseract_conf, tesseract_lines
        elif easyocr_text:
            return easyocr_text, easyocr_conf, easyocr_lines
        elif tesseract_text:
            return tesseract_text, tesseract_conf, tesseract_lines
        else:
            return "No text detected", 0.0, []
    
    def detect_language(self, text: str) -> Optional[str]:
        """Detect primary language in extracted text"""
        try:
            # Simple character-based detection
            if not text.strip():
                return None
            
            # Count different script characters
            latin_count = sum(1 for c in text if ord(c) < 256)
            devanagari_count = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)
            malayalam_count = sum(1 for c in text if 0x0D00 <= ord(c) <= 0x0D7F)
            tamil_count = sum(1 for c in text if 0x0B80 <= ord(c) <= 0x0BFF)
            kannada_count = sum(1 for c in text if 0x0C80 <= ord(c) <= 0x0CFF)
            telugu_count = sum(1 for c in text if 0x0C00 <= ord(c) <= 0x0C7F)
            
            # Determine primary language
            max_count = max(latin_count, devanagari_count, malayalam_count, tamil_count, kannada_count, telugu_count)
            
            if max_count == malayalam_count and malayalam_count > 0:
                return 'ml'
            elif max_count == tamil_count and tamil_count > 0:
                return 'ta'
            elif max_count == kannada_count and kannada_count > 0:
                return 'kn'
            elif max_count == telugu_count and telugu_count > 0:
                return 'te'
            elif max_count == devanagari_count and devanagari_count > 0:
                return 'hi'
            else:
                return 'en'
                
        except Exception as e:
            logger.warning(f"Language detection failed: {e}")
            return None

# Initialize global OCR processor
ocr_processor = OCRProcessor()

# Utility functions
def validate_image_file(file: UploadFile) -> tuple[bool, str]:
    """Validate uploaded image file"""
    # Check file extension
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in ALLOWED_IMAGE_FORMATS:
        return False, f"File type {file_ext} not supported"
    
    # Check file size (approximate)
    if hasattr(file, 'size') and file.size > MAX_IMAGE_SIZE_MB * 1024 * 1024:
        return False, f"File size exceeds maximum limit of {MAX_IMAGE_SIZE_MB}MB"
    
    return True, "Valid"

async def load_image_from_upload(file: UploadFile) -> np.ndarray:
    """Load image from uploaded file"""
    try:
        # Read file content
        content = await file.read()
        
        # Convert to numpy array
        np_array = np.frombuffer(content, np.uint8)
        
        # Decode image
        image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)
        
        if image is None:
            raise Exception("Failed to decode image")
        
        return image
        
    except Exception as e:
        logger.error(f"Error loading image: {e}")
        raise

# API Endpoints
@app.post("/extract-text", response_model=OCRResult)
async def extract_text_from_image(
    file: UploadFile = File(...),
    languages: str = Form('["en", "ml"]'),  # JSON string
    preprocess: bool = Form(True),
    confidence_threshold: float = Form(0.5),
    output_format: str = Form('text'),
    method: str = Form('hybrid')  # 'tesseract', 'easyocr', or 'hybrid'
):
    """Extract text from uploaded image"""
    
    start_time = datetime.now(timezone.utc)
    
    # Validate file
    is_valid, error_msg = validate_image_file(file)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error_msg)
    
    try:
        # Parse languages
        import json
        try:
            language_list = json.loads(languages)
        except:
            language_list = ['en', 'ml']
        
        # Load and preprocess image
        image = await load_image_from_upload(file)
        
        if preprocess:
            image = await ocr_processor.preprocess_image(image, enhance=True)
        
        # Extract text based on method
        if method == 'tesseract' and ocr_processor.tesseract_available:
            text, confidence, lines = await ocr_processor.extract_text_tesseract(image, language_list)
        elif method == 'easyocr' and ocr_processor.easyocr_available:
            text, confidence, lines = await ocr_processor.extract_text_easyocr(image, language_list)
        else:  # hybrid or fallback
            text, confidence, lines = await ocr_processor.extract_text_hybrid(image, language_list)
        
        # Filter results by confidence threshold
        if confidence < confidence_threshold:
            logger.warning(f"OCR confidence {confidence:.2f} below threshold {confidence_threshold}")
        
        # Detect language
        detected_language = ocr_processor.detect_language(text)
        
        # Calculate processing time
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        # Prepare response
        result = OCRResult(
            success=True,
            extracted_text=text,
            confidence_score=confidence,
            language_detected=detected_language,
            processing_time_ms=processing_time,
            word_count=len(text.split()) if text else 0,
            lines=lines if output_format == 'structured' else [],
            metadata={
                'original_filename': file.filename,
                'file_size': file.size if hasattr(file, 'size') else 0,
                'languages_requested': language_list,
                'method_used': method,
                'preprocessed': preprocess
            }
        )
        
        logger.info(f"OCR completed for {file.filename} - {len(text)} characters extracted in {processing_time:.2f}ms")
        
        return result
        
    except Exception as e:
        logger.error(f"OCR processing failed: {e}")
        return OCRResult(
            success=False,
            extracted_text="",
            confidence_score=0.0,
            language_detected=None,
            processing_time_ms=(datetime.now(timezone.utc) - start_time).total_seconds() * 1000,
            word_count=0,
            metadata={'error': str(e)}
        )

@app.post("/process-document")
async def process_document_ocr(
    request: DocumentOCRRequest,
    db: Session = Depends(get_db)
):
    """Process OCR for a specific document in the database"""
    
    # Get document
    document = db.query(Document).filter_by(id=request.document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    # Check if file exists
    if not os.path.exists(document.file_path):
        raise HTTPException(status_code=404, detail="Document file not found")
    
    try:
        start_time = datetime.now(timezone.utc)
        
        # Process based on file type
        if document.mime_type.startswith('image/'):
            # Load image
            image = cv2.imread(document.file_path)
            if image is None:
                raise Exception("Failed to load image file")
            
            # Preprocess if requested
            if request.preprocess:
                image = await ocr_processor.preprocess_image(image, enhance=True)
            
            # Extract text
            text, confidence, lines = await ocr_processor.extract_text_hybrid(image, request.languages)
            
        elif document.mime_type == 'application/pdf':
            # For PDFs, we would need to convert to images first
            # This is a placeholder for PDF processing
            text = f"PDF OCR processing not yet implemented for {document.file_path}"
            confidence = 0.0
            lines = []
            
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type for OCR")
        
        # Update document with OCR results
        document.ocr_text = text
        document.ocr_confidence = confidence
        document.language_detected = ocr_processor.detect_language(text)
        
        # If no extracted text exists, use OCR text
        if not document.extracted_text:
            document.extracted_text = text
        
        db.commit()
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        logger.info(f"Document OCR completed for {document.id} - {len(text)} characters extracted")
        
        return {
            "success": True,
            "document_id": request.document_id,
            "extracted_text": text[:1000],  # Limit response size
            "confidence_score": confidence,
            "language_detected": document.language_detected,
            "processing_time_ms": processing_time,
            "word_count": len(text.split()) if text else 0
        }
        
    except Exception as e:
        logger.error(f"Document OCR processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"OCR processing failed: {str(e)}")

@app.get("/languages")
async def get_supported_languages():
    """Get list of supported languages"""
    languages = []
    for code, tesseract_code in SUPPORTED_LANGUAGES.items():
        lang_info = {
            'code': code,
            'tesseract_code': tesseract_code,
            'available_in_tesseract': ocr_processor.tesseract_available,
            'available_in_easyocr': code in ['en', 'hi', 'ml', 'ta', 'kn', 'te'] and ocr_processor.easyocr_available
        }
        languages.append(lang_info)
    
    return {
        'languages': languages,
        'tesseract_available': ocr_processor.tesseract_available,
        'easyocr_available': ocr_processor.easyocr_available
    }

@app.get("/capabilities")
async def get_ocr_capabilities():
    """Get OCR service capabilities"""
    return {
        'tesseract': {
            'available': ocr_processor.tesseract_available,
            'version': pytesseract.get_tesseract_version().string if ocr_processor.tesseract_available else None
        },
        'easyocr': {
            'available': ocr_processor.easyocr_available,
            'supported_languages': ['en', 'hi', 'ml', 'ta', 'kn', 'te'] if ocr_processor.easyocr_available else []
        },
        'supported_formats': list(ALLOWED_IMAGE_FORMATS),
        'max_file_size_mb': MAX_IMAGE_SIZE_MB,
        'preprocessing_available': True,
        'language_detection': True
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    
    try:
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'tesseract': 'available' if ocr_processor.tesseract_available else 'unavailable',
            'easyocr': 'available' if ocr_processor.easyocr_available else 'unavailable',
            'version': '1.0.0'
        }
        
        # If neither OCR engine is available, mark as unhealthy
        if not ocr_processor.tesseract_available and not ocr_processor.easyocr_available:
            health_status['status'] = 'unhealthy'
            health_status['error'] = 'No OCR engines available'
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': str(e)
        }

if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting OCR Service on port {service_config.ocr_service_port}")
    logger.info(f"Tesseract available: {ocr_processor.tesseract_available}")
    logger.info(f"EasyOCR available: {ocr_processor.easyocr_available}")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=service_config.ocr_service_port,
        log_level="info"
    )
