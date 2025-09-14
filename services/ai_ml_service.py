"""
MetroMind AI/ML Service
Document classification, analysis, and machine learning processing
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import os
import re
import json
import asyncio
import numpy as np
from collections import Counter
import hashlib

# NLP and ML imports
try:
    from transformers import pipeline, AutoTokenizer, AutoModel
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False

try:
    from langdetect import detect, detect_langs
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False

# Import our models and config
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Document, User, DocumentCategory, Priority, DocumentEmbedding
from config import service_config, ai_config
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)
app = FastAPI(
    title="MetroMind AI/ML Service",
    description="Document classification, analysis, and machine learning processing",
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

# Pydantic models
class DocumentAnalysisRequest(BaseModel):
    document_id: str
    text: Optional[str] = None
    analyze_sentiment: bool = True
    extract_entities: bool = True
    generate_summary: bool = True
    detect_language: bool = True
    classify_category: bool = True
    determine_priority: bool = True

class TextAnalysisRequest(BaseModel):
    text: str
    analyze_sentiment: bool = True
    extract_entities: bool = True
    generate_summary: bool = True
    detect_language: bool = True
    classify_category: bool = True
    determine_priority: bool = True

class AnalysisResult(BaseModel):
    success: bool
    language_detected: Optional[str] = None
    category: Optional[str] = None
    priority: Optional[str] = None
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None
    entities: List[Dict[str, Any]] = []
    summary: Optional[str] = None
    keywords: List[str] = []
    confidence_scores: Dict[str, float] = {}
    processing_time_ms: float = 0
    metadata: Dict[str, Any] = {}

class EmbeddingRequest(BaseModel):
    text: str
    model_name: str = "all-MiniLM-L6-v2"

class EmbeddingResult(BaseModel):
    success: bool
    embedding: List[float] = []
    dimension: int = 0
    model_used: str = ""
    processing_time_ms: float = 0

# AI/ML Model Manager
class AIModelManager:
    """Manages AI/ML models and their loading/unloading"""
    
    def __init__(self):
        self.models = {}
        self.model_cache_size = 3  # Maximum models to keep in memory
        self.model_usage = {}  # Track model usage for LRU eviction
        
        # Initialize models
        self._initialize_models()
        
    def _initialize_models(self):
        """Initialize available models"""
        logger.info("Initializing AI/ML models...")
        
        # Load sentence transformer for embeddings
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                model_name = "all-MiniLM-L6-v2"
                self.models['sentence_transformer'] = SentenceTransformer(model_name)
                logger.info(f"Loaded sentence transformer: {model_name}")
            except Exception as e:
                logger.warning(f"Failed to load sentence transformer: {e}")
        
        # Load spaCy model for NER
        if SPACY_AVAILABLE:
            try:
                # Try to load English model
                self.models['spacy_en'] = spacy.load("en_core_web_sm")
                logger.info("Loaded spaCy English model")
            except OSError:
                logger.warning("spaCy English model not found. Run: python -m spacy download en_core_web_sm")
        
        # Initialize transformers pipelines
        if TRANSFORMERS_AVAILABLE:
            try:
                # Sentiment analysis
                self.models['sentiment'] = pipeline("sentiment-analysis", 
                                                   model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                                                   return_all_scores=True)
                logger.info("Loaded sentiment analysis model")
            except Exception as e:
                logger.warning(f"Failed to load sentiment model: {e}")
                
            try:
                # Summarization
                self.models['summarization'] = pipeline("summarization", 
                                                       model="facebook/bart-large-cnn")
                logger.info("Loaded summarization model")
            except Exception as e:
                logger.warning(f"Failed to load summarization model: {e}")
        
        logger.info(f"AI/ML models initialized: {list(self.models.keys())}")
    
    def get_model(self, model_name: str):
        """Get model with LRU caching"""
        if model_name in self.models:
            self.model_usage[model_name] = datetime.now(timezone.utc)
            return self.models[model_name]
        return None
    
    def is_available(self, model_name: str) -> bool:
        """Check if model is available"""
        return model_name in self.models

# Global model manager
model_manager = AIModelManager()

# Text processing utilities
class DocumentProcessor:
    """Main document processing class"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep Malayalam characters
        text = re.sub(r'[^\w\s\u0D00-\u0D7F\u0900-\u097F]', ' ', text)
        
        return text.strip()
    
    @staticmethod
    def extract_keywords(text: str, max_keywords: int = 10) -> List[str]:
        """Extract keywords from text using simple frequency analysis"""
        if not text:
            return []
        
        # Clean text
        text = DocumentProcessor.clean_text(text.lower())
        
        # Split into words
        words = text.split()
        
        # Remove common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
            'by', 'this', 'that', 'these', 'those', 'is', 'are', 'was', 'were', 'be', 'been',
            'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
            'may', 'might', 'can', 'must', 'shall', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his', 'her', 'its', 'our', 'their'
        }
        
        # Filter words
        filtered_words = [word for word in words if len(word) > 2 and word not in stop_words]
        
        # Count frequencies
        word_freq = Counter(filtered_words)
        
        # Return top keywords
        return [word for word, _ in word_freq.most_common(max_keywords)]
    
    @staticmethod
    def detect_language(text: str) -> Optional[str]:
        """Detect language of text"""
        if not text or not LANGDETECT_AVAILABLE:
            return None
        
        try:
            # Detect language
            detected_lang = detect(text)
            
            # Map to our supported languages
            lang_mapping = {
                'en': 'en',
                'hi': 'hi',
                'ml': 'ml',  # Malayalam
                'ta': 'ta',  # Tamil
                'kn': 'kn',  # Kannada
                'te': 'te',  # Telugu
                'bn': 'bn',  # Bengali
                'mr': 'mr',  # Marathi
                'gu': 'gu',  # Gujarati
                'pa': 'pa',  # Punjabi
            }
            
            return lang_mapping.get(detected_lang, 'en')
        
        except Exception as e:
            logger.warning(f"Language detection failed: {e}")
            
            # Fallback: character-based detection
            if any('\u0D00' <= char <= '\u0D7F' for char in text):
                return 'ml'  # Malayalam
            elif any('\u0900' <= char <= '\u097F' for char in text):
                return 'hi'  # Hindi/Devanagari
            elif any('\u0B80' <= char <= '\u0BFF' for char in text):
                return 'ta'  # Tamil
            else:
                return 'en'  # Default to English
    
    @staticmethod
    def classify_document_category(text: str, filename: str = "") -> Tuple[DocumentCategory, float]:
        """Classify document category using keyword matching"""
        if not text:
            return DocumentCategory.OTHER, 0.0
        
        text_lower = f"{text.lower()} {filename.lower()}"
        
        # Define category keywords with weights
        category_keywords = {
            DocumentCategory.SAFETY: {
                'safety': 3, 'accident': 3, 'incident': 3, 'hazard': 3, 'emergency': 3,
                'evacuation': 2, 'fire': 2, 'security': 2, 'risk': 2, 'danger': 2,
                'unsafe': 2, 'precaution': 1, 'warning': 1, 'alert': 1
            },
            DocumentCategory.MAINTENANCE: {
                'maintenance': 3, 'repair': 3, 'inspection': 3, 'service': 2, 'equipment': 2,
                'technical': 2, 'mechanical': 2, 'electrical': 2, 'fault': 2, 'breakdown': 2,
                'preventive': 2, 'corrective': 1, 'spare': 1, 'parts': 1, 'tools': 1
            },
            DocumentCategory.FINANCE: {
                'budget': 3, 'finance': 3, 'payment': 3, 'invoice': 3, 'cost': 2, 'expense': 2,
                'revenue': 2, 'accounting': 2, 'audit': 2, 'financial': 2, 'procurement': 2,
                'purchase': 2, 'money': 1, 'price': 1, 'amount': 1, 'billing': 1
            },
            DocumentCategory.OPERATIONS: {
                'operation': 3, 'schedule': 3, 'timetable': 3, 'passenger': 2, 'service': 2,
                'performance': 2, 'capacity': 2, 'frequency': 2, 'route': 2, 'station': 2,
                'platform': 2, 'train': 2, 'metro': 2, 'rail': 2, 'transport': 1
            },
            DocumentCategory.HR: {
                'hr': 3, 'human': 2, 'employee': 3, 'staff': 3, 'training': 2, 'recruitment': 2,
                'salary': 2, 'leave': 2, 'policy': 2, 'personnel': 2, 'attendance': 2,
                'performance': 1, 'appraisal': 1, 'promotion': 1, 'transfer': 1
            },
            DocumentCategory.LEGAL: {
                'legal': 3, 'contract': 3, 'agreement': 3, 'compliance': 2, 'regulation': 2,
                'law': 2, 'litigation': 2, 'terms': 2, 'conditions': 2, 'clause': 1,
                'court': 1, 'case': 1, 'lawyer': 1, 'attorney': 1
            },
            DocumentCategory.REGULATORY: {
                'regulatory': 3, 'commissioner': 3, 'ministry': 3, 'government': 2, 'compliance': 2,
                'standard': 2, 'specification': 2, 'guideline': 2, 'directive': 2, 'policy': 2,
                'rule': 2, 'notification': 1, 'circular': 1, 'order': 1
            },
            DocumentCategory.ENVIRONMENTAL: {
                'environmental': 3, 'environment': 3, 'pollution': 2, 'waste': 2, 'green': 1,
                'ecology': 2, 'impact': 2, 'assessment': 2, 'carbon': 1, 'emission': 1,
                'sustainable': 1, 'renewable': 1, 'conservation': 1
            }
        }
        
        # Calculate scores for each category
        category_scores = {}
        for category, keywords in category_keywords.items():
            score = 0
            keyword_matches = 0
            
            for keyword, weight in keywords.items():
                if keyword in text_lower:
                    count = text_lower.count(keyword)
                    score += count * weight
                    keyword_matches += count
            
            # Normalize score by text length and keyword matches
            if keyword_matches > 0:
                category_scores[category] = score / max(len(text_lower.split()) / 100, 1)
        
        if not category_scores:
            return DocumentCategory.OTHER, 0.0
        
        # Get category with highest score
        best_category = max(category_scores, key=category_scores.get)
        confidence = min(category_scores[best_category] / 10, 1.0)  # Normalize to 0-1
        
        return best_category, confidence
    
    @staticmethod
    def determine_priority(text: str, filename: str = "") -> Tuple[Priority, float]:
        """Determine document priority using keyword matching"""
        if not text:
            return Priority.MEDIUM, 0.0
        
        text_lower = f"{text.lower()} {filename.lower()}"
        
        # Priority keywords with weights
        priority_keywords = {
            Priority.CRITICAL: {
                'emergency': 5, 'urgent': 4, 'critical': 4, 'immediate': 4, 'asap': 3,
                'crisis': 4, 'disaster': 4, 'failure': 3, 'breakdown': 3, 'accident': 4
            },
            Priority.HIGH: {
                'important': 3, 'priority': 3, 'high': 2, 'attention': 2, 'escalate': 3,
                'significant': 2, 'major': 2, 'serious': 2, 'concern': 1, 'issue': 1
            },
            Priority.MEDIUM: {
                'medium': 2, 'normal': 1, 'regular': 1, 'standard': 1, 'routine': 1
            },
            Priority.LOW: {
                'low': 2, 'minor': 2, 'routine': 1, 'informational': 1, 'fyi': 1
            }
        }
        
        # Calculate scores
        priority_scores = {}
        for priority, keywords in priority_keywords.items():
            score = 0
            for keyword, weight in keywords.items():
                if keyword in text_lower:
                    count = text_lower.count(keyword)
                    score += count * weight
            priority_scores[priority] = score
        
        # Determine priority
        if priority_scores[Priority.CRITICAL] > 0:
            return Priority.CRITICAL, min(priority_scores[Priority.CRITICAL] / 10, 1.0)
        elif priority_scores[Priority.HIGH] > 0:
            return Priority.HIGH, min(priority_scores[Priority.HIGH] / 8, 1.0)
        elif priority_scores[Priority.LOW] > priority_scores[Priority.MEDIUM]:
            return Priority.LOW, min(priority_scores[Priority.LOW] / 5, 1.0)
        else:
            return Priority.MEDIUM, 0.5
    
    @staticmethod
    async def analyze_sentiment(text: str) -> Tuple[float, str, float]:
        """Analyze sentiment using transformer model"""
        if not text or not model_manager.is_available('sentiment'):
            return 0.0, 'neutral', 0.0
        
        try:
            sentiment_pipeline = model_manager.get_model('sentiment')
            
            # Truncate text if too long
            max_length = 512
            if len(text) > max_length:
                text = text[:max_length]
            
            results = sentiment_pipeline(text)[0]  # Get first result since return_all_scores=True
            
            # Find the result with highest score
            best_result = max(results, key=lambda x: x['score'])
            
            # Convert label to sentiment
            label_mapping = {
                'LABEL_0': 'negative',
                'LABEL_1': 'neutral', 
                'LABEL_2': 'positive',
                'NEGATIVE': 'negative',
                'NEUTRAL': 'neutral',
                'POSITIVE': 'positive'
            }
            
            sentiment_label = label_mapping.get(best_result['label'], 'neutral')
            
            # Convert to numeric score (-1 to 1)
            if sentiment_label == 'positive':
                sentiment_score = best_result['score']
            elif sentiment_label == 'negative':
                sentiment_score = -best_result['score']
            else:
                sentiment_score = 0.0
            
            return sentiment_score, sentiment_label, best_result['score']
        
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return 0.0, 'neutral', 0.0
    
    @staticmethod
    async def extract_entities(text: str) -> List[Dict[str, Any]]:
        """Extract named entities using spaCy"""
        if not text or not model_manager.is_available('spacy_en'):
            return []
        
        try:
            nlp = model_manager.get_model('spacy_en')
            
            # Truncate text if too long
            if len(text) > 1000000:  # spaCy limit
                text = text[:1000000]
            
            doc = nlp(text)
            
            entities = []
            for ent in doc.ents:
                entities.append({
                    'text': ent.text,
                    'label': ent.label_,
                    'description': spacy.explain(ent.label_),
                    'start': ent.start_char,
                    'end': ent.end_char,
                    'confidence': getattr(ent, 'confidence', 0.9)
                })
            
            return entities
        
        except Exception as e:
            logger.error(f"Entity extraction failed: {e}")
            return []
    
    @staticmethod
    async def generate_summary(text: str, max_length: int = 150) -> Optional[str]:
        """Generate summary using transformer model"""
        if not text or not model_manager.is_available('summarization'):
            # Fallback to simple extractive summary
            return DocumentProcessor.generate_simple_summary(text, max_length)
        
        try:
            summarizer = model_manager.get_model('summarization')
            
            # Skip if text is too short
            if len(text.split()) < 30:
                return text[:max_length] + "..." if len(text) > max_length else text
            
            # Truncate if too long for model
            max_input_length = 1024
            if len(text) > max_input_length:
                text = text[:max_input_length]
            
            summary = summarizer(text, max_length=max_length, min_length=30, do_sample=False)[0]
            return summary['summary_text']
        
        except Exception as e:
            logger.error(f"Summary generation failed: {e}")
            return DocumentProcessor.generate_simple_summary(text, max_length)
    
    @staticmethod
    def generate_simple_summary(text: str, max_length: int = 150) -> str:
        """Generate simple extractive summary"""
        if not text:
            return ""
        
        if len(text) <= max_length:
            return text
        
        # Split into sentences
        sentences = text.split('.')[:3]  # Take first 3 sentences
        summary = '. '.join(sentences).strip()
        
        if len(summary) > max_length:
            summary = summary[:max_length] + "..."
        
        return summary
    
    @staticmethod
    async def generate_embeddings(text: str, model_name: str = "all-MiniLM-L6-v2") -> Optional[np.ndarray]:
        """Generate text embeddings using sentence transformer"""
        if not text or not model_manager.is_available('sentence_transformer'):
            return None
        
        try:
            model = model_manager.get_model('sentence_transformer')
            embeddings = model.encode([text])
            return embeddings[0]
        
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            return None

# Global processor instance
document_processor = DocumentProcessor()

# API Endpoints
@app.post("/analyze-document", response_model=AnalysisResult)
async def analyze_document(
    request: DocumentAnalysisRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Analyze a document from the database"""
    
    start_time = datetime.now(timezone.utc)
    
    # Get document
    document = db.query(Document).filter_by(id=request.document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    # Use provided text or document's extracted text
    text = request.text or document.extracted_text or document.ocr_text or ""
    
    if not text:
        raise HTTPException(status_code=400, detail="No text available for analysis")
    
    try:
        result = await analyze_text_content(
            text, 
            document.original_filename,
            request.analyze_sentiment,
            request.extract_entities,
            request.generate_summary,
            request.detect_language,
            request.classify_category,
            request.determine_priority
        )
        
        # Update document with analysis results
        if request.detect_language and result.language_detected:
            document.language_detected = result.language_detected
        
        if request.classify_category and result.category:
            document.category = DocumentCategory(result.category)
        
        if request.determine_priority and result.priority:
            document.priority = Priority(int(result.priority))
        
        if request.analyze_sentiment and result.sentiment_score is not None:
            document.sentiment_score = result.sentiment_score
        
        if request.generate_summary and result.summary:
            document.summary = result.summary
        
        # Store key entities
        if request.extract_entities and result.entities:
            document.key_entities = {
                'entities': result.entities[:10],  # Store top 10 entities
                'keywords': result.keywords
            }
        
        db.commit()
        
        # Generate embeddings in background
        if model_manager.is_available('sentence_transformer'):
            background_tasks.add_task(generate_and_store_embeddings, document.id, text, db)
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        result.processing_time_ms = processing_time
        
        logger.info(f"Document {request.document_id} analyzed in {processing_time:.2f}ms")
        
        return result
    
    except Exception as e:
        logger.error(f"Document analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.post("/analyze-text", response_model=AnalysisResult)
async def analyze_text(request: TextAnalysisRequest):
    """Analyze arbitrary text"""
    
    start_time = datetime.now(timezone.utc)
    
    if not request.text:
        raise HTTPException(status_code=400, detail="No text provided for analysis")
    
    try:
        result = await analyze_text_content(
            request.text,
            "",  # No filename
            request.analyze_sentiment,
            request.extract_entities,
            request.generate_summary,
            request.detect_language,
            request.classify_category,
            request.determine_priority
        )
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        result.processing_time_ms = processing_time
        
        return result
    
    except Exception as e:
        logger.error(f"Text analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

async def analyze_text_content(
    text: str,
    filename: str = "",
    analyze_sentiment: bool = True,
    extract_entities: bool = True,
    generate_summary: bool = True,
    detect_language: bool = True,
    classify_category: bool = True,
    determine_priority: bool = True
) -> AnalysisResult:
    """Core text analysis function"""
    
    result = AnalysisResult(success=True)
    confidence_scores = {}
    
    # Clean text
    clean_text = document_processor.clean_text(text)
    
    # Extract keywords
    result.keywords = document_processor.extract_keywords(clean_text)
    
    # Language detection
    if detect_language:
        result.language_detected = document_processor.detect_language(clean_text)
    
    # Category classification
    if classify_category:
        category, confidence = document_processor.classify_document_category(clean_text, filename)
        result.category = category.value
        confidence_scores['category'] = confidence
    
    # Priority determination
    if determine_priority:
        priority, confidence = document_processor.determine_priority(clean_text, filename)
        result.priority = str(priority.value)
        confidence_scores['priority'] = confidence
    
    # Sentiment analysis
    if analyze_sentiment:
        sentiment_score, sentiment_label, confidence = await document_processor.analyze_sentiment(clean_text)
        result.sentiment_score = sentiment_score
        result.sentiment_label = sentiment_label
        confidence_scores['sentiment'] = confidence
    
    # Entity extraction
    if extract_entities:
        entities = await document_processor.extract_entities(clean_text)
        result.entities = entities
        confidence_scores['entities'] = 1.0 if entities else 0.0
    
    # Summary generation
    if generate_summary:
        summary = await document_processor.generate_summary(clean_text)
        result.summary = summary
        confidence_scores['summary'] = 1.0 if summary else 0.0
    
    result.confidence_scores = confidence_scores
    
    return result

async def generate_and_store_embeddings(document_id: str, text: str, db: Session):
    """Generate and store document embeddings"""
    try:
        embeddings = await document_processor.generate_embeddings(text)
        if embeddings is not None:
            # Serialize embeddings
            embedding_bytes = embeddings.tobytes()
            
            # Create embedding record
            doc_embedding = DocumentEmbedding(
                document_id=document_id,
                embedding_model="all-MiniLM-L6-v2",
                embedding_vector=embedding_bytes,
                chunk_index=0,
                chunk_text=text[:500]  # Store first 500 chars as reference
            )
            
            db.add(doc_embedding)
            db.commit()
            
            logger.info(f"Embeddings generated and stored for document {document_id}")
    
    except Exception as e:
        logger.error(f"Failed to generate embeddings for document {document_id}: {e}")

@app.post("/generate-embeddings", response_model=EmbeddingResult)
async def generate_embeddings_endpoint(request: EmbeddingRequest):
    """Generate text embeddings"""
    
    start_time = datetime.now(timezone.utc)
    
    if not request.text:
        raise HTTPException(status_code=400, detail="No text provided")
    
    try:
        embeddings = await document_processor.generate_embeddings(request.text, request.model_name)
        
        if embeddings is None:
            return EmbeddingResult(
                success=False,
                processing_time_ms=(datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        return EmbeddingResult(
            success=True,
            embedding=embeddings.tolist(),
            dimension=len(embeddings),
            model_used=request.model_name,
            processing_time_ms=processing_time
        )
    
    except Exception as e:
        logger.error(f"Embedding generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Embedding generation failed: {str(e)}")

@app.get("/models")
async def get_available_models():
    """Get list of available AI/ML models"""
    return {
        'models': list(model_manager.models.keys()),
        'capabilities': {
            'sentiment_analysis': model_manager.is_available('sentiment'),
            'summarization': model_manager.is_available('summarization'),
            'embeddings': model_manager.is_available('sentence_transformer'),
            'named_entity_recognition': model_manager.is_available('spacy_en'),
            'language_detection': LANGDETECT_AVAILABLE,
            'text_classification': True,  # Rule-based, always available
            'priority_detection': True,   # Rule-based, always available
        },
        'supported_languages': ai_config.supported_languages
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    
    try:
        # Check model availability
        models_loaded = len(model_manager.models)
        
        health_status = {
            'status': 'healthy' if models_loaded > 0 else 'degraded',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'models_loaded': models_loaded,
            'available_models': list(model_manager.models.keys()),
            'transformers_available': TRANSFORMERS_AVAILABLE,
            'sentence_transformers_available': SENTENCE_TRANSFORMERS_AVAILABLE,
            'spacy_available': SPACY_AVAILABLE,
            'langdetect_available': LANGDETECT_AVAILABLE,
            'version': '1.0.0'
        }
        
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
    
    logger.info(f"Starting AI/ML Service on port {service_config.ai_ml_service_port}")
    logger.info(f"Models available: {list(model_manager.models.keys())}")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=service_config.ai_ml_service_port,
        log_level="info"
    )
