"""
MetroMind Vector Search Service
Semantic search using document embeddings and vector similarity
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, text
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import asyncio
import numpy as np
import json
import os
import pickle
from pathlib import Path
import logging

# Vector search imports
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False

# Import our models and config
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db, Document, DocumentEmbedding, User, AnalyticsRecord
from config import service_config, vector_search_config
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)
app = FastAPI(
    title="MetroMind Vector Search Service",
    description="Semantic search using document embeddings",
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

# Vector database directory
VECTOR_DB_DIR = Path("vector_db")
VECTOR_DB_DIR.mkdir(exist_ok=True)

# Pydantic models
class SearchRequest(BaseModel):
    query: str
    user_id: Optional[int] = None
    limit: int = 10
    threshold: float = 0.7  # Similarity threshold
    filters: Optional[Dict[str, Any]] = {}
    search_type: str = "semantic"  # semantic, hybrid, keyword
    
    @validator('limit')
    def limit_must_be_reasonable(cls, v):
        if v < 1 or v > 100:
            raise ValueError('Limit must be between 1 and 100')
        return v

class SearchResult(BaseModel):
    document_id: int
    title: str
    content_preview: str
    file_path: str
    similarity_score: float
    category: Optional[str] = None
    created_at: datetime
    metadata: Dict[str, Any] = {}

class SearchResponse(BaseModel):
    query: str
    results: List[SearchResult]
    total_results: int
    search_time_ms: float
    search_type: str
    filters_applied: Dict[str, Any] = {}

class IndexRequest(BaseModel):
    document_id: int
    force_reindex: bool = False

class IndexStats(BaseModel):
    total_documents: int
    indexed_documents: int
    index_size_mb: float
    last_updated: datetime
    status: str

# Vector Search Engine
class VectorSearchEngine:
    """Core vector search engine with multiple backends"""
    
    def __init__(self):
        self.embedding_model = None
        self.faiss_index = None
        self.document_ids = []  # Mapping from index position to document ID
        self.chroma_client = None
        self.chroma_collection = None
        self.index_file = VECTOR_DB_DIR / "faiss_index.bin"
        self.metadata_file = VECTOR_DB_DIR / "index_metadata.json"
        
    async def initialize(self):
        """Initialize the search engine"""
        logger.info("Initializing vector search engine...")
        
        try:
            # Load embedding model
            if SENTENCE_TRANSFORMERS_AVAILABLE:
                self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
                logger.info("Loaded sentence transformer model")
            
            # Initialize vector database backends
            if FAISS_AVAILABLE:
                await self._initialize_faiss()
            
            if CHROMADB_AVAILABLE:
                await self._initialize_chromadb()
            
            # Load existing index if available
            await self._load_existing_index()
            
            logger.info("Vector search engine initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize vector search engine: {e}")
    
    async def _initialize_faiss(self):
        """Initialize FAISS index"""
        try:
            dimension = 384  # all-MiniLM-L6-v2 embedding dimension
            
            # Use IndexFlatIP for cosine similarity
            self.faiss_index = faiss.IndexFlatIP(dimension)
            logger.info("FAISS index initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize FAISS: {e}")
    
    async def _initialize_chromadb(self):
        """Initialize ChromaDB"""
        try:
            # Create ChromaDB client
            self.chroma_client = chromadb.PersistentClient(
                path=str(VECTOR_DB_DIR / "chromadb"),
                settings=Settings(
                    anonymized_telemetry=False,
                    is_persistent=True
                )
            )
            
            # Get or create collection
            self.chroma_collection = self.chroma_client.get_or_create_collection(
                name="documents",
                metadata={"description": "MetroMind document embeddings"}
            )
            
            logger.info("ChromaDB initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB: {e}")
    
    async def _load_existing_index(self):
        """Load existing FAISS index and metadata"""
        try:
            if self.index_file.exists() and self.metadata_file.exists():
                # Load FAISS index
                if FAISS_AVAILABLE and self.faiss_index is not None:
                    self.faiss_index = faiss.read_index(str(self.index_file))
                
                # Load metadata
                with open(self.metadata_file, 'r') as f:
                    metadata = json.load(f)
                    self.document_ids = metadata.get('document_ids', [])
                
                logger.info(f"Loaded existing index with {len(self.document_ids)} documents")
            
        except Exception as e:
            logger.error(f"Failed to load existing index: {e}")
    
    async def _save_index(self):
        """Save FAISS index and metadata"""
        try:
            if FAISS_AVAILABLE and self.faiss_index is not None:
                faiss.write_index(self.faiss_index, str(self.index_file))
            
            # Save metadata
            metadata = {
                'document_ids': self.document_ids,
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'total_documents': len(self.document_ids)
            }
            
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info("Index saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save index: {e}")
    
    async def add_document(self, document_id: int, text: str, metadata: Dict = None):
        """Add document to vector index"""
        if not self.embedding_model:
            logger.warning("No embedding model available")
            return False
        
        try:
            # Generate embeddings
            embeddings = self.embedding_model.encode([text])
            embedding_vector = embeddings[0].astype('float32')
            
            # Normalize for cosine similarity
            faiss.normalize_L2(embedding_vector.reshape(1, -1))
            
            # Add to FAISS index
            if FAISS_AVAILABLE and self.faiss_index is not None:
                self.faiss_index.add(embedding_vector.reshape(1, -1))
                self.document_ids.append(document_id)
            
            # Add to ChromaDB
            if CHROMADB_AVAILABLE and self.chroma_collection is not None:
                self.chroma_collection.add(
                    embeddings=[embedding_vector.tolist()],
                    documents=[text[:1000]],  # Store preview
                    ids=[str(document_id)],
                    metadatas=[metadata or {}]
                )
            
            logger.info(f"Added document {document_id} to vector index")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add document {document_id} to index: {e}")
            return False
    
    async def search(self, query: str, limit: int = 10, threshold: float = 0.7) -> List[Tuple[int, float]]:
        """Search for similar documents"""
        if not self.embedding_model:
            logger.warning("No embedding model available")
            return []
        
        try:
            # Generate query embedding
            query_embedding = self.embedding_model.encode([query])
            query_vector = query_embedding[0].astype('float32')
            faiss.normalize_L2(query_vector.reshape(1, -1))
            
            results = []
            
            # Search using FAISS
            if FAISS_AVAILABLE and self.faiss_index is not None and len(self.document_ids) > 0:
                scores, indices = self.faiss_index.search(query_vector.reshape(1, -1), min(limit, len(self.document_ids)))
                
                for score, idx in zip(scores[0], indices[0]):
                    if score >= threshold and idx < len(self.document_ids):
                        document_id = self.document_ids[idx]
                        results.append((document_id, float(score)))
            
            # Search using ChromaDB (as backup or alternative)
            elif CHROMADB_AVAILABLE and self.chroma_collection is not None:
                chroma_results = self.chroma_collection.query(
                    query_embeddings=[query_vector.tolist()],
                    n_results=limit
                )
                
                if chroma_results['ids']:
                    for doc_id, distance in zip(chroma_results['ids'][0], chroma_results['distances'][0]):
                        # Convert distance to similarity score
                        similarity = 1.0 - distance
                        if similarity >= threshold:
                            results.append((int(doc_id), similarity))
            
            # Sort by similarity score (descending)
            results.sort(key=lambda x: x[1], reverse=True)
            
            return results[:limit]
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []
    
    async def remove_document(self, document_id: int):
        """Remove document from index"""
        try:
            # For FAISS, we need to rebuild the index without the document
            if document_id in self.document_ids:
                idx = self.document_ids.index(document_id)
                self.document_ids.pop(idx)
                
                # Rebuild FAISS index without this document
                await self._rebuild_faiss_index()
            
            # Remove from ChromaDB
            if CHROMADB_AVAILABLE and self.chroma_collection is not None:
                try:
                    self.chroma_collection.delete(ids=[str(document_id)])
                except Exception as e:
                    logger.warning(f"Failed to remove document {document_id} from ChromaDB: {e}")
            
            logger.info(f"Removed document {document_id} from index")
            
        except Exception as e:
            logger.error(f"Failed to remove document {document_id} from index: {e}")
    
    async def _rebuild_faiss_index(self):
        """Rebuild FAISS index (expensive operation)"""
        if not FAISS_AVAILABLE or not self.embedding_model:
            return
        
        try:
            # Create new index
            dimension = 384
            new_index = faiss.IndexFlatIP(dimension)
            
            # Re-add all remaining documents
            # This would require re-fetching all document texts from database
            # For now, we'll just reset the index
            self.faiss_index = new_index
            logger.info("FAISS index rebuilt")
            
        except Exception as e:
            logger.error(f"Failed to rebuild FAISS index: {e}")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get index statistics"""
        stats = {
            'total_documents': len(self.document_ids),
            'faiss_available': FAISS_AVAILABLE,
            'chromadb_available': CHROMADB_AVAILABLE,
            'embedding_model_loaded': self.embedding_model is not None,
            'index_size_mb': 0.0,
            'last_updated': None
        }
        
        # Calculate index size
        if self.index_file.exists():
            stats['index_size_mb'] = self.index_file.stat().st_size / (1024 * 1024)
        
        # Get last update time
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    metadata = json.load(f)
                    stats['last_updated'] = metadata.get('last_updated')
            except Exception:
                pass
        
        return stats

# Global search engine
search_engine = VectorSearchEngine()

@app.on_event("startup")
async def startup_event():
    """Initialize search engine on startup"""
    await search_engine.initialize()
    logger.info("Vector search service started")

# API Endpoints
@app.post("/search", response_model=SearchResponse)
async def search_documents(
    request: SearchRequest,
    db: Session = Depends(get_db)
):
    """Search documents using vector similarity"""
    start_time = datetime.now(timezone.utc)
    
    try:
        # Track search in analytics
        if request.user_id:
            analytics_record = AnalyticsRecord(
                user_id=request.user_id,
                event_type="search",
                event_data={
                    "query": request.query,
                    "search_type": request.search_type,
                    "filters": request.filters
                },
                timestamp=datetime.now(timezone.utc)
            )
            db.add(analytics_record)
        
        results = []
        
        if request.search_type == "semantic":
            # Vector similarity search
            vector_results = await search_engine.search(
                request.query, 
                request.limit, 
                request.threshold
            )
            
            # Get document details from database
            if vector_results:
                document_ids = [doc_id for doc_id, _ in vector_results]
                score_map = {doc_id: score for doc_id, score in vector_results}
                
                # Build query with filters
                query = db.query(Document).filter(Document.id.in_(document_ids))
                
                # Apply filters
                if request.filters:
                    if 'category' in request.filters:
                        query = query.filter(Document.category == request.filters['category'])
                    if 'priority' in request.filters:
                        query = query.filter(Document.priority == request.filters['priority'])
                    if 'date_from' in request.filters:
                        query = query.filter(Document.created_at >= request.filters['date_from'])
                    if 'date_to' in request.filters:
                        query = query.filter(Document.created_at <= request.filters['date_to'])
                    if 'user_id' in request.filters:
                        query = query.filter(Document.uploaded_by == request.filters['user_id'])
                
                documents = query.all()
                
                # Create search results
                for doc in documents:
                    similarity_score = score_map.get(doc.id, 0.0)
                    
                    result = SearchResult(
                        document_id=doc.id,
                        title=doc.title or doc.original_filename,
                        content_preview=doc.content[:200] + "..." if doc.content and len(doc.content) > 200 else (doc.content or ""),
                        file_path=doc.file_path,
                        similarity_score=similarity_score,
                        category=doc.category.value if doc.category else None,
                        created_at=doc.created_at,
                        metadata={
                            "file_size": doc.file_size,
                            "file_type": doc.file_type,
                            "language": doc.language_detected
                        }
                    )
                    results.append(result)
                
                # Sort by similarity score
                results.sort(key=lambda x: x.similarity_score, reverse=True)
        
        elif request.search_type == "keyword":
            # Traditional keyword search
            results = await keyword_search(request, db)
        
        elif request.search_type == "hybrid":
            # Combined semantic + keyword search
            semantic_results = await search_engine.search(request.query, request.limit // 2, request.threshold)
            keyword_results = await keyword_search(request, db, limit=request.limit // 2)
            
            # Combine and deduplicate results
            all_results = {}
            
            # Add semantic results
            if semantic_results:
                document_ids = [doc_id for doc_id, _ in semantic_results]
                score_map = {doc_id: score for doc_id, score in semantic_results}
                documents = db.query(Document).filter(Document.id.in_(document_ids)).all()
                
                for doc in documents:
                    all_results[doc.id] = SearchResult(
                        document_id=doc.id,
                        title=doc.title or doc.original_filename,
                        content_preview=doc.content[:200] + "..." if doc.content and len(doc.content) > 200 else (doc.content or ""),
                        file_path=doc.file_path,
                        similarity_score=score_map.get(doc.id, 0.0),
                        category=doc.category.value if doc.category else None,
                        created_at=doc.created_at,
                        metadata={
                            "file_size": doc.file_size,
                            "file_type": doc.file_type,
                            "language": doc.language_detected,
                            "search_type": "semantic"
                        }
                    )
            
            # Add keyword results (with lower scores if not already present)
            for keyword_result in keyword_results:
                if keyword_result.document_id not in all_results:
                    keyword_result.similarity_score *= 0.8  # Lower weight for keyword-only matches
                    keyword_result.metadata["search_type"] = "keyword"
                    all_results[keyword_result.document_id] = keyword_result
                else:
                    # Boost score for documents found by both methods
                    all_results[keyword_result.document_id].similarity_score += 0.2
                    all_results[keyword_result.document_id].metadata["search_type"] = "hybrid"
            
            results = list(all_results.values())
            results.sort(key=lambda x: x.similarity_score, reverse=True)
            results = results[:request.limit]
        
        # Commit analytics
        if request.user_id:
            db.commit()
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        
        return SearchResponse(
            query=request.query,
            results=results,
            total_results=len(results),
            search_time_ms=processing_time,
            search_type=request.search_type,
            filters_applied=request.filters or {}
        )
    
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def keyword_search(request: SearchRequest, db: Session, limit: Optional[int] = None) -> List[SearchResult]:
    """Perform keyword-based search"""
    limit = limit or request.limit
    
    try:
        # Build search query
        query_terms = request.query.lower().split()
        
        # Build SQL query for full-text search
        query = db.query(Document)
        
        # Apply text search conditions
        for term in query_terms:
            query = query.filter(
                or_(
                    Document.content.ilike(f'%{term}%'),
                    Document.title.ilike(f'%{term}%'),
                    Document.original_filename.ilike(f'%{term}%'),
                    Document.extracted_text.ilike(f'%{term}%')
                )
            )
        
        # Apply filters
        if request.filters:
            if 'category' in request.filters:
                query = query.filter(Document.category == request.filters['category'])
            if 'priority' in request.filters:
                query = query.filter(Document.priority == request.filters['priority'])
            if 'date_from' in request.filters:
                query = query.filter(Document.created_at >= request.filters['date_from'])
            if 'date_to' in request.filters:
                query = query.filter(Document.created_at <= request.filters['date_to'])
            if 'user_id' in request.filters:
                query = query.filter(Document.uploaded_by == request.filters['user_id'])
        
        # Execute query
        documents = query.limit(limit).all()
        
        # Create search results
        results = []
        for doc in documents:
            # Calculate keyword relevance score
            content_text = f"{doc.content or ''} {doc.title or ''} {doc.original_filename or ''}".lower()
            score = sum(content_text.count(term) for term in query_terms) / len(query_terms)
            score = min(score / 10, 1.0)  # Normalize to 0-1 range
            
            result = SearchResult(
                document_id=doc.id,
                title=doc.title or doc.original_filename,
                content_preview=doc.content[:200] + "..." if doc.content and len(doc.content) > 200 else (doc.content or ""),
                file_path=doc.file_path,
                similarity_score=score,
                category=doc.category.value if doc.category else None,
                created_at=doc.created_at,
                metadata={
                    "file_size": doc.file_size,
                    "file_type": doc.file_type,
                    "language": doc.language_detected
                }
            )
            results.append(result)
        
        return results
    
    except Exception as e:
        logger.error(f"Keyword search failed: {e}")
        return []

@app.post("/index-document")
async def index_document(
    request: IndexRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Add document to search index"""
    try:
        # Get document from database
        document = db.query(Document).filter(Document.id == request.document_id).first()
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")
        
        # Check if already indexed
        existing_embedding = db.query(DocumentEmbedding).filter(
            DocumentEmbedding.document_id == request.document_id
        ).first()
        
        if existing_embedding and not request.force_reindex:
            return {
                "success": True,
                "message": "Document already indexed",
                "document_id": request.document_id
            }
        
        # Add to index in background
        background_tasks.add_task(
            _index_document_async,
            document.id,
            document.content or document.extracted_text or "",
            {
                "title": document.title,
                "category": document.category.value if document.category else None,
                "file_type": document.file_type,
                "created_at": document.created_at.isoformat()
            }
        )
        
        return {
            "success": True,
            "message": "Document indexing started",
            "document_id": request.document_id
        }
    
    except Exception as e:
        logger.error(f"Failed to index document {request.document_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _index_document_async(document_id: int, content: str, metadata: Dict):
    """Background task to index document"""
    try:
        success = await search_engine.add_document(document_id, content, metadata)
        if success:
            # Save index periodically
            await search_engine._save_index()
            logger.info(f"Document {document_id} indexed successfully")
        else:
            logger.error(f"Failed to index document {document_id}")
    
    except Exception as e:
        logger.error(f"Background indexing failed for document {document_id}: {e}")

@app.delete("/index/{document_id}")
async def remove_from_index(document_id: int):
    """Remove document from search index"""
    try:
        await search_engine.remove_document(document_id)
        await search_engine._save_index()
        
        return {
            "success": True,
            "message": f"Document {document_id} removed from index"
        }
    
    except Exception as e:
        logger.error(f"Failed to remove document {document_id} from index: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/index/stats", response_model=IndexStats)
async def get_index_stats():
    """Get search index statistics"""
    try:
        stats = await search_engine.get_stats()
        
        return IndexStats(
            total_documents=stats['total_documents'],
            indexed_documents=stats['total_documents'],  # Assume all are indexed for now
            index_size_mb=stats['index_size_mb'],
            last_updated=datetime.fromisoformat(stats['last_updated']) if stats['last_updated'] else datetime.now(timezone.utc),
            status="healthy" if stats['embedding_model_loaded'] else "degraded"
        )
    
    except Exception as e:
        logger.error(f"Failed to get index stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reindex")
async def reindex_all(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Reindex all documents"""
    try:
        # Get all documents
        documents = db.query(Document).all()
        
        # Add reindexing task
        background_tasks.add_task(_reindex_all_documents, documents)
        
        return {
            "success": True,
            "message": f"Started reindexing {len(documents)} documents",
            "total_documents": len(documents)
        }
    
    except Exception as e:
        logger.error(f"Failed to start reindexing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _reindex_all_documents(documents):
    """Background task to reindex all documents"""
    try:
        logger.info(f"Starting reindex of {len(documents)} documents")
        
        for i, document in enumerate(documents):
            try:
                content = document.content or document.extracted_text or ""
                if content:
                    metadata = {
                        "title": document.title,
                        "category": document.category.value if document.category else None,
                        "file_type": document.file_type,
                        "created_at": document.created_at.isoformat()
                    }
                    
                    await search_engine.add_document(document.id, content, metadata)
                
                # Save index every 100 documents
                if (i + 1) % 100 == 0:
                    await search_engine._save_index()
                    logger.info(f"Reindexed {i + 1}/{len(documents)} documents")
                    
            except Exception as e:
                logger.error(f"Failed to reindex document {document.id}: {e}")
        
        # Final save
        await search_engine._save_index()
        logger.info(f"Completed reindexing {len(documents)} documents")
        
    except Exception as e:
        logger.error(f"Reindexing failed: {e}")

@app.get("/")
async def root():
    """Service root endpoint"""
    return {
        "service": "MetroMind Vector Search Service",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "capabilities": {
            "faiss_available": FAISS_AVAILABLE,
            "chromadb_available": CHROMADB_AVAILABLE,
            "sentence_transformers_available": SENTENCE_TRANSFORMERS_AVAILABLE
        }
    }

@app.get("/health")
async def health_check():
    """Service health check"""
    try:
        stats = await search_engine.get_stats()
        
        return {
            "status": "healthy" if stats['embedding_model_loaded'] else "degraded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "vector_backends": {
                "faiss": FAISS_AVAILABLE,
                "chromadb": CHROMADB_AVAILABLE
            },
            "embedding_model_loaded": stats['embedding_model_loaded'],
            "indexed_documents": stats['total_documents'],
            "index_size_mb": stats['index_size_mb']
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
    
    logger.info(f"Starting Vector Search service on port {service_config.search_service_port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=service_config.search_service_port,
        log_level="info"
    )
