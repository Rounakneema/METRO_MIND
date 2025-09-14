from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from typing import Dict, List, Optional
from sqlalchemy.orm import Session

from ..database import get_db
from ..services.ai_service import AIService

# Initialize router
router = APIRouter(
    prefix="/api/ai",
    tags=["ai"],
    responses={404: {"description": "Not found"}}
)

# Initialize AI service
ai_service = AIService()

@router.post("/process")
async def process_document(
    file: UploadFile = File(...),
    model: str = Form(...),
    query: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    """
    Process a document with AI models
    """
    try:
        # In production, this would save the file, call OCR service first,
        # then pass to AI service for analysis
        return {
            "status": "success",
            "message": "Document submitted for AI processing",
            "model": model,
            "query": query,
            "filename": file.filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/insights")
async def get_insights(db: Session = Depends(get_db)):
    """
    Get AI insights across all documents
    """
    try:
        # In production, this would query the database for documents with AI analysis
        # and aggregate the results
        return {
            "categories": [
                {"name": "Invoice", "count": 58, "color": "#FF6384"},
                {"name": "Contract", "count": 42, "color": "#36A2EB"},
                {"name": "Report", "count": 27, "color": "#FFCE56"},
                {"name": "Email", "count": 73, "color": "#4BC0C0"}
            ],
            "sentiments": {
                "positive": 45,
                "neutral": 120,
                "negative": 35
            },
            "entities": {
                "PERSON": 156,
                "ORG": 92,
                "DATE": 183,
                "MONEY": 67
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/relationships")
async def get_document_relationships(db: Session = Depends(get_db)):
    """
    Get document relationships for visualization
    """
    try:
        # In production, this would analyze connections between documents
        # based on content similarity, references, etc.
        return {
            "nodes": [
                {"id": "doc1", "name": "Invoice #1234", "type": "invoice", "size": 10},
                {"id": "doc2", "name": "Contract A", "type": "contract", "size": 15},
                {"id": "doc3", "name": "Email Thread", "type": "email", "size": 8},
                {"id": "doc4", "name": "Report Q2", "type": "report", "size": 12},
                {"id": "doc5", "name": "Invoice #5678", "type": "invoice", "size": 10},
                {"id": "doc6", "name": "Contract B", "type": "contract", "size": 14},
                {"id": "doc7", "name": "Meeting Notes", "type": "notes", "size": 7}
            ],
            "connections": [
                {"source": "doc1", "target": "doc2", "strength": 0.8},
                {"source": "doc1", "target": "doc5", "strength": 0.5},
                {"source": "doc2", "target": "doc4", "strength": 0.7},
                {"source": "doc3", "target": "doc4", "strength": 0.6},
                {"source": "doc2", "target": "doc6", "strength": 0.9},
                {"source": "doc4", "target": "doc7", "strength": 0.4},
                {"source": "doc6", "target": "doc7", "strength": 0.5}
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/models")
async def get_available_models():
    """
    Get available AI models for document processing
    """
    try:
        return {
            "models": [
                {
                    "id": "text-analysis",
                    "name": "Text Analysis Model",
                    "description": "Extracts key information from document text"
                },
                {
                    "id": "document-classification",
                    "name": "Document Classification Model",
                    "description": "Categorizes documents by type"
                },
                {
                    "id": "entity-extraction",
                    "name": "Entity Extraction Model",
                    "description": "Identifies people, organizations, dates, and other entities"
                },
                {
                    "id": "sentiment-analysis",
                    "name": "Sentiment Analysis Model",
                    "description": "Detects sentiment and emotional tone in documents"
                }
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))