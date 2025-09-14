# download_models.py
from sentence_transformers import SentenceTransformer
from transformers import pipeline
import spacy

print("--- Downloading AI/ML Models ---")

# 1. Models from AI/ML Service
print("Downloading Summarizer (bart-large-cnn)...")
pipeline("summarization", model="facebook/bart-large-cnn")

print("Downloading Classifier (bart-large-mnli)...")
pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# 2. Model from RAG & Search Service
print("Downloading Embedding Model (all-MiniLM-L6-v2)...")
SentenceTransformer('all-MiniLM-L6-v2')

# 3. Model from spaCy (from logs)
print("Downloading spaCy English Model...")
spacy.cli.download("en_core_web_sm")

print("--- All models have been downloaded and cached locally. ---")