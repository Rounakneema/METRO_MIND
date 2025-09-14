# 🏗️ MetroMind Microservices Architecture

## 🎯 Architecture Overview

MetroMind uses a distributed microservices architecture designed for scalability, maintainability, and confidential document processing. All AI/ML models run locally to ensure data privacy.

## 📊 Service Architecture Diagram

```
                    ┌─────────────────────────────────────────┐
                    │              FRONTEND LAYER             │
                    ├─────────────────┬───────────────────────┤
                    │  Web App (3000) │  Mobile App (Cordova) │
                    └─────────────────┴───────────────────────┘
                                        │
                    ┌─────────────────────────────────────────┐
                    │             API GATEWAY                 │
                    │          (Port 8080)                    │
                    │  • Authentication                       │
                    │  • Rate Limiting                        │
                    │  • Load Balancing                       │
                    │  • Service Discovery                    │
                    └─────────────────────────────────────────┘
                                        │
        ┌──────────────┬────────────────┼────────────────┬──────────────┐
        │              │                │                │              │
┌───────▼──────┐ ┌─────▼─────┐ ┌────────▼────────┐ ┌─────▼─────┐ ┌─────▼─────┐
│ Auth Service │ │Doc Service│ │   OCR Service   │ │AI Service │ │RAG Service│
│  (Port 8001) │ │(Port 8002)│ │  (Port 8003)    │ │(Port 8004)│ │(Port 8005)│
└──────────────┘ └───────────┘ └─────────────────┘ └───────────┘ └───────────┘
        │              │                │                │              │
        └──────────────┼────────────────┼────────────────┼──────────────┘
                       │                │                │
        ┌──────────────▼────────────────▼────────────────▼──────────────┐
        │                    DATA LAYER                                  │
        ├─────────────────┬─────────────────┬─────────────────────────┤
        │  PostgreSQL     │   Vector DB     │      Redis Cache        │
        │  (Port 5432)    │  (Port 6333)    │      (Port 6379)        │
        │  • User Data    │  • Embeddings   │  • Sessions             │
        │  • Documents    │  • Semantic     │  • Queue Management     │
        │  • Metadata     │    Search       │  • Model Cache          │
        └─────────────────┴─────────────────┴─────────────────────────┘
```

## 🔧 Individual Service Specifications

### 1. **API Gateway Service** (Port 8080)
**Technology**: FastAPI + Traefik
**Responsibilities**:
- Request routing and load balancing
- Authentication and authorization
- Rate limiting and throttling
- API versioning and documentation
- Service health monitoring
- CORS and security headers

### 2. **Authentication Service** (Port 8001)
**Technology**: FastAPI + JWT + bcrypt
**Responsibilities**:
- User authentication and authorization
- JWT token generation and validation
- Role-based access control (RBAC)
- Session management
- Password policies and security

### 3. **Document Management Service** (Port 8002)
**Technology**: FastAPI + SQLAlchemy + MinIO
**Responsibilities**:
- Document upload and storage
- Metadata extraction and management
- Version control and history
- Document classification and tagging
- File format conversion

### 4. **OCR Processing Service** (Port 8003)
**Technology**: FastAPI + Tesseract + EasyOCR + PaddleOCR
**Responsibilities**:
- Multi-language OCR (English, Hindi, Malayalam, Tamil)
- Image preprocessing and enhancement
- Text extraction and confidence scoring
- Layout analysis and structure detection
- Handwritten text recognition

### 5. **AI/ML Processing Service** (Port 8004)
**Technology**: FastAPI + HuggingFace + Transformers
**Responsibilities**:
- Document classification and categorization
- Named Entity Recognition (NER)
- Sentiment analysis and emotion detection
- Text summarization and key point extraction
- Local model serving (no external APIs)

### 6. **RAG (Retrieval Augmented Generation) Service** (Port 8005)
**Technology**: FastAPI + LangChain + Local LLMs
**Responsibilities**:
- Document embedding generation
- Semantic search and retrieval
- Context-aware question answering
- Multi-document synthesis
- Local LLM inference (Llama, Mistral, Code Llama)

### 7. **Vector Database Service** (Port 8006)
**Technology**: Qdrant or ChromaDB
**Responsibilities**:
- Vector storage and indexing
- Similarity search and clustering
- Embedding version management
- Performance optimization
- Backup and recovery

### 8. **Notification Service** (Port 8007)
**Technology**: FastAPI + WebSocket + Redis PubSub
**Responsibilities**:
- Real-time notifications
- WebSocket connections management
- Push notifications to mobile apps
- Email and SMS notifications
- Event-driven messaging

### 9. **Gamification Service** (Port 8008)
**Technology**: FastAPI + PostgreSQL
**Responsibilities**:
- Achievement system and badges
- Leaderboards and competitions
- Points and rewards management
- Progress tracking
- Social features

### 10. **Analytics Service** (Port 8009)
**Technology**: FastAPI + ClickHouse + Grafana
**Responsibilities**:
- Usage analytics and reporting
- Performance monitoring
- Business intelligence
- Data visualization
- Audit logging

## 🗄️ Data Architecture

### Primary Database (PostgreSQL)
```sql
-- Core entities
- users (authentication, profiles, preferences)
- documents (metadata, versions, classifications)
- tasks (workflows, assignments, status)
- organizations (departments, hierarchies)
- audit_logs (compliance, security)
```

### Vector Database (Qdrant/ChromaDB)
```python
# Document embeddings for semantic search
- document_embeddings (vectors, metadata)
- user_query_embeddings (search history)
- knowledge_base (RAG contexts)
```

### Cache Layer (Redis)
```python
# High-performance caching
- session_cache (user sessions)
- model_cache (AI model outputs)
- api_cache (frequently accessed data)
- task_queue (background jobs)
```

## 🐳 Containerization Strategy

### Docker Architecture
```
metromind-microservices/
├── services/
│   ├── api-gateway/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── auth-service/
│   ├── document-service/
│   ├── ocr-service/
│   ├── ai-ml-service/
│   ├── rag-service/
│   ├── vector-db-service/
│   ├── notification-service/
│   ├── gamification-service/
│   └── analytics-service/
├── infrastructure/
│   ├── docker-compose.yml
│   ├── docker-compose.prod.yml
│   ├── kubernetes/
│   └── monitoring/
└── shared/
    ├── models/ (local AI models)
    ├── config/
    └── utils/
```

## 🤖 Local AI Model Strategy

### Model Categories

#### 1. **Text Processing Models**
- **BERT/RoBERTa**: Document classification
- **SpaCy Models**: Named Entity Recognition
- **Sentence Transformers**: Embedding generation
- **DistilBERT**: Lightweight text analysis

#### 2. **Language Models (RAG)**
- **Llama 2 7B/13B**: General purpose reasoning
- **Code Llama**: Technical document analysis
- **Mistral 7B**: Multilingual processing
- **OpenOrca**: Instruction following

#### 3. **OCR Models**
- **Tesseract 5.0**: Traditional OCR
- **EasyOCR**: Neural OCR for multiple languages
- **PaddleOCR**: Advanced layout analysis
- **TrOCR**: Transformer-based OCR

#### 4. **Malayalam Language Support**
```python
# Specialized Malayalam models
- malayalam-bert: Text classification
- indic-transformers: NER and sentiment
- tesseract-mal: Malayalam OCR
- transliteration models: Script conversion
```

## 🚀 Deployment Options

### 1. **Local Development**
```bash
# Single command deployment
docker-compose -f docker-compose.dev.yml up -d
```

### 2. **Production (Docker Swarm)**
```bash
# Scalable production deployment
docker stack deploy -c docker-compose.prod.yml metromind
```

### 3. **Kubernetes**
```bash
# Cloud-native deployment
kubectl apply -f kubernetes/
```

### 4. **Cloud Platforms**
- **AWS**: ECS, EKS, EC2
- **Azure**: Container Instances, AKS
- **Google Cloud**: GKE, Cloud Run
- **On-Premise**: Docker Swarm, OpenStack

## 🔒 Security Architecture

### Security Layers
1. **Network Security**: TLS/SSL, VPN, Firewall rules
2. **API Security**: JWT, OAuth2, Rate limiting
3. **Data Security**: Encryption at rest/transit, RBAC
4. **Container Security**: Image scanning, runtime protection
5. **Compliance**: Audit logging, data governance

### Data Privacy
- All AI processing happens locally
- No external API calls for sensitive documents
- End-to-end encryption
- GDPR/compliance ready

## 📈 Scalability Features

### Horizontal Scaling
- Stateless services design
- Load balancing across instances
- Auto-scaling based on metrics
- Database read replicas

### Performance Optimization
- Connection pooling
- Caching strategies
- Async processing
- Resource optimization

## 🔧 Development Workflow

### Local Development
```bash
# Start all services
make dev-up

# Run specific service
make service-up SERVICE=ocr-service

# Run tests
make test

# Deploy to production
make deploy ENV=production
```

### CI/CD Pipeline
1. **Code Commit** → Automated testing
2. **Build** → Docker images
3. **Test** → Integration tests
4. **Deploy** → Staging/Production
5. **Monitor** → Health checks

## 📊 Monitoring & Observability

### Metrics Collection
- **Prometheus**: Metrics collection
- **Grafana**: Visualization
- **Jaeger**: Distributed tracing
- **ELK Stack**: Log aggregation

### Health Checks
- Service availability
- Resource utilization
- API response times
- Error rates and alerts

This architecture ensures MetroMind is production-ready, scalable, and suitable for deployment anywhere while maintaining the highest security standards for confidential document processing.
