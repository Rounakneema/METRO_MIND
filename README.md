# MetroMind MVP - Intelligent Document Management System

## 🚀 Overview

MetroMind is a revolutionary AI-powered document management system designed for KMRL (Kochi Metro Rail Limited) to handle thousands of multilingual documents with intelligent processing, real-time notifications, and semantic search capabilities.

### ✨ Key Features

- **🤖 AI-Powered Processing**: Advanced OCR with multi-language support (English, Malayalam, Hindi, Tamil)
- **🔍 Semantic Search**: Vector-based document search using embeddings
- **⚡ Real-time Notifications**: WebSocket-based live updates and email alerts
- **🎯 Smart Classification**: Automatic document categorization (Safety, Maintenance, Finance, Operations, etc.)
- **🔐 Secure Authentication**: JWT-based auth with role-based access control
- **📊 Analytics Dashboard**: Comprehensive insights and performance metrics
- **🌐 Multi-language Support**: Built for Indian languages and bilingual content

## 🏗️ Architecture

### Microservices Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend UI   │◄───┤   API Gateway   │◄───┤   Load Balancer │
│   (React/Next)  │    │   (Port 8000)   │    │     (Nginx)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                    ┌───────────┼───────────┐
                    │           │           │
            ┌───────▼──────┐   │   ┌───────▼──────┐
            │ User Service │   │   │Document Svc  │
            │ (Port 8005)  │   │   │ (Port 8003)  │
            └──────────────┘   │   └──────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                     │                     │
┌───────▼──────┐    ┌────────▼──────┐    ┌────────▼──────┐
│   OCR Svc    │    │   AI/ML Svc   │    │ Notification  │
│ (Port 8001)  │    │ (Port 8004)   │    │   Service     │
└──────────────┘    └───────────────┘    │ (Port 8006)   │
                                         └───────────────┘
        │                     │                     │
┌───────▼──────┐    ┌────────▼──────┐    ┌────────▼──────┐
│   RAG Svc    │    │Vector Search  │    │   Monitoring  │
│ (Port 8002)  │    │ (Port 8007)   │    │   Stack       │
└──────────────┘    └───────────────┘    └───────────────┘
```

### Service Details

| Service | Port | Description | Key Technologies |
|---------|------|-------------|------------------|
| **API Gateway** | 8000 | Main entry point, request routing | FastAPI, JWT |
| **OCR Service** | 8001 | Multi-language OCR processing | Tesseract, OpenCV |
| **RAG Service** | 8002 | AI chat and document Q&A | OpenAI, ChromaDB |
| **Document Service** | 8003 | File upload and management | FastAPI, PostgreSQL |
| **AI/ML Service** | 8004 | Model management and inference | PyTorch, Transformers |
| **User Service** | 8005 | Authentication and user management | JWT, bcrypt |
| **Notification Service** | 8006 | Real-time notifications | WebSocket, SMTP |
| **Vector Search Service** | 8007 | Semantic document search | FAISS, SentenceTransformers |

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** (v2.0+)
- **Git**
- **8GB RAM** (minimum), 16GB recommended
- **10GB free disk space**

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd metromind-mvp
   ```

2. **Start the services**:
   ```bash
   cd infrastructure
   docker-compose up -d
   ```

3. **Wait for services to be healthy**:
   ```bash
   docker-compose ps
   ```

### Windows Quick Start

For Windows users, use the PowerShell deployment script:

```powershell
# Run as Administrator
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\infrastructure\deploy.ps1
```

## 📊 API Documentation

### Service Endpoints

Once running, access API documentation:

- **API Gateway**: http://localhost:8000/docs
- **User Service**: http://localhost:8005/docs
- **Document Service**: http://localhost:8003/docs
- **OCR Service**: http://localhost:8001/docs
- **AI/ML Service**: http://localhost:8004/docs
- **Notification Service**: http://localhost:8006/docs
- **Vector Search Service**: http://localhost:8007/docs

### Frontend & Monitoring

- **Admin Dashboard**: http://localhost:3000
- **Grafana Dashboard**: http://localhost:3001 (admin/secure_password_123)
- **Prometheus Metrics**: http://localhost:9090
- **Jaeger Tracing**: http://localhost:16686

## 🐳 Docker Services

Monitor service health:
```bash
docker-compose ps
docker-compose logs -f [service-name]
```

### Scaling Services

```bash
# Scale document processing
docker-compose up -d --scale document-service=3

# Scale AI/ML service  
docker-compose up -d --scale ai-ml-service=2
```

## 🔍 Troubleshooting

### Common Issues

1. **Services not starting**:
   ```bash
   docker-compose down
   docker-compose up -d --force-recreate
   ```

2. **Database connection errors**:
   ```bash
   docker-compose exec postgres pg_isready -U metromind_user
   ```

3. **Memory issues**:
   - Increase Docker memory allocation to 8GB+
   - Reduce `MAX_BATCH_SIZE` in AI/ML service

### Logs

```bash
# View all logs
docker-compose logs

# Follow specific service logs
docker-compose logs -f api-gateway
docker-compose logs -f ai-ml-service

# View error logs only
docker-compose logs | grep ERROR
```

## 🔧 Project Structure

```
metromind-mvp/
├── services/                    # Microservices
│   ├── api-gateway/            # API Gateway (Port 8000)
│   ├── document-service/       # Document Management (Port 8003)
│   ├── ocr-service/           # OCR Processing (Port 8001)
│   ├── rag-service/           # AI Chat & Q&A (Port 8002)
│   ├── ai-ml-service/         # ML Models (Port 8004)
│   ├── user-service/          # Authentication (Port 8005)
│   ├── notification-service/   # Real-time Notifications (Port 8006)
│   └── vector-search-service/ # Semantic Search (Port 8007)
├── shared/                     # Shared utilities
│   ├── database.py            # Database models
│   ├── auth.py               # Authentication utilities
│   └── utils/               # Common utilities
├── infrastructure/            # Infrastructure & deployment
│   ├── docker-compose.yml    # Development orchestration
│   ├── docker-compose.prod.yml # Production orchestration
│   ├── deploy.ps1           # Windows deployment script
│   ├── .env.template        # Environment variables template
│   └── monitoring/          # Grafana, Prometheus configs
├── frontend/                 # Web interface
├── mobile_app/              # Flutter mobile app
└── old-files/               # Legacy/unused files
```

## 🎯 Key Implemented Features

### ✅ Complete Implementation Status

**Core Services** (8/8 Implemented):
- ✅ **API Gateway**: Request routing, authentication
- ✅ **User Service**: JWT auth, user management, sessions
- ✅ **Document Service**: File upload, processing pipeline
- ✅ **OCR Service**: Multi-language text extraction
- ✅ **AI/ML Service**: Model management, embeddings, classification
- ✅ **RAG Service**: AI chat, document Q&A
- ✅ **Notification Service**: WebSocket notifications, email alerts
- ✅ **Vector Search Service**: FAISS-based semantic search

**Infrastructure** (100% Complete):
- ✅ **Docker Compose**: Full orchestration with 8 services
- ✅ **Database**: PostgreSQL with comprehensive schema
- ✅ **Caching**: Redis for sessions and caching
- ✅ **Monitoring**: Prometheus, Grafana, Jaeger
- ✅ **Load Balancing**: Nginx configuration

**Security & Authentication**:
- ✅ **JWT Authentication**: Token-based auth with refresh
- ✅ **Password Security**: bcrypt hashing
- ✅ **Session Management**: Redis-backed sessions
- ✅ **Role-based Access**: Admin, employee, manager roles
- ✅ **Audit Logging**: Complete action tracking

**AI & ML Capabilities**:
- ✅ **Multi-language OCR**: English, Malayalam, Hindi, Tamil
- ✅ **Smart Classification**: Document type detection
- ✅ **Semantic Search**: Vector similarity using FAISS
- ✅ **Embeddings**: SentenceTransformers integration
- ✅ **Model Management**: Dynamic model loading/unloading

**Real-time Features**:
- ✅ **WebSocket Notifications**: Live updates
- ✅ **Email Alerts**: SMTP integration with templates
- ✅ **Broadcast Messages**: Department/role-based notifications
- ✅ **Connection Management**: Multiple user sessions

## 💾 Database Schema

The system uses a comprehensive PostgreSQL schema with the following key tables:

- **users**: User accounts and profiles
- **documents**: Document metadata and status
- **ocr_jobs**: OCR processing tracking
- **notifications**: User notifications
- **user_sessions**: Authentication sessions
- **ai_models**: ML model registry
- **audit_logs**: Security and action logging
- **vector_embeddings**: Document embeddings for search

## 🔒 Security Features

- **JWT Authentication** with configurable expiration
- **bcrypt Password Hashing** with salt
- **Rate Limiting** on API endpoints
- **Input Validation** and sanitization
- **Audit Logging** for all user actions
- **Role-based Access Control**
- **Token Blacklisting** for secure logout
- **Session Management** with Redis

## 📈 Performance Metrics

### Recommended System Requirements

- **Minimum**: 8GB RAM, 4 CPU cores, 20GB storage
- **Recommended**: 16GB RAM, 8 CPU cores, 100GB SSD
- **Production**: 32GB RAM, 16 CPU cores, 500GB SSD

### Performance Targets

- **Response Time**: < 2s for document upload
- **OCR Processing**: < 30s for standard documents
- **Search Latency**: < 500ms for vector search
- **Memory Usage**: < 8GB total system usage

## 🧪 Testing the System

### Quick Functionality Test

1. **Start Services**:
   ```bash
   cd infrastructure && docker-compose up -d
   ```

2. **Register a User**:
   ```bash
   curl -X POST http://localhost:8005/auth/register \
     -H "Content-Type: application/json" \
     -d '{"email":"test@kmrl.gov.in","password":"Test123","first_name":"Test","last_name":"User","department":"operations"}'
   ```

3. **Upload a Document**:
   ```bash
   # Get token from login response, then:
   curl -X POST http://localhost:8003/documents/upload \
     -H "Authorization: Bearer <token>" \
     -F "file=@sample_document.pdf"
   ```

4. **Search Documents**:
   ```bash
   curl -X POST http://localhost:8007/search \
     -H "Content-Type: application/json" \
     -d '{"query":"safety procedures","limit":10}'
   ```

## 📞 Support & Maintenance

### Health Monitoring

All services include health check endpoints at `/health`. Monitor system health:

```bash
# Check all services
for service in api-gateway user-service document-service ocr-service ai-ml-service rag-service notification-service vector-search-service; do
  echo "Checking $service..."
  curl -s http://localhost:800$(($(echo $service | wc -c) % 8))/health | jq .status
done
```

### Backup Strategy

The system includes automated backup capabilities:
- **Database**: PostgreSQL daily backups
- **Documents**: File system snapshots
- **Configuration**: Environment and Docker configs
- **Models**: ML model versioning

## 📄 License

This project is proprietary software developed for KMRL (Kochi Metro Rail Limited).

---

**Built with ❤️ for KMRL** - Revolutionizing document management with AI

*Last Updated: December 2024*
