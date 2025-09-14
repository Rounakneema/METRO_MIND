"""
MetroMind API Gateway Service
Central routing, authentication, and load balancing
"""

from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
import asyncio
from typing import Dict, List, Optional
import logging
import time
import random
from datetime import datetime, timezone
import json

# Import our models and config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import service_config, get_service_urls
from utils.logging_utils import setup_logger

# Setup
logger = setup_logger(__name__)
app = FastAPI(
    title="MetroMind API Gateway",
    description="Central API gateway for all MetroMind services",
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

# Service registry
SERVICE_REGISTRY = {
    "auth": {
        "instances": [f"http://localhost:{service_config.auth_service_port}"],
        "health_endpoint": "/health",
        "priority": 1
    },
    "documents": {
        "instances": [f"http://localhost:{service_config.document_service_port}"],
        "health_endpoint": "/health",
        "priority": 1
    },
    "ocr": {
        "instances": [f"http://localhost:{service_config.ocr_service_port}"],
        "health_endpoint": "/health",
        "priority": 2
    },
    "ai": {
        "instances": [f"http://localhost:{service_config.ai_ml_service_port}"],
        "health_endpoint": "/health",
        "priority": 2
    },
    "search": {
        "instances": [f"http://localhost:{service_config.search_service_port}"],
        "health_endpoint": "/health",
        "priority": 3
    },
    "notifications": {
        "instances": [f"http://localhost:{service_config.notification_service_port}"],
        "health_endpoint": "/health",
        "priority": 3
    },
    "integrations": {
        "instances": [f"http://localhost:{service_config.integration_service_port}"],
        "health_endpoint": "/health",
        "priority": 3
    },
    "analytics": {
        "instances": [f"http://localhost:{service_config.analytics_service_port}"],
        "health_endpoint": "/health",
        "priority": 3
    },
    "email": {
        "instances": ["http://localhost:8009"],
        "health_endpoint": "/health",
        "priority": 3
    },
    "tasks": {
        "instances": ["http://localhost:8010"],
        "health_endpoint": "/health",
        "priority": 2
    },
    "chatbot": {
        "instances": ["http://localhost:8011"],
        "health_endpoint": "/health",
        "priority": 2
    }
}

# Service health tracking
service_health = {}
last_health_check = {}

# HTTP client
http_client = httpx.AsyncClient(timeout=30.0)

class LoadBalancer:
    """Simple round-robin load balancer with health checking"""
    
    def __init__(self):
        self.current_instance = {}
        
    def get_instance(self, service_name: str) -> Optional[str]:
        """Get next available instance for service"""
        if service_name not in SERVICE_REGISTRY:
            return None
            
        instances = SERVICE_REGISTRY[service_name]["instances"]
        healthy_instances = [
            instance for instance in instances 
            if service_health.get(instance, False)
        ]
        
        if not healthy_instances:
            # Fallback to any instance if none are healthy
            healthy_instances = instances
            
        if not healthy_instances:
            return None
            
        # Round-robin selection
        current = self.current_instance.get(service_name, 0)
        instance = healthy_instances[current % len(healthy_instances)]
        self.current_instance[service_name] = current + 1
        
        return instance

load_balancer = LoadBalancer()

async def check_service_health():
    """Background task to check service health"""
    while True:
        try:
            for service_name, config in SERVICE_REGISTRY.items():
                for instance in config["instances"]:
                    try:
                        response = await http_client.get(
                            f"{instance}{config['health_endpoint']}",
                            timeout=5.0
                        )
                        service_health[instance] = response.status_code == 200
                        last_health_check[instance] = datetime.now(timezone.utc)
                    except Exception as e:
                        service_health[instance] = False
                        logger.warning(f"Health check failed for {instance}: {e}")
                        
            await asyncio.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logger.error(f"Health check loop error: {e}")
            await asyncio.sleep(60)

# Start health checking on startup
@app.on_event("startup")
async def startup_event():
    """Start background tasks"""
    asyncio.create_task(check_service_health())
    logger.info("API Gateway started with service health monitoring")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await http_client.aclose()

def get_service_from_path(path: str) -> Optional[str]:
    """Extract service name from request path"""
    path = path.lstrip('/')
    
    # Define path mappings
    path_mappings = {
        'auth': ['auth', 'login', 'register', 'profile', 'users', 'admin'],
        'documents': ['documents', 'upload', 'files'],
        'ocr': ['ocr', 'extract-text', 'process-document'],
        'ai': ['ai', 'analyze-document', 'analyze-text', 'generate-embeddings', 'models'],
        'search': ['search', 'vector-search', 'semantic-search'],
        'notifications': ['notifications', 'alerts', 'websocket'],
        'integrations': ['integrations', 'email', 'whatsapp', 'sharepoint'],
        'analytics': ['analytics', 'reports', 'metrics', 'insights']
    }
    
    path_parts = path.split('/')
    first_part = path_parts[0] if path_parts else ""
    
    for service, patterns in path_mappings.items():
        if first_part in patterns:
            return service
    
    # Default routing based on first path segment
    if first_part in SERVICE_REGISTRY:
        return first_part
        
    return None

async def proxy_request(request: Request, service_name: str) -> Response:
    """Proxy request to appropriate service"""
    instance = load_balancer.get_instance(service_name)
    if not instance:
        raise HTTPException(
            status_code=503,
            detail=f"Service {service_name} is not available"
        )
    
    # Strip service prefix from path for backend routing
    original_path = request.url.path.lstrip('/')
    path_parts = original_path.split('/')
    
    # If path starts with service name, remove it
    if path_parts and path_parts[0] == service_name:
        # Remove service prefix: /auth/login -> /login
        backend_path = '/' + '/'.join(path_parts[1:]) if len(path_parts) > 1 else '/'
    else:
        # Keep original path if no service prefix
        backend_path = '/' + original_path
    
    # Prepare request URL
    url = f"{instance}{backend_path}"
    if request.url.query:
        url += f"?{request.url.query}"
    
    headers = dict(request.headers)
    # Remove host header to avoid conflicts
    headers.pop('host', None)
    
    try:
        # Handle different HTTP methods
        if request.method == "GET":
            response = await http_client.get(url, headers=headers)
        elif request.method == "POST":
            body = await request.body()
            response = await http_client.post(
                url, 
                headers=headers, 
                content=body
            )
        elif request.method == "PUT":
            body = await request.body()
            response = await http_client.put(
                url, 
                headers=headers, 
                content=body
            )
        elif request.method == "DELETE":
            response = await http_client.delete(url, headers=headers)
        else:
            raise HTTPException(
                status_code=405, 
                detail=f"Method {request.method} not allowed"
            )
        
        # Log request
        logger.info(f"Proxied {request.method} {url} -> {response.status_code}")
        
        # Return response
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.headers.get("content-type")
        )
        
    except httpx.TimeoutException:
        logger.error(f"Timeout proxying to {url}")
        raise HTTPException(status_code=504, detail="Gateway timeout")
    except Exception as e:
        logger.error(f"Error proxying to {url}: {e}")
        raise HTTPException(status_code=502, detail="Bad gateway")

@app.get("/")
async def root():
    """API Gateway root endpoint"""
    return {
        "service": "MetroMind API Gateway",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": list(SERVICE_REGISTRY.keys())
    }

@app.get("/health")
async def health_check():
    """Gateway health check"""
    healthy_services = 0
    total_services = 0
    
    service_status = {}
    
    for service_name, config in SERVICE_REGISTRY.items():
        service_healthy = False
        for instance in config["instances"]:
            if service_health.get(instance, False):
                service_healthy = True
                break
        
        service_status[service_name] = "healthy" if service_healthy else "unhealthy"
        if service_healthy:
            healthy_services += 1
        total_services += 1
    
    overall_status = "healthy" if healthy_services >= total_services * 0.7 else "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": service_status,
        "healthy_services": healthy_services,
        "total_services": total_services,
        "uptime_seconds": time.time() - startup_time
    }

@app.get("/services")
async def list_services():
    """List all available services"""
    services_info = {}
    
    for service_name, config in SERVICE_REGISTRY.items():
        healthy_instances = [
            instance for instance in config["instances"]
            if service_health.get(instance, False)
        ]
        
        services_info[service_name] = {
            "instances": config["instances"],
            "healthy_instances": healthy_instances,
            "priority": config["priority"],
            "health_endpoint": config["health_endpoint"],
            "status": "healthy" if healthy_instances else "unhealthy"
        }
    
    return {
        "services": services_info,
        "total_services": len(SERVICE_REGISTRY)
    }

# Email Integration Routes
@app.api_route("/email/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_email(request: Request, path: str):
    """Proxy requests to email integration service"""
    return await proxy_request(request, "email", path)

# Task Management Routes
@app.api_route("/tasks/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_tasks(request: Request, path: str):
    """Proxy requests to task management service"""
    return await proxy_request(request, "tasks", path)

# Chatbot Routes
@app.api_route("/chat/{path:path}", methods=["GET", "POST"])
async def proxy_chatbot(request: Request, path: str):
    """Proxy requests to chatbot service"""
    return await proxy_request(request, "chatbot", path)

# Catch-all route for proxying
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def catch_all(request: Request, path: str):
    """Catch-all route to proxy requests to appropriate services"""
    
    # Skip gateway-specific paths
    if path in ["", "health", "services"]:
        raise HTTPException(status_code=404, detail="Not found")
    
    service_name = get_service_from_path(path)
    if not service_name:
        raise HTTPException(
            status_code=404,
            detail=f"No service found for path: /{path}"
        )
    
    return await proxy_request(request, service_name)

# WebSocket proxy for real-time features
from fastapi import WebSocket

@app.websocket("/ws/{service_name}")
async def websocket_proxy(websocket: WebSocket, service_name: str):
    """WebSocket proxy for real-time communications"""
    await websocket.accept()
    
    instance = load_balancer.get_instance(service_name)
    if not instance:
        await websocket.close(code=1003, reason="Service not available")
        return
    
    # Note: WebSocket proxying is complex and might need additional libraries
    # For now, we'll implement basic WebSocket support
    try:
        while True:
            data = await websocket.receive_text()
            # Echo back for now - in production, proxy to actual service
            await websocket.send_text(f"Echo from {service_name}: {data}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close()

# Metrics endpoint
@app.get("/metrics")
async def metrics():
    """Prometheus-style metrics"""
    metrics_data = []
    
    # Service health metrics
    for service_name, config in SERVICE_REGISTRY.items():
        for instance in config["instances"]:
            health_status = 1 if service_health.get(instance, False) else 0
            metrics_data.append(
                f'metromind_service_health{{service="{service_name}",instance="{instance}"}} {health_status}'
            )
    
    # Gateway metrics
    metrics_data.append(f'metromind_gateway_uptime_seconds {time.time() - startup_time}')
    
    return Response(
        content="\n".join(metrics_data),
        media_type="text/plain"
    )

# Store startup time
startup_time = time.time()

if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting API Gateway on port {service_config.api_gateway_port}")
    logger.info(f"Registered services: {list(SERVICE_REGISTRY.keys())}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=service_config.api_gateway_port,
        log_level="info"
    )
