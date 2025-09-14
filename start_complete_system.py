"""
MetroMind Complete System Startup Script
Starts all services including automation, email integration, task management, and chatbot
"""

import asyncio
import subprocess
import time
import signal
import sys
import os
from pathlib import Path
import requests
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetroMindOrchestrator:
    """Complete MetroMind system orchestrator with all services"""
    
    def __init__(self):
        self.services = {}
        self.running = True
        
        # Service definitions with automation services
        self.service_definitions = {
            "api_gateway": {
                "script": "services/api_gateway.py",
                "port": 8000,
                "priority": 1,
                "description": "Central API Gateway and Routing"
            },
            "auth_service": {
                "script": "services/auth_service.py", 
                "port": 8005,
                "priority": 1,
                "description": "Authentication and Authorization"
            },
            "document_service": {
                "script": "services/document_service.py",
                "port": 8003,
                "priority": 2,
                "description": "Document Management and Storage"
            },
            "ocr_service": {
                "script": "services/ocr_service.py",
                "port": 8001,
                "priority": 3,
                "description": "OCR and Image Processing"
            },
            "ai_ml_service": {
                "script": "services/ai_ml_service.py",
                "port": 8004,
                "priority": 3,
                "description": "AI/ML Processing and Analysis"
            },
            "search_service": {
                "script": "services/search_service.py",
                "port": 8007,
                "priority": 4,
                "description": "Vector Search and Semantic Search"
            },
            "notification_service": {
                "script": "services/notification_service.py",
                "port": 8006,
                "priority": 4,
                "description": "Real-time Notifications and Alerts"
            },
            "analytics_service": {
                "script": "services/analytics_service.py",
                "port": 8018,
                "priority": 5,
                "description": "Analytics and Reporting"
            },
            "integration_service": {
                "script": "services/integration_service.py",
                "port": 8008,
                "priority": 5,
                "description": "External System Integrations"
            },
            # New automation services
            "email_integration": {
                "script": "services/email_integration_service.py",
                "port": 8009,
                "priority": 3,
                "description": "Email Integration and Automation"
            },
            "task_management": {
                "script": "services/task_management_service.py",
                "port": 8010,
                "priority": 2,
                "description": "Task Management and Assignment"
            },
            "rag_chatbot": {
                "script": "services/rag_chatbot_service.py",
                "port": 8011,
                "priority": 4,
                "description": "RAG-based Intelligent Chatbot"
            }
        }
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        self.stop_all_services()
        sys.exit(0)
    
    def check_port_availability(self, port):
        """Check if port is available"""
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) != 0
    
    def start_service(self, name, config):
        """Start a single service"""
        try:
            script_path = Path(config["script"])
            
            if not script_path.exists():
                logger.error(f"Service script not found: {script_path}")
                return False
            
            # Check port availability
            if not self.check_port_availability(config["port"]):
                logger.warning(f"Port {config['port']} already in use for {name}")
                return False
            
            # Start the service
            cmd = [sys.executable, str(script_path)]
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.getcwd()
            )
            
            self.services[name] = {
                "process": process,
                "config": config,
                "start_time": time.time(),
                "status": "starting"
            }
            
            logger.info(f"✅ Started {name} on port {config['port']} - {config['description']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start {name}: {e}")
            return False
    
    def check_service_health(self, name, config):
        """Check if service is healthy"""
        try:
            response = requests.get(f"http://localhost:{config['port']}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def start_all_services(self):
        """Start all services in priority order"""
        logger.info("🚀 Starting MetroMind Complete System with Automation...")
        
        # Create necessary directories
        directories = [
            "data/uploads", "data/temp", "data/processed", "data/watch_folder",
            "data/temp/email_attachments", "data/logs", "models/cache"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        # Group services by priority
        priority_groups = {}
        for name, config in self.service_definitions.items():
            priority = config["priority"]
            if priority not in priority_groups:
                priority_groups[priority] = []
            priority_groups[priority].append((name, config))
        
        # Start services by priority
        for priority in sorted(priority_groups.keys()):
            logger.info(f"\n📋 Starting Priority {priority} Services:")
            
            for name, config in priority_groups[priority]:
                if self.start_service(name, config):
                    # Wait a moment between services
                    time.sleep(2)
            
            # Wait for services in this priority to be ready
            logger.info(f"⏳ Waiting for Priority {priority} services to be ready...")
            
            max_wait = 30  # 30 seconds max wait
            wait_time = 0
            
            while wait_time < max_wait:
                all_ready = True
                for name, config in priority_groups[priority]:
                    if name in self.services:
                        if not self.check_service_health(name, config):
                            all_ready = False
                            break
                        else:
                            self.services[name]["status"] = "running"
                
                if all_ready:
                    logger.info(f"✅ All Priority {priority} services are ready!")
                    break
                
                time.sleep(2)
                wait_time += 2
            
            if wait_time >= max_wait:
                logger.warning(f"⚠️ Some Priority {priority} services may not be fully ready")
        
        self.print_service_status()
        self.print_access_information()
    
    def stop_all_services(self):
        """Stop all services gracefully"""
        logger.info("🛑 Stopping all services...")
        
        for name, service_info in self.services.items():
            try:
                process = service_info["process"]
                if process.poll() is None:  # Process is still running
                    process.terminate()
                    
                    # Wait for graceful shutdown
                    try:
                        process.wait(timeout=10)
                        logger.info(f"✅ Stopped {name}")
                    except subprocess.TimeoutExpired:
                        process.kill()
                        logger.warning(f"⚠️ Force killed {name}")
                        
            except Exception as e:
                logger.error(f"❌ Error stopping {name}: {e}")
        
        self.services.clear()
        logger.info("🏁 All services stopped")
    
    def monitor_services(self):
        """Monitor service health and restart if needed"""
        while self.running:
            try:
                failed_services = []
                
                for name, service_info in self.services.items():
                    process = service_info["process"]
                    config = service_info["config"]
                    
                    # Check if process is still running
                    if process.poll() is not None:
                        logger.error(f"❌ Service {name} has stopped unexpectedly")
                        failed_services.append((name, config))
                        continue
                    
                    # Check health endpoint
                    if not self.check_service_health(name, config):
                        logger.warning(f"⚠️ Service {name} health check failed")
                
                # Restart failed services
                for name, config in failed_services:
                    logger.info(f"🔄 Restarting {name}...")
                    del self.services[name]
                    self.start_service(name, config)
                
                # Wait before next check
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error in service monitoring: {e}")
                time.sleep(10)
    
    def print_service_status(self):
        """Print current service status"""
        logger.info("\n" + "="*80)
        logger.info("📊 METROMIND COMPLETE SYSTEM STATUS")
        logger.info("="*80)
        
        for name, service_info in self.services.items():
            config = service_info["config"]
            status = service_info["status"]
            uptime = int(time.time() - service_info["start_time"])
            
            status_icon = "🟢" if status == "running" else "🟡"
            logger.info(f"{status_icon} {name:<20} Port: {config['port']:<5} Status: {status:<10} Uptime: {uptime}s")
            logger.info(f"   └─ {config['description']}")
        
        logger.info("="*80)
    
    def print_access_information(self):
        """Print access information"""
        logger.info("\n" + "🌐 ACCESS INFORMATION")
        logger.info("="*50)
        logger.info("🎮 Main Dashboard:     http://localhost:3000")
        logger.info("🔗 API Gateway:       http://localhost:8000")
        logger.info("📚 API Documentation: http://localhost:8000/docs")
        logger.info("📊 Analytics:         http://localhost:8000/analytics/dashboard")
        logger.info("💬 Chatbot:           http://localhost:8000/chat")
        logger.info("📧 Email Integration: http://localhost:8009")
        logger.info("📋 Task Management:   http://localhost:8010")
        logger.info("🔍 Search Service:    http://localhost:8007")
        logger.info("="*50)
        
        logger.info("\n🎯 AUTOMATION FEATURES ACTIVE:")
        logger.info("✅ Automatic document processing from file uploads")
        logger.info("✅ Email integration for document submissions")
        logger.info("✅ Intelligent task auto-assignment")
        logger.info("✅ Real-time notifications and alerts")
        logger.info("✅ RAG-based chatbot for document queries")
        logger.info("✅ Vector search and semantic document retrieval")
        logger.info("✅ Multi-language OCR processing")
        logger.info("✅ AI-powered document categorization and summarization")
        
        logger.info("\n📝 DEFAULT CREDENTIALS:")
        logger.info("Station Controller: username=station_controller, password=StationCtrl123")
        logger.info("Finance Manager:    username=finance_manager, password=FinanceManager123")
        logger.info("Maintenance Head:   username=maintenance_head, password=MaintenanceHead123")
        logger.info("Admin:              username=admin, password=Admin123")
    
    async def run(self):
        """Run the complete system"""
        try:
            self.setup_signal_handlers()
            self.start_all_services()
            
            logger.info("\n🎉 MetroMind Complete System is now running!")
            logger.info("📱 Open http://localhost:3000 to access the dashboard")
            logger.info("💬 Try the chatbot to ask questions about your documents")
            logger.info("📧 Send emails with attachments to test automation")
            logger.info("\nPress Ctrl+C to stop all services\n")
            
            # Start monitoring in background
            monitor_task = asyncio.create_task(asyncio.to_thread(self.monitor_services))
            
            # Keep main thread alive
            while self.running:
                await asyncio.sleep(1)
                
            monitor_task.cancel()
            
        except KeyboardInterrupt:
            logger.info("🛑 Shutdown requested by user")
        except Exception as e:
            logger.error(f"❌ System error: {e}")
        finally:
            self.stop_all_services()

def main():
    """Main entry point"""
    orchestrator = MetroMindOrchestrator()
    asyncio.run(orchestrator.run())

if __name__ == "__main__":
    main()