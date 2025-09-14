"""
MetroMind Services Startup Script
Starts all microservices in the correct order
"""

import subprocess
import sys
import time
import os
import signal
import threading
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config import print_config_summary, service_config

# Service definitions
SERVICES = [
    {
        'name': 'Auth Service',
        'script': 'services/auth_service.py',
        'port': service_config.auth_service_port,
        'essential': True
    },
    {
        'name': 'Document Service',
        'script': 'services/document_service.py',
        'port': service_config.document_service_port,
        'essential': True
    },
    {
        'name': 'OCR Service',
        'script': 'services/ocr_service.py',
        'port': service_config.ocr_service_port,
        'essential': True
    },
    {
        'name': 'AI/ML Service',
        'script': 'services/ai_ml_service.py',
        'port': service_config.ai_ml_service_port,
        'essential': True
    },
    {
        'name': 'Analytics Service',
        'script': 'services/analytics_service.py',
        'port': service_config.analytics_service_port,
        'essential': True
    },
    {
        'name': 'API Gateway Service',
        'script': 'services/api_gateway.py',
        'port': service_config.api_gateway_port,
        'essential': True
    },
    {
        'name': 'Integration Server',
        'script': 'services/integration_service.py',
        'port': service_config.integration_service_port,
        'essential': True
    },
    {
        'name': 'Notification Service',
        'script': 'services/notification_service.py',
        'port': service_config.notification_service_port,
        'essential': True
    },    
    {
        'name': 'Model Downloader Service',
        'script': 'services/model_downloader.py',
        'port': service_config.model_downloader_port,
        'essential': True
    },
    {
        'name': 'Search Service',
        'script': 'services/search_service.py',
        'port': service_config.search_service_port,
        'essential': True
    },
    {
        'name': 'RAG Chatbot Service',
        'script': 'services/rag_chatbot_service.py',
        'port': 8020,
        'essential': True
    },
    {
        'name': 'Task Management Service',
        'script': 'services/task_management_service.py',
        'port': 8021,
        'essential': True
    }
]

class ServiceManager:
    def __init__(self):
        self.processes = {}
        self.running = True
        
    def start_service(self, service):
        """Start a single service"""
        script_path = project_root / service['script']
        
        if not script_path.exists():
            logger.error(f"Service script not found: {script_path}")
            return None
        
        try:
            logger.info(f"Starting {service['name']} on port {service['port']}...")
            
            # Start the service
            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            self.processes[service['name']] = {
                'process': process,
                'service': service
            }
            
            # Start log monitoring in a separate thread
            log_thread = threading.Thread(
                target=self.monitor_service_logs,
                args=(service['name'], process),
                daemon=True
            )
            log_thread.start()
            
            # Give service time to start
            time.sleep(2)
            
            if process.poll() is None:
                logger.info(f"✅ {service['name']} started successfully")
                return process
            else:
                logger.error(f"❌ {service['name']} failed to start")
                return None
                
        except Exception as e:
            logger.error(f"Failed to start {service['name']}: {e}")
            return None
    
    def monitor_service_logs(self, service_name, process):
        """Monitor service logs"""
        try:
            for line in iter(process.stdout.readline, ''):
                if line:
                    print(f"[{service_name}] {line.strip()}")
        except:
            pass
    
    def start_all_services(self):
        """Start all services"""
        logger.info("🚀 Starting MetroMind Services...")
        print_config_summary()
        
        # Initialize database first
        self.init_database()
        
        # Start services in order
        for service in SERVICES:
            if not self.start_service(service):
                if service['essential']:
                    logger.error(f"Essential service {service['name']} failed to start. Stopping...")
                    self.stop_all_services()
                    return False
        
        logger.info("🎉 All services started successfully!")
        self.print_status()
        return True
    
    def init_database(self):
        """Initialize database"""
        try:
            logger.info("🗄️ Initializing database...")
            from database import db_manager
            
            # Create tables
            db_manager.create_tables()
            
            # Create admin user
            db_manager.create_admin_user()
            
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            logger.info("Please ensure PostgreSQL is running on the configured port")
            sys.exit(1)
    
    def print_status(self):
        """Print service status"""
        print("\n" + "="*60)
        print("METROMIND SERVICES STATUS")
        print("="*60)
        
        for name, info in self.processes.items():
            process = info['process']
            service = info['service']
            
            if process.poll() is None:
                status = "🟢 RUNNING"
            else:
                status = "🔴 STOPPED"
            
            print(f"{status} {name:<20} Port: {service['port']}")
        
        print("\n📊 Access Points:")
        print(f"• Auth Service: http://localhost:{service_config.auth_service_port}/docs")
        print(f"• Document Service: http://localhost:{service_config.document_service_port}/docs")
        print(f"• OCR Service: http://localhost:{service_config.ocr_service_port}/docs")
        print(f"• AI/ML Service: http://localhost:{service_config.ai_ml_service_port}/docs")
        print(f"• Analytics Service: http://localhost:{service_config.analytics_service_port}/docs")
        print(f"• API Gateway Service: http://localhost:{service_config.api_gateway_port}/docs")
        print(f"• Integration Service: http://localhost:{service_config.integration_service_port}/docs")
        print(f"• Notification Service: http://localhost:{service_config.notification_service_port}/docs")
        print(f"• Model Downloader Service: http://localhost:{service_config.model_downloader_port}/docs")
        print(f"• Search Service: http://localhost:{service_config.search_service_port}/docs")
        
        print("\n🔑 Default Admin Credentials:")
        print("Username: admin")
        print("Password: MetroAdmin@2024")
        
        print("\n⚠️ Press Ctrl+C to stop all services")
        print("="*60)
    
    def stop_all_services(self):
        """Stop all services"""
        logger.info("🛑 Stopping all services...")
        
        for name, info in self.processes.items():
            process = info['process']
            try:
                logger.info(f"Stopping {name}...")
                process.terminate()
                
                # Wait for graceful shutdown
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Force killing {name}...")
                    process.kill()
                
                logger.info(f"✅ {name} stopped")
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")
        
        self.processes.clear()
        logger.info("🎯 All services stopped")
    
    def monitor_services(self):
        """Monitor running services"""
        try:
            while self.running:
                time.sleep(5)
                
                # Check if any essential service has died
                for name, info in list(self.processes.items()):
                    process = info['process']
                    service = info['service']
                    
                    if process.poll() is not None:
                        logger.error(f"💀 {name} has died unexpectedly")
                        
                        if service['essential']:
                            logger.error("Essential service died. Restarting...")
                            # Try to restart
                            new_process = self.start_service(service)
                            if not new_process:
                                logger.error("Failed to restart essential service. Shutting down...")
                                self.running = False
                                break
                        
        except KeyboardInterrupt:
            self.running = False
        except Exception as e:
            logger.error(f"Error in service monitoring: {e}")
            self.running = False

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal")
    manager.running = False

def main():
    """Main entry point"""
    global manager
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    manager = ServiceManager()
    
    try:
        # Start all services
        if manager.start_all_services():
            # Monitor services
            manager.monitor_services()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Cleanup
        manager.stop_all_services()
        logger.info("👋 MetroMind Services shutdown complete")

if __name__ == "__main__":
    main()
