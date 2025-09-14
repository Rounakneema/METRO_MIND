"""
MetroMind Automation Setup Script
Quick setup for email and WhatsApp automation
"""

import requests
import json
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

def setup_email_automation():
    """Setup email automation with sample configuration"""
    
    print("=== Email Integration Setup ===")
    
    # Sample email configuration (user should modify these)
    email_config = {
        "name": "KMRL Email Automation",
        "description": "Automatically process documents from emails",
        "server": "imap.gmail.com",  # or your email server
        "port": 993,
        "username": "your-email@gmail.com",  # CHANGE THIS
        "password": "your-app-password",     # CHANGE THIS
        "use_ssl": True,
        "folder": "INBOX",
        "mark_as_read": True,
        "process_attachments": True,
        "max_emails_per_sync": 20,
        "sync_interval_minutes": 5,
        "auto_sync": True
    }
    
    print("Sample Email Configuration:")
    print(f"Server: {email_config['server']}")
    print(f"Username: {email_config['username']}")
    print("⚠️  Please update the username and password in the script before running!")
    print("")
    
    # Ask user if they want to proceed
    response = input("Do you want to set up email automation with these settings? (y/n): ")
    
    if response.lower() != 'y':
        print("Email automation setup skipped.")
        return False
    
    try:
        # Call the integration service to setup email automation
        setup_response = requests.post(
            "http://localhost:8009/setup-automation",
            json={"email_config": email_config},
            timeout=30
        )
        
        if setup_response.status_code == 200:
            result = setup_response.json()
            print(f"✅ Email automation setup successful: {result['message']}")
            return True
        else:
            print(f"❌ Email automation setup failed: {setup_response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up email automation: {e}")
        return False

def setup_whatsapp_automation():
    """Setup WhatsApp automation with sample configuration"""
    
    print("=== WhatsApp Integration Setup ===")
    
    # Sample WhatsApp configuration (user should modify these)
    whatsapp_config = {
        "name": "KMRL WhatsApp Automation",
        "description": "Automatically process documents from WhatsApp Business",
        "phone_number_id": "your-phone-number-id",     # CHANGE THIS
        "access_token": "your-whatsapp-access-token",  # CHANGE THIS
        "webhook_verify_token": "your-webhook-token",  # CHANGE THIS
        "webhook_url": "https://your-domain.com/webhooks/whatsapp",  # CHANGE THIS
        "sync_interval_minutes": 2,
        "auto_sync": True
    }
    
    print("Sample WhatsApp Configuration:")
    print(f"Phone Number ID: {whatsapp_config['phone_number_id']}")
    print(f"Webhook URL: {whatsapp_config['webhook_url']}")
    print("⚠️  Please update the configuration values in the script before running!")
    print("")
    
    # Ask user if they want to proceed
    response = input("Do you want to set up WhatsApp automation with these settings? (y/n): ")
    
    if response.lower() != 'y':
        print("WhatsApp automation setup skipped.")
        return False
    
    try:
        # Call the integration service to setup WhatsApp automation
        setup_response = requests.post(
            "http://localhost:8009/setup-automation",
            json={"whatsapp_config": whatsapp_config},
            timeout=30
        )
        
        if setup_response.status_code == 200:
            result = setup_response.json()
            print(f"✅ WhatsApp automation setup successful: {result['message']}")
            return True
        else:
            print(f"❌ WhatsApp automation setup failed: {setup_response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up WhatsApp automation: {e}")
        return False

def check_services():
    """Check if required services are running"""
    required_services = [
        ("Integration Service", "http://localhost:8009/health"),
        ("AI/ML Service", "http://localhost:8004/health"),
        ("Notification Service", "http://localhost:8006/health")
    ]
    
    print("=== Checking Required Services ===")
    all_running = True
    
    for service_name, health_url in required_services:
        try:
            response = requests.get(health_url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {service_name}: Running")
            else:
                print(f"❌ {service_name}: Not responding properly")
                all_running = False
        except Exception as e:
            print(f"❌ {service_name}: Not running ({e})")
            all_running = False
    
    return all_running

def main():
    """Main setup function"""
    print("🚀 MetroMind Automation Setup")
    print("=" * 50)
    
    # Check if services are running
    if not check_services():
        print("")
        print("⚠️  Some required services are not running.")
        print("Please start the services first using:")
        print("python start_services.py")
        print("")
        return
    
    print("")
    print("✅ All required services are running!")
    print("")
    
    # Setup email automation
    email_success = setup_email_automation()
    print("")
    
    # Setup WhatsApp automation
    whatsapp_success = setup_whatsapp_automation()
    print("")
    
    # Summary
    print("=== Setup Summary ===")
    if email_success:
        print("✅ Email automation: Configured")
    else:
        print("❌ Email automation: Not configured")
    
    if whatsapp_success:
        print("✅ WhatsApp automation: Configured")
    else:
        print("❌ WhatsApp automation: Not configured")
    
    if email_success or whatsapp_success:
        print("")
        print("🎉 Automation setup completed!")
        print("The system will now automatically:")
        print("- Monitor for new emails/messages")
        print("- Extract and process attachments")
        print("- Analyze documents with AI")
        print("- Assign tasks to appropriate users")
        print("- Send real-time notifications")
        print("")
        print("You can monitor the automation in the dashboard at:")
        print("http://localhost:3000")
    else:
        print("")
        print("ℹ️  No automation was configured.")
        print("You can run this script again after updating the configuration.")

if __name__ == "__main__":
    main()