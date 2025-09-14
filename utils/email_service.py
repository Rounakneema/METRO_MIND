"""
MetroMind Email Service
Email notifications and communications
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional, Dict, Any
import asyncio
import jinja2
from pathlib import Path
import logging

from config import integration_config, app_config
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

class EmailService:
    """Email service for sending notifications and communications"""
    
    def __init__(self):
        self.smtp_server = integration_config.smtp_server
        self.smtp_port = integration_config.smtp_port
        self.username = integration_config.smtp_username
        self.password = integration_config.smtp_password
        self.enabled = bool(self.smtp_server and self.username and self.password)
        
        # Template environment
        template_dir = Path(app_config.data_directory) / "templates" / "emails"
        template_dir.mkdir(parents=True, exist_ok=True)
        
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(template_dir)),
            autoescape=jinja2.select_autoescape(['html', 'xml'])
        )
        
        # Create default templates if they don't exist
        self._create_default_templates()
        
        logger.info(f"Email service initialized - Enabled: {self.enabled}")
    
    def _create_default_templates(self):
        """Create default email templates"""
        template_dir = Path(app_config.data_directory) / "templates" / "emails"
        
        # Base template
        base_template = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #2c5aa0; color: white; padding: 20px; text-align: center; }
        .content { padding: 20px; background: #f9f9f9; }
        .footer { padding: 20px; text-align: center; color: #666; font-size: 0.9em; }
        .btn { display: inline-block; padding: 10px 20px; background: #2c5aa0; color: white; text-decoration: none; border-radius: 5px; }
        .alert { padding: 15px; margin: 10px 0; border-radius: 5px; }
        .alert-info { background: #d1ecf1; border-left: 4px solid #0c5460; }
        .alert-success { background: #d4edda; border-left: 4px solid #155724; }
        .alert-warning { background: #fff3cd; border-left: 4px solid #856404; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚇 MetroMind</h1>
            <p>Kochi Metro Rail Limited - Document Management System</p>
        </div>
        <div class="content">
            {% block content %}{% endblock %}
        </div>
        <div class="footer">
            <p>© 2024 KMRL - MetroMind System</p>
            <p>This is an automated message. Please do not reply to this email.</p>
        </div>
    </div>
</body>
</html>"""
        
        # User approval template
        approval_template = """{% extends "base.html" %}
{% block content %}
<h2>Account Status Update</h2>

{% if approved %}
    <div class="alert alert-success">
        <h3>🎉 Account Approved!</h3>
        <p>Dear {{ first_name }},</p>
        <p>Your MetroMind account has been approved! You can now access the system with full privileges.</p>
        
        <h4>Next Steps:</h4>
        <ul>
            <li>Log in to the system using your username and password</li>
            <li>Complete your profile information</li>
            <li>Start managing your documents efficiently</li>
        </ul>
        
        <p style="text-align: center; margin: 20px 0;">
            <a href="{{ system_url }}" class="btn">Access MetroMind</a>
        </p>
    </div>
{% else %}
    <div class="alert alert-warning">
        <h3>Account Registration Update</h3>
        <p>Dear {{ first_name }},</p>
        <p>We regret to inform you that your MetroMind account registration could not be approved at this time.</p>
        
        {% if notes %}
        <h4>Additional Information:</h4>
        <p><em>{{ notes }}</em></p>
        {% endif %}
        
        <p>If you have any questions or would like to reapply, please contact your system administrator.</p>
    </div>
{% endif %}

<p>If you have any questions, please contact the MetroMind support team.</p>
{% endblock %}"""
        
        # Admin notification template
        admin_notification_template = """{% extends "base.html" %}
{% block content %}
<h2>{{ subject }}</h2>

<div class="alert alert-info">
    <p>{{ message }}</p>
</div>

{% if action_required %}
<h4>Action Required:</h4>
<ul>
    {% for action in actions %}
    <li>{{ action }}</li>
    {% endfor %}
</ul>

<p style="text-align: center; margin: 20px 0;">
    <a href="{{ admin_url }}" class="btn">Go to Admin Panel</a>
</p>
{% endif %}
{% endblock %}"""
        
        # Document alert template
        document_alert_template = """{% extends "base.html" %}
{% block content %}
<h2>📄 Document Alert</h2>

<div class="alert alert-{{ alert_type }}">
    <h3>{{ document_title }}</h3>
    <p><strong>Category:</strong> {{ category }}</p>
    <p><strong>Priority:</strong> {{ priority }}</p>
    <p><strong>Uploaded by:</strong> {{ uploaded_by }}</p>
    <p><strong>Date:</strong> {{ upload_date }}</p>
</div>

<h4>Document Summary:</h4>
<p>{{ summary }}</p>

{% if action_required %}
<div class="alert alert-warning">
    <h4>⚠️ Action Required</h4>
    <p>{{ action_message }}</p>
</div>
{% endif %}

<p style="text-align: center; margin: 20px 0;">
    <a href="{{ document_url }}" class="btn">View Document</a>
</p>
{% endblock %}"""
        
        # Write templates
        templates = {
            "base.html": base_template,
            "approval.html": approval_template,
            "admin_notification.html": admin_notification_template,
            "document_alert.html": document_alert_template
        }
        
        for filename, content in templates.items():
            template_file = template_dir / filename
            if not template_file.exists():
                template_file.write_text(content, encoding='utf-8')
                logger.info(f"Created email template: {filename}")
    
    async def send_email(self, to_email: str, subject: str, html_body: str, 
                        text_body: Optional[str] = None, 
                        attachments: Optional[List[Dict]] = None) -> bool:
        """
        Send email asynchronously
        
        Args:
            to_email: Recipient email address
            subject: Email subject
            html_body: HTML email body
            text_body: Plain text body (optional)
            attachments: List of attachment dicts with 'filename' and 'content'
            
        Returns:
            True if sent successfully, False otherwise
        """
        
        if not self.enabled:
            logger.warning("Email service is disabled - email not sent")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.username
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Add text part
            if text_body:
                text_part = MIMEText(text_body, 'plain')
                msg.attach(text_part)
            
            # Add HTML part
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            
            # Add attachments
            if attachments:
                for attachment in attachments:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment['content'])
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {attachment["filename"]}'
                    )
                    msg.attach(part)
            
            # Send email
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}")
            return False
    
    async def send_approval_email(self, to_email: str, first_name: str, 
                                approved: bool, notes: Optional[str] = None) -> bool:
        """Send user approval/rejection email"""
        
        try:
            template = self.jinja_env.get_template('approval.html')
            
            html_body = template.render(
                title="Account Status Update",
                first_name=first_name,
                approved=approved,
                notes=notes,
                system_url="http://localhost:3001"  # Update with actual URL
            )
            
            subject = "MetroMind Account Approved" if approved else "MetroMind Account Registration Update"
            
            return await self.send_email(to_email, subject, html_body)
            
        except Exception as e:
            logger.error(f"Failed to send approval email: {e}")
            return False
    
    async def send_admin_notification(self, to_email: str, subject: str, 
                                    message: str, action_required: bool = True,
                                    actions: Optional[List[str]] = None) -> bool:
        """Send notification to admin"""
        
        try:
            template = self.jinja_env.get_template('admin_notification.html')
            
            html_body = template.render(
                title="Admin Notification",
                subject=subject,
                message=message,
                action_required=action_required,
                actions=actions or [],
                admin_url="http://localhost:3001/admin"  # Update with actual URL
            )
            
            return await self.send_email(to_email, f"MetroMind Admin: {subject}", html_body)
            
        except Exception as e:
            logger.error(f"Failed to send admin notification: {e}")
            return False
    
    async def send_document_alert(self, to_email: str, document_data: Dict[str, Any]) -> bool:
        """Send document processing alert"""
        
        try:
            template = self.jinja_env.get_template('document_alert.html')
            
            alert_type = "warning" if document_data.get('priority') in ['HIGH', 'CRITICAL'] else "info"
            
            html_body = template.render(
                title="Document Alert",
                document_title=document_data.get('title', 'New Document'),
                category=document_data.get('category', 'Unknown'),
                priority=document_data.get('priority', 'Medium'),
                uploaded_by=document_data.get('uploaded_by', 'System'),
                upload_date=document_data.get('upload_date', 'Now'),
                summary=document_data.get('summary', 'Document processed successfully'),
                alert_type=alert_type,
                action_required=document_data.get('action_required', False),
                action_message=document_data.get('action_message', ''),
                document_url=document_data.get('url', '#')
            )
            
            subject_prefix = "🚨 URGENT" if document_data.get('priority') == 'CRITICAL' else "📄"
            subject = f"{subject_prefix} Document Alert: {document_data.get('title', 'New Document')}"
            
            return await self.send_email(to_email, subject, html_body)
            
        except Exception as e:
            logger.error(f"Failed to send document alert: {e}")
            return False
    
    async def send_bulk_notification(self, recipients: List[str], subject: str, 
                                   html_body: str, text_body: Optional[str] = None) -> Dict[str, bool]:
        """Send bulk email notifications"""
        
        results = {}
        
        # Send emails concurrently
        tasks = []
        for recipient in recipients:
            task = asyncio.create_task(
                self.send_email(recipient, subject, html_body, text_body)
            )
            tasks.append((recipient, task))
        
        # Wait for all tasks to complete
        for recipient, task in tasks:
            try:
                result = await task
                results[recipient] = result
            except Exception as e:
                logger.error(f"Bulk email failed for {recipient}: {e}")
                results[recipient] = False
        
        success_count = sum(1 for r in results.values() if r)
        logger.info(f"Bulk email completed: {success_count}/{len(recipients)} successful")
        
        return results
    
    def test_connection(self) -> bool:
        """Test SMTP connection"""
        
        if not self.enabled:
            return False
        
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.username, self.password)
            logger.info("SMTP connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}")
            return False

# Global email service instance
email_service = EmailService()

# Convenience functions
async def send_welcome_email(email: str, name: str):
    """Send welcome email to new user"""
    html_body = f"""
    <h2>Welcome to MetroMind, {name}!</h2>
    <p>Your account has been created and is pending approval.</p>
    <p>You will receive another email once your account is approved.</p>
    """
    return await email_service.send_email(email, "Welcome to MetroMind", html_body)

async def send_system_alert(admin_emails: List[str], alert_message: str):
    """Send system alert to administrators"""
    html_body = f"""
    <h2>🚨 System Alert</h2>
    <div style="background: #fff3cd; padding: 15px; border-left: 4px solid #856404;">
        <p><strong>Alert:</strong> {alert_message}</p>
        <p><strong>Time:</strong> {asyncio.get_event_loop().time()}</p>
    </div>
    """
    return await email_service.send_bulk_notification(
        admin_emails, "MetroMind System Alert", html_body
    )

if __name__ == "__main__":
    # Test email service
    async def test():
        service = EmailService()
        print(f"Email service enabled: {service.enabled}")
        
        if service.enabled:
            connection_ok = service.test_connection()
            print(f"SMTP connection: {'OK' if connection_ok else 'Failed'}")
    
    asyncio.run(test())
