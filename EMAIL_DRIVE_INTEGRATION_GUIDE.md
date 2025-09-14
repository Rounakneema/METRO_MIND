# Email and Google Drive Integration Guide

## Overview

The MetroMind system now supports comprehensive email and Google Drive integration for automated document processing and task management. This guide walks you through setting up both integrations.

## ✉️ Email Integration

### Features
- **Automatic Email Processing**: Monitors IMAP inbox for new emails
- **Attachment Handling**: Extracts and processes PDF, Word, Excel, and image attachments
- **AI-Powered Analysis**: Uses ML models to categorize and extract insights from documents
- **Task Creation**: Automatically creates tasks based on email content and priority
- **Real-time Processing**: Background service continuously monitors for new emails

### Setup Process

1. **Environment Configuration**
   ```bash
   # Set these environment variables or add to .env file
   EMAIL_IMAP_SERVER=imap.gmail.com
   EMAIL_IMAP_PORT=993
   EMAIL_USERNAME=your-email@gmail.com
   EMAIL_PASSWORD=your-app-password  # Use app-specific password for Gmail
   
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USERNAME=your-email@gmail.com
   SMTP_PASSWORD=your-app-password
   ```

2. **Integration Registration**
   - Navigate to the Integrations page in the web dashboard
   - Click "Setup Email" on the Email Integration card
   - The system will automatically detect your configuration and enable email processing

3. **Verification**
   - Send a test email with an attachment to your configured email address
   - Check the Dashboard for new tasks and documents created automatically
   - Monitor the Integration Service logs for processing status

### Gmail Setup Notes
- Enable 2-factor authentication on your Gmail account
- Generate an App Password: Google Account → Security → App passwords
- Use the App Password (not your regular password) in the configuration

## 📁 Google Drive Integration

### Features
- **OAuth2 Authentication**: Secure authorization flow
- **Folder Monitoring**: Monitor specific folders for new documents
- **File Synchronization**: Download and process new files automatically
- **Document Analysis**: AI processing of uploaded documents
- **Metadata Extraction**: Automatic extraction of file properties and content insights

### Setup Process

1. **Google API Setup**
   - Visit the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing one
   - Enable the Google Drive API
   - Create OAuth2 credentials (Desktop Application type)
   - Download the credentials JSON file

2. **System Configuration**
   ```bash
   # Set environment variables
   GOOGLE_DRIVE_CREDENTIALS_FILE=./data/google_drive_credentials.json
   GOOGLE_DRIVE_TOKEN_FILE=./data/google_drive_token.json
   ```

3. **Credentials Setup**
   - Place the downloaded credentials JSON file at the specified path
   - Ensure the data directory exists: `mkdir -p ./data`

4. **Authorization Flow**
   - Navigate to the Integrations page
   - Click "Setup Drive" on the Google Drive card
   - Click "Authorize Google Drive Access" when the dialog opens
   - Complete the OAuth flow in your browser
   - Copy the authorization code back to the application
   - Click "Complete Setup"

5. **Integration Configuration**
   - After authorization, create a Google Drive integration
   - Specify folder IDs to monitor (optional - defaults to root)
   - Configure file type filters (PDF, DOCX, XLSX, etc.)
   - Set sync interval (default: 15 minutes)

## 🔧 Technical Architecture

### Email Processing Pipeline
```
Email Received → IMAP Fetch → Attachment Extraction → Document Creation → AI Analysis → Task Assignment
```

### Google Drive Processing Pipeline
```
File Added to Drive → Webhook/Polling → File Download → Document Creation → AI Analysis → Task Assignment
```

### Database Integration
Both integrations create:
- **Document records** with metadata and file paths
- **Task entries** with AI-generated summaries and priority levels
- **Integration logs** for monitoring and debugging

## 🚀 Getting Started

### Quick Setup Commands
```bash
# 1. Configure email environment variables
cp .env.example .env
# Edit .env with your email credentials

# 2. Start the services
python start_services.py

# 3. Access the web dashboard
# Navigate to http://localhost:3001
# Go to Integrations page
# Follow the setup wizards for Email and Google Drive
```

### Monitoring and Management
- **Dashboard**: Real-time view of processed documents and tasks
- **Integration Status**: Monitor sync status and error logs
- **Task Management**: Review and manage auto-generated tasks
- **Analytics**: Track processing volumes and success rates

## 🛠️ Advanced Configuration

### Email Filters
Configure rules to process only specific emails:
```python
# In integration configuration
{
    "subject_filters": ["urgent", "priority"],
    "sender_whitelist": ["@company.com"],
    "attachment_types": [".pdf", ".docx", ".xlsx"]
}
```

### Google Drive Folder Monitoring
```python
# Monitor specific folders
{
    "folder_ids": ["1ABC123DEF456", "1GHI789JKL012"],
    "file_types": ["application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"],
    "recursive": true
}
```

### AI Processing Customization
- **Document Classification**: Automatic categorization based on content
- **Priority Detection**: ML-based urgency assessment
- **Summary Generation**: Extract key insights and create summaries
- **Entity Recognition**: Identify important names, dates, and locations

## 🔐 Security Considerations

### Email Security
- Use app-specific passwords instead of main account passwords
- Enable 2FA on email accounts
- Regularly rotate access credentials
- Monitor for unauthorized access

### Google Drive Security
- OAuth2 tokens are stored securely and encrypted
- Minimal required permissions (read-only access to Drive)
- Token refresh handling for long-term access
- Audit logs for all file access

## 📊 Monitoring and Troubleshooting

### Common Issues

**Email Not Processing**
```bash
# Check integration service logs
tail -f data/logs/integration_service.log

# Verify email configuration
curl http://localhost:8017/integrations
```

**Google Drive Authorization Fails**
```bash
# Check credentials file exists
ls -la ./data/google_drive_credentials.json

# Verify API is enabled in Google Cloud Console
# Check OAuth redirect URIs match application settings
```

### Performance Optimization
- **Batch Processing**: Configure batch sizes for large volumes
- **Sync Intervals**: Adjust based on expected file frequency
- **Resource Allocation**: Monitor CPU and memory usage during processing
- **Database Optimization**: Regular cleanup of processed items

## 🎯 Next Steps

After setting up both integrations:

1. **Test the Workflow**: Send test emails and upload test files
2. **Configure Rules**: Set up filters and processing rules
3. **Monitor Performance**: Check dashboard analytics
4. **Scale Deployment**: Configure for production workloads
5. **User Training**: Onboard team members on the automated workflow

## 📞 Support

For additional support:
- Check the logs in `data/logs/` directory
- Review the API documentation at `http://localhost:8017/docs`
- Monitor service health at `http://localhost:8010/health`

---

This integration enables a fully automated document processing pipeline that transforms your email and Google Drive into an intelligent task management system.