#!/bin/bash
# Start both backend and frontend services

# For PowerShell (Windows), use semicolon to run multiple commands:
# cd "D:\E244\sih_mvp"; python start_services.py

# For Command Prompt (Windows), use & to run multiple commands:
# cd "D:\E244\sih_mvp" & python start_services.py

# For running both backend and frontend:
# cd "D:\E244\sih_mvp"; python start_services.py; cd frontend; npm start

echo "Starting MetroMind Backend and Frontend Services..."
echo ""
echo "To start both services in PowerShell, run:"
echo 'cd "D:\E244\sih_mvp"; python start_services.py'
echo ""
echo "In a separate terminal, run:"
echo 'cd "D:\E244\sih_mvp\frontend"; npm start'
echo ""
echo "Or to run both in sequence (backend first, then frontend):"
echo 'cd "D:\E244\sih_mvp"; python start_services.py; cd frontend; npm start'