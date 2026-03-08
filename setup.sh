#!/bin/bash

# Sales Analytics Dashboard - Local Setup Script
# This script sets up and runs the dashboard on port 10000

echo "🚀 Sales Analytics Dashboard - Local Setup"
echo "==========================================="

# Check for MongoDB
echo "📦 Checking MongoDB..."
if command -v mongod &> /dev/null; then
    echo "✅ MongoDB found"
else
    echo "⚠️  MongoDB not found. Please install MongoDB or run via Docker:"
    echo "   docker run -d -p 27017:27017 --name mongodb mongo:latest"
fi

# Setup Backend
echo ""
echo "🔧 Setting up Backend..."
cd backend

# Create .env for local setup
cat > .env << 'EOF'
MONGO_URL="mongodb://localhost:27017"
DB_NAME="sales_dashboard"
CORS_ORIGINS="*"
EOF

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt -q

echo "✅ Backend ready"

# Setup Frontend
echo ""
echo "🔧 Setting up Frontend..."
cd ../frontend

# Create .env for local setup
cat > .env << 'EOF'
REACT_APP_BACKEND_URL=http://localhost:10000
EOF

yarn install --silent

echo "✅ Frontend ready"

# Start services
echo ""
echo "🚀 Starting services..."
echo ""
echo "To start the application, run these commands in separate terminals:"
echo ""
echo "Terminal 1 (Backend on port 10000):"
echo "  cd backend && source venv/bin/activate && uvicorn server:app --host 0.0.0.0 --port 10000 --reload"
echo ""
echo "Terminal 2 (Frontend on port 3000):"
echo "  cd frontend && yarn start"
echo ""
echo "Then open http://localhost:3000 in your browser"
echo "Click 'Refresh Data' to load the sales data"
