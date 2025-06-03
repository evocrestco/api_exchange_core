#!/bin/bash
# E2E Testing Setup Script
# 
# This script sets up the complete e2e testing environment

set -e

echo "🚀 Setting up E2E Testing Environment"
echo "======================================"

# Check if we're in the right directory
if [[ ! -f "docker-compose.yml" ]]; then
    echo "❌ Error: Please run this script from the e2e/ directory"
    exit 1
fi

# Step 1: Copy environment file if it doesn't exist
if [[ ! -f ".env" ]]; then
    echo "📋 Copying .env.example to .env..."
    cp .env.example .env
    echo "✅ Created .env file (you can customize it if needed)"
else
    echo "📋 .env file already exists"
fi

# Step 2: Start Docker Compose services
echo "🐳 Starting Docker Compose services..."
docker-compose up -d

# Step 3: Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check if PostgreSQL is ready
echo "🔍 Checking PostgreSQL connection..."
if docker-compose exec -T postgres pg_isready -U test_user -d e2e_test; then
    echo "✅ PostgreSQL is ready"
else
    echo "❌ PostgreSQL is not ready. Check docker-compose logs."
    exit 1
fi

# Step 4: Initialize database
echo "🗃️  Initializing database..."
if python setup_test_db.py; then
    echo "✅ Database initialized successfully"
else
    echo "❌ Database initialization failed"
    exit 1
fi

# Step 5: Check if Azure Functions Core Tools is installed
echo "🔧 Checking Azure Functions Core Tools..."
if command -v func &> /dev/null; then
    echo "✅ Azure Functions Core Tools is installed"
    func --version
else
    echo "⚠️  Azure Functions Core Tools not found"
    echo "   Install with: npm install -g azure-functions-core-tools@4 --unsafe-perm true"
    echo "   Or visit: https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local"
fi

# Step 6: Install Python dependencies if requirements.txt exists
if [[ -f "requirements.txt" ]]; then
    echo "📦 Installing Python dependencies..."
    pip install -r requirements.txt
    echo "✅ Python dependencies installed"
fi

echo ""
echo "🎉 E2E Testing Environment Setup Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Start Azure Functions: func start"
echo "2. Run tests: python run_e2e_tests.py --scenario all --count 2"
echo ""
echo "Services running:"
echo "- PostgreSQL: localhost:5432"
echo "- Azurite: localhost:10000-10002"
echo ""
echo "To stop services: docker-compose down"