#!/bin/bash
#
# Coffee Pipeline E2E Test Runner
#
# This script handles the complete test setup and execution:
# 1. Starts docker-compose services
# 2. Sets up database schema
# 3. Prompts user to start Azure Functions
# 4. Runs the E2E test
# 5. Cleans up
#
# Usage:
#     ./run_e2e_test.sh
#
# Requirements:
#     - Docker and docker-compose installed
#     - Python 3.8+ with requests and psycopg2 packages
#     - Azure Functions Core Tools (func)
#

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}☕ Coffee Pipeline E2E Test Runner${NC}"
echo "=========================================="

# Check prerequisites
echo -e "${YELLOW}🔍 Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not found. Please install Docker.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ docker-compose not found. Please install docker-compose.${NC}"
    exit 1
fi

if ! command -v func &> /dev/null; then
    echo -e "${RED}❌ Azure Functions Core Tools not found.${NC}"
    echo -e "${YELLOW}💡 Install with: npm install -g azure-functions-core-tools@4${NC}"
    exit 1
fi

if ! python3 -c "import requests, psycopg2" &> /dev/null; then
    echo -e "${RED}❌ Missing Python dependencies.${NC}"
    echo -e "${YELLOW}💡 Install with: pip install requests psycopg2-binary${NC}"
    exit 1
fi

echo -e "${GREEN}✅ All prerequisites found${NC}"

# Start services
echo -e "${YELLOW}🐳 Starting Docker services...${NC}"
docker-compose up -d

# Wait for services to be healthy
echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
sleep 10

# Check service health
if ! docker-compose ps | grep -q "healthy"; then
    echo -e "${RED}❌ Services not healthy. Checking status:${NC}"
    docker-compose ps
    echo -e "${YELLOW}💡 Try waiting a bit longer and check docker-compose logs${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker services are running${NC}"

# Setup database
echo -e "${YELLOW}🗄️  Setting up database schema...${NC}"
cd database
python3 setup.py
cd ..

echo -e "${GREEN}✅ Database schema created${NC}"

# Prompt user to start Functions
echo ""
echo -e "${YELLOW}🚀 IMPORTANT: Start Azure Functions in another terminal${NC}"
echo -e "${BLUE}Run this command in a separate terminal:${NC}"
echo -e "${GREEN}    cd $(pwd) && func start${NC}"
echo ""
echo -e "${YELLOW}Wait for Functions to start (you'll see endpoints listed), then press Enter to continue...${NC}"
read -p ""

# Run the test
echo -e "${YELLOW}🧪 Running E2E test...${NC}"
python3 test_e2e_pipeline.py

# Store test result
test_result=$?

# Cleanup prompt
echo ""
echo -e "${YELLOW}🧹 Test completed. Clean up services?${NC}"
echo -e "${BLUE}This will stop docker-compose services.${NC}"
read -p "Stop services? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}🛑 Stopping Docker services...${NC}"
    docker-compose down
    echo -e "${GREEN}✅ Services stopped${NC}"
else
    echo -e "${BLUE}💡 Services left running. Stop with: docker-compose down${NC}"
fi

# Final result
echo ""
if [ $test_result -eq 0 ]; then
    echo -e "${GREEN}🎉 E2E TEST PASSED! Coffee pipeline is working! ☕✨${NC}"
else
    echo -e "${RED}❌ E2E TEST FAILED. Check the output above for details.${NC}"
fi

exit $test_result