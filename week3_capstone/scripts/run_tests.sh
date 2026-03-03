#!/bin/bash

################################################################################
# Test Runner for E-Commerce Pipeline
# Run this script to execute all tests
################################################################################

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Header
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo "${GREEN}🧪 E-COMMERCE PIPELINE TEST SUITE${NC}"
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo "Started: $(date)"
echo "Project: $PROJECT_DIR"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "${YELLOW}📦 Creating virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo "${YELLOW}📦 Activating virtual environment...${NC}"
source venv/bin/activate

# Install test dependencies
echo "${YELLOW}📦 Installing test dependencies...${NC}"
pip install --upgrade pip > /dev/null
pip install pytest pytest-cov pandas > /dev/null 2>&1

# Check if tests directory exists
if [ ! -d "tests" ]; then
    echo "${RED}❌ Tests directory not found!${NC}"
    echo "Creating tests directory..."
    mkdir -p tests
    touch tests/__init__.py
fi

# Run the tests
echo ""
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo "${GREEN}🔬 Running Tests...${NC}"
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Run pytest with coverage
if python -m pytest tests/ -v --cov=src --cov-report=term --cov-report=html --tb=short; then
    echo ""
    echo "${GREEN}✅ ALL TESTS PASSED!${NC}"
    TEST_RESULT=0
else
    echo ""
    echo "${RED}❌ SOME TESTS FAILED!${NC}"
    TEST_RESULT=1
fi

# Show coverage summary
echo ""
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo "${GREEN}📊 COVERAGE SUMMARY${NC}"
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
python -m pytest tests/ --cov=src --cov-report=term --quiet > /dev/null 2>&1 || true

echo ""
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo "📝 To run specific test: pytest tests/test_extractors.py -v"
echo "⏱️  Finished: $(date)"
echo "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

exit $TEST_RESULT
