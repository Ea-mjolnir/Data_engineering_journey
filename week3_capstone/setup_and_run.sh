#!/bin/bash

# setup_and_run.sh - Complete setup and run script for the E-Commerce Pipeline
# Run this from INSIDE your week3_capstone folder

set -e  # Stop script if any command fails
set -u  # Stop script if undefined variable is used

# Colors for pretty output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   E-COMMERCE ANALYTICS PIPELINE - SETUP & RUN         ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if we're in the right directory
if [ ! -d "src" ] || [ ! -f "requirements.txt" ]; then
    echo -e "${RED}❌ ERROR: Must run from week3_capstone folder with src/ and requirements.txt${NC}"
    echo -e "${YELLOW}   Current directory: $(pwd)${NC}"
    echo -e "${YELLOW}   Make sure you're in: ~/Data_engineering_Journey/week3_capstone${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Found correct directory: $(pwd)${NC}"
echo ""

# Step 1: Check Python version
echo -e "${BLUE}📌 STEP 1: Checking Python installation...${NC}"
if command -v python3 &>/dev/null; then
    python_version=$(python3 --version)
    echo -e "${GREEN}   ✅ $python_version found${NC}"
else
    echo -e "${RED}   ❌ Python3 not found! Please install Python 3.8+${NC}"
    exit 1
fi
echo ""

# Step 2: Create virtual environment
echo -e "${BLUE}📌 STEP 2: Creating virtual environment...${NC}"
if [ -d "venv" ]; then
    echo -e "${YELLOW}   ⚠️  Virtual environment already exists${NC}"
    read -p "   Do you want to recreate it? (y/n): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}   Removing old venv...${NC}"
        rm -rf venv
        python3 -m venv venv
        echo -e "${GREEN}   ✅ New virtual environment created${NC}"
    else
        echo -e "${GREEN}   ✅ Using existing virtual environment${NC}"
    fi
else
    python3 -m venv venv
    echo -e "${GREEN}   ✅ Virtual environment created${NC}"
fi
echo ""

# Step 3: Activate virtual environment
echo -e "${BLUE}📌 STEP 3: Activating virtual environment...${NC}"
source venv/bin/activate
echo -e "${GREEN}   ✅ Activated: $(which python)${NC}"
echo ""

# Step 4: Upgrade pip
echo -e "${BLUE}📌 STEP 4: Upgrading pip...${NC}"
pip install --upgrade pip
echo -e "${GREEN}   ✅ Pip upgraded to $(pip --version | cut -d' ' -f2)${NC}"
echo ""

# Step 5: Install dependencies
echo -e "${BLUE}📌 STEP 5: Installing dependencies from requirements.txt...${NC}"
echo -e "${YELLOW}   This may take a few minutes...${NC}"
pip install -r requirements.txt
echo -e "${GREEN}   ✅ All dependencies installed successfully${NC}"
echo ""

# Step 6: Check if .env exists
echo -e "${BLUE}📌 STEP 6: Checking environment configuration...${NC}"
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo -e "${YELLOW}   ⚠️  .env file not found. Creating from .env.example...${NC}"
        cp .env.example .env
        echo -e "${YELLOW}   ⚠️  Please edit .env with your actual credentials:${NC}"
        echo -e "${YELLOW}      nano .env${NC}"
        read -p "   Press Enter to continue after editing (or Ctrl+C to cancel)..."
    else
        echo -e "${RED}   ❌ No .env or .env.example found!${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}   ✅ .env file exists${NC}"
fi
echo ""

# Step 7: Show installed packages
echo -e "${BLUE}📌 STEP 7: Installed packages summary...${NC}"
echo -e "${GREEN}   Key packages installed:${NC}"
pip list | grep -E "boto3|pandas|requests|pytest|pyarrow" | sed 's/^/      /'
echo ""

# Step 8: Run the pipeline
echo -e "${BLUE}📌 STEP 8: Running the pipeline...${NC}"
echo -e "${YELLOW}   ========================================${NC}"
echo -e "${YELLOW}   Starting pipeline execution...${NC}"
echo -e "${YELLOW}   ========================================${NC}"
echo ""

# Ask if they want to run
read -p "   Ready to run the pipeline? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    python3 src/main.py
    EXIT_CODE=$?
    
    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✅ Pipeline completed successfully!${NC}"
    else
        echo -e "${RED}❌ Pipeline failed with exit code $EXIT_CODE${NC}"
    fi
else
    echo -e "${YELLOW}   Skipping pipeline execution${NC}"
fi
echo ""

# Step 9: Show logs location
echo -e "${BLUE}📌 Pipeline logs:${NC}"
latest_log=$(ls -t logs/ 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
    echo -e "${GREEN}   Latest log: logs/$latest_log${NC}"
    echo -e "${YELLOW}   View with: cat logs/$latest_log${NC}"
else
    echo -e "${YELLOW}   No logs found yet${NC}"
fi
echo ""

# Step 10: Reminders
echo -e "${BLUE}📌 Important Reminders:${NC}"
echo -e "${GREEN}   ✅ Virtual environment is ACTIVE: $(which python)${NC}"
echo -e "${YELLOW}   ⚠️  When done, deactivate with: deactivate${NC}"
echo -e "${YELLOW}   ⚠️  Your .env file contains secrets - NEVER commit it!${NC}"
echo ""

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   SETUP COMPLETE!                                       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
