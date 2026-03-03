#!/bin/bash

################################################################################
# EC2 Pipeline Runner
# Run this script on EC2 to execute your data pipeline
################################################################################

set -e

# Colors for pretty output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
PROJECT_DIR="/home/ubuntu/week3_capstone"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo -e "$1" | tee -a "$LOG_FILE"
}

# Change to project directory
cd "$PROJECT_DIR"

# Start logging
log "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
log "${GREEN}📊 E-COMMERCE DATA PIPELINE${NC}"
log "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
log "Started: $(date)"
log "Project: $PROJECT_DIR"
log "Host: $(hostname)"
log "Log file: $LOG_FILE"
log ""

# Activate virtual environment
log "${YELLOW}📦 Activating virtual environment...${NC}"
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    log "${GREEN}✓ Virtual environment activated${NC}"
else
    log "${RED}✗ Virtual environment not found at $VENV_DIR${NC}"
    exit 1
fi

# Check AWS credentials
log "\n${YELLOW}🔐 Checking AWS credentials...${NC}"
if aws sts get-caller-identity &>/dev/null; then
    ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    log "${GREEN}✓ AWS credentials valid (Account: $ACCOUNT)${NC}"
else
    log "${RED}✗ AWS credentials invalid or not configured${NC}"
    exit 1
fi

# Verify S3 buckets are accessible
log "\n${YELLOW}🪣 Checking S3 buckets...${NC}"
if [ -f ".env" ]; then
    source .env
    log "Bronze bucket: $S3_BRONZE_BUCKET"
    log "Silver bucket: $S3_SILVER_BUCKET"
    log "Gold bucket: $S3_GOLD_BUCKET"
    
    if aws s3 ls "s3://$S3_BRONZE_BUCKET" &>/dev/null; then
        log "${GREEN}✓ Bronze bucket accessible${NC}"
    else
        log "${RED}✗ Cannot access bronze bucket${NC}"
        exit 1
    fi
else
    log "${RED}✗ .env file not found${NC}"
    exit 1
fi

# Run the pipeline
log "\n${YELLOW}⚡ Starting pipeline execution...${NC}"
START_TIME=$(date +%s)

if python3 src/main.py 2>&1 | tee -a "$LOG_FILE"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "\n${GREEN}✅ Pipeline completed successfully!${NC}"
    log "Duration: ${DURATION} seconds"
    EXIT_CODE=0
else
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log "\n${RED}❌ Pipeline failed${NC}"
    log "Duration: ${DURATION} seconds"
    EXIT_CODE=1
fi

# Show summary
log "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
log "${GREEN}📋 EXECUTION SUMMARY${NC}"
log "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
log "Exit code: $EXIT_CODE"
log "Duration: ${DURATION} seconds"
log "Log file: $LOG_FILE"

# Show last few lines of data processed (if successful)
if [ $EXIT_CODE -eq 0 ]; then
    log "\n${YELLOW}📊 Recent data processed:${NC}"
    echo "----------------------------------------" | tee -a "$LOG_FILE"
    aws s3 ls s3://$S3_GOLD_BUCKET/gold/ --recursive | tail -5 | tee -a "$LOG_FILE"
fi

log "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
log "Finished: $(date)"

# Clean up old logs (keep last 30 days)
log "\n${YELLOW}🧹 Cleaning up old logs...${NC}"
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete
find "$LOG_DIR" -name "pipeline_*.log" -mtime +1 -exec gzip {} \; 2>/dev/null || true
log "${GREEN}✓ Log cleanup complete${NC}"

exit $EXIT_CODE
