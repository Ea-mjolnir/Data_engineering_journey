#!/bin/bash

################################################################################
# Manual Pipeline Monitoring Dashboard
# Run this anytime to check pipeline status
################################################################################

# Get the script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"

# Clear screen for clean display
clear

# Header
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           E-COMMERCE PIPELINE MONITORING DASHBOARD            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "🕐 Last Updated: $(date)"
echo "📁 Project: $PROJECT_DIR"
echo ""

# 1. SYSTEM RESOURCES
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💻 SYSTEM RESOURCES"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# CPU Usage
if command -v mpstat &> /dev/null; then
    CPU_IDLE=$(mpstat 1 1 | tail -1 | awk '{print $12}')
    CPU_USED=$(echo "100 - $CPU_IDLE" | bc)
    echo "CPU Usage:    ${CPU_USED}%"
else
    CPU_USED=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
    echo "CPU Usage:    ${CPU_USED}%"
fi

# Memory Usage
MEM_TOTAL=$(free -h | awk 'NR==2{print $2}')
MEM_USED=$(free -h | awk 'NR==2{print $3}')
MEM_PERCENT=$(free | awk 'NR==2{printf "%.1f", $3*100/$2}')
echo "Memory Usage: $MEM_USED / $MEM_TOTAL (${MEM_PERCENT}%)"

# Disk Usage
DISK_INFO=$(df -h "$PROJECT_DIR" | awk 'NR==2{printf "%s / %s (%s)", $3, $2, $5}')
echo "Disk Usage:   $DISK_INFO"

# 2. RECENT PIPELINE RUNS
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 RECENT PIPELINE RUNS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -d "$LOG_DIR" ] && [ "$(ls -A $LOG_DIR 2>/dev/null)" ]; then
    echo "Last 5 pipeline executions:"
    echo ""
    printf "%-20s %-10s %-15s %s\n" "DATE" "STATUS" "DURATION" "FILE"
    echo "───────────────────────────────────────────────────────────────"
    
    ls -t "$LOG_DIR"/pipeline_*.log 2>/dev/null | head -5 | while read logfile; do
        if [ -f "$logfile" ]; then
            # Extract timestamp from filename
            filename=$(basename "$logfile")
            timestamp=$(echo "$filename" | sed 's/pipeline_//' | sed 's/.log//' | cut -c1-15)
            
           
            # Check if successful (multiple possible success messages)
            if grep -q "Status: SUCCESS" "$logfile" 2>/dev/null || \
               grep -q "Pipeline completed successfully" "$logfile" 2>/dev/null; then
                status="✅ SUCCESS"
            else
                status="❌ FAILED"
            fi
           

            # Get duration
            duration=$(grep "Duration:" "$logfile" 2>/dev/null | tail -1 | awk '{print $2}' | sed 's/seconds/s/')
            if [ -z "$duration" ]; then
                duration="N/A"
            fi
            
            printf "%-20s %-10s %-15s %s\n" "$timestamp" "$status" "$duration" ""
        fi
    done
else
    echo "No pipeline logs found yet"
fi

# 3. S3 DATA LAKE STATUS
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "☁️  S3 DATA LAKE STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Load bucket names from .env
if [ -f "$PROJECT_DIR/.env" ]; then
    source "$PROJECT_DIR/.env"
    
    # Show bucket info
    echo "Bronze Bucket: $S3_BRONZE_BUCKET"
    echo "Silver Bucket: $S3_SILVER_BUCKET"
    echo "Gold Bucket:   $S3_GOLD_BUCKET"
    echo ""
    
    # Count files in each layer
    echo "File counts by layer:"
    echo "───────────────────────────────────────────────────────────────"
    
    # Bronze
    BRONZE_COUNT=$(aws s3 ls "s3://$S3_BRONZE_BUCKET/bronze/" --recursive 2>/dev/null | wc -l)
    if [ "$BRONZE_COUNT" -gt 0 ]; then
        echo "🥉 Bronze:  $BRONZE_COUNT files"
        
        # Show latest file in bronze
        LATEST_BRONZE=$(aws s3 ls "s3://$S3_BRONZE_BUCKET/bronze/api/$(date +%Y)/$(date +%m)/$(date +%d)/" --recursive 2>/dev/null | tail -1 | awk '{print $4}')
        if [ -n "$LATEST_BRONZE" ]; then
            echo "           Latest: $LATEST_BRONZE"
        fi
    else
        echo "🥉 Bronze:  No files yet"
    fi
    
    # Silver
    SILVER_COUNT=$(aws s3 ls "s3://$S3_SILVER_BUCKET/silver/" --recursive 2>/dev/null | wc -l)
    if [ "$SILVER_COUNT" -gt 0 ]; then
        echo "🥈 Silver:  $SILVER_COUNT files"
    else
        echo "🥈 Silver:  No files yet"
    fi
    
    # Gold
    GOLD_COUNT=$(aws s3 ls "s3://$S3_GOLD_BUCKET/gold/" --recursive 2>/dev/null | wc -l)
    if [ "$GOLD_COUNT" -gt 0 ]; then
        echo "🥇 Gold:    $GOLD_COUNT files"
        
        # Show latest gold files
        echo ""
        echo "Latest Gold files:"
        aws s3 ls "s3://$S3_GOLD_BUCKET/gold/" --recursive --human-readable 2>/dev/null | tail -3 | sed 's/^/  /'
    else
        echo "🥇 Gold:    No files yet"
    fi
else
    echo "⚠️  .env file not found - cannot show S3 status"
fi

# 4. CRON SCHEDULE
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "⏰ SCHEDULED RUNS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if crontab -l 2>/dev/null | grep -q "run_pipeline.sh"; then
    echo "✅ Pipeline is scheduled to run automatically:"
    crontab -l | grep "run_pipeline.sh" | sed 's/^/   /'
    
    # Show next run time (simplified)
    if crontab -l | grep -q "0 2"; then
        echo "   Next run: Tonight at 2:00 AM"
    fi
else
    echo "ℹ️  No automatic schedule found (manual runs only)"
fi

# 5. LAST 24 HOURS ACTIVITY
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 LAST 24 HOURS ACTIVITY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check today's data in bronze
TODAY=$(date +%Y/%m/%d)
BRONZE_TODAY=$(aws s3 ls "s3://$S3_BRONZE_BUCKET/bronze/api/$TODAY/" --recursive 2>/dev/null | wc -l)

if [ "$BRONZE_TODAY" -gt 0 ]; then
    echo "✅ New data arrived today: $BRONZE_TODAY files in bronze/api/$TODAY/"
    
    # Show sample of today's files
    echo ""
    echo "Today's files:"
    aws s3 ls "s3://$S3_BRONZE_BUCKET/bronze/api/$TODAY/" --human-readable 2>/dev/null | tail -3 | sed 's/^/  /'
else
    echo "⏳ No new data yet today"
fi

# Footer
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Run './scripts/run_pipeline.sh' to execute pipeline manually  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
