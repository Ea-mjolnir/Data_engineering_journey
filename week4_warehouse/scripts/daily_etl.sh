#!/bin/bash
# Simple Daily ETL Automation

# Set up paths
PROJECT_ROOT="/home/odinsbeard/Data_engineering_Journey/week4_warehouse"
LOG_DIR="$PROJECT_ROOT/logs/daily"
mkdir -p "$LOG_DIR"

# Get today's date
DATE=$(date +%Y%m%d)
LOG_FILE="$LOG_DIR/etl_$DATE.log"

{
    echo "========================================="
    echo "🚀 ETL AUTO-RUN STARTED AT: $(date)"
    echo "========================================="

    # -----------------------------------------------------------------
    # STEP 1: Find the latest batch
    # -----------------------------------------------------------------
    echo "[1/3] Finding latest batch..."
    
    LATEST_BATCH=$(ls -dt "$PROJECT_ROOT/data"/batch_* 2>/dev/null | head -1)
    
    if [ -z "$LATEST_BATCH" ]; then
        echo "❌ No batch found! Run generator first."
        exit 1
    fi
    
    BATCH_NAME=$(basename "$LATEST_BATCH")
    echo "✅ Using batch: $BATCH_NAME"
    
    # -----------------------------------------------------------------
    # STEP 2: Load to staging (auto-confirm)
    # -----------------------------------------------------------------
    echo "[2/3] Loading to staging..."
    
    # Use 'yes' command to automatically answer 'y' to prompts
    yes | "$PROJECT_ROOT/scripts/load_staging.sh" "$BATCH_NAME"
    
    if [ $? -ne 0 ]; then
        echo "❌ Staging load failed!"
        exit 1
    fi
    echo "✅ Staging load complete"
    
    # -----------------------------------------------------------------
    # STEP 3: Run ETL with quality checks
    # -----------------------------------------------------------------
    echo "[3/3] Running ETL pipeline..."
    
    "$PROJECT_ROOT/scripts/run_warehouse_etl_and_qualitychecks.sh"
    
    if [ $? -ne 0 ]; then
        echo "❌ ETL failed!"
        exit 1
    fi
    echo "✅ ETL complete"
    
    echo "========================================="
    echo "✅ ETL AUTO-RUN COMPLETED AT: $(date)"
    echo "========================================="

} >> "$LOG_FILE" 2>&1

echo "ETL auto-run complete. Check log: $LOG_FILE"
