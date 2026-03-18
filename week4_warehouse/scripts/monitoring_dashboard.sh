#!/bin/bash

################################################################################
# TERMINAL DASHBOARD - Data Pipeline Monitoring
# Shows real-time status of ETL, Warehouse, Quality Checks, and Batches
# CORRECTED: Uses .env for database credentials (no password prompts)
################################################################################

# Colors for beautiful output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[1;37m'
NC='\033[0m'
BOLD='\033[1m'

# Clear screen and hide cursor
clear
tput civis

# Function to cleanup on exit
cleanup() {
    tput cnorm  # Show cursor again
    echo -e "\n${GREEN}👋 Dashboard closed. Goodbye!${NC}"
    exit 0
}

# Trap Ctrl+C to cleanup
trap cleanup INT

# Load database credentials from .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/.env"
else
    echo -e "${RED}❌ .env file not found!${NC}"
    echo -e "${YELLOW}Please create .env file with:${NC}"
    echo "DB_HOST=localhost"
    echo "DB_USER=data_engineer"
    echo "DB_PASSWORD=your_password"
    echo "DB_NAME=ecommerce_warehouse"
    exit 1
fi

# Verify database credentials are set
if [ -z "$DB_PASSWORD" ]; then
    echo -e "${RED}❌ DB_PASSWORD not set in .env file!${NC}"
    exit 1
fi

# Function to draw a separator line
draw_line() {
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
}

# Function to run SQL query and get single value
get_db_count() {
    local query="$1"
    PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -t -c "$query" 2>/dev/null | xargs
}

# Function to run SQL query and get formatted output
run_db_query() {
    local query="$1"
    PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -t -c "$query" 2>/dev/null
}

# Test database connection
TEST_CONN=$(PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -c "SELECT 1;" 2>&1)
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Cannot connect to database!${NC}"
    echo -e "${YELLOW}Error: $TEST_CONN${NC}"
    exit 1
fi

# Main dashboard loop
while true; do
    # Clear screen for fresh update
    clear
    
    # Header
    echo -e "${BOLD}${MAGENTA}┌─────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${BOLD}${MAGENTA}│            DATA PIPELINE MONITORING DASHBOARD                 │${NC}"
    echo -e "${BOLD}${MAGENTA}└─────────────────────────────────────────────────────────────┘${NC}"
    echo -e "${CYAN}  Last Updated: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    draw_line
    echo ""
    
    # ------------------------------------------------------------------------
    # SECTION 1: ETL STATUS
    # ------------------------------------------------------------------------
    echo -e "${BOLD}${WHITE}📊 ETL STATUS${NC}"
    echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
    
    # Check last ETL run
    LOG_DIR="$PROJECT_ROOT/logs/daily"
    LAST_ETL_LOG=$(ls -t "$LOG_DIR"/etl_*.log 2>/dev/null | head -1)
    
    if [ -f "$LAST_ETL_LOG" ]; then
        LAST_RUN=$(basename "$LAST_ETL_LOG" | sed 's/etl_\(.*\)\.log/\1/')
        LAST_RUN_TIME=$(stat -c %y "$LAST_ETL_LOG" | cut -d'.' -f1)
        
        # Check if last run was successful
        if grep -q "COMPLETED\|SUCCESS" "$LAST_ETL_LOG" 2>/dev/null; then
            echo -e "  ${BOLD}Last Run:${NC}     ${GREEN}✅ SUCCESS${NC}"
        else
            echo -e "  ${BOLD}Last Run:${NC}     ${RED}❌ FAILED${NC}"
        fi
        echo -e "  ${BOLD}Date:${NC}         $LAST_RUN"
        echo -e "  ${BOLD}Time:${NC}         $LAST_RUN_TIME"
        echo -e "  ${BOLD}Log:${NC}          $(basename "$LAST_ETL_LOG")"
    else
        echo -e "  ${BOLD}Last Run:${NC}     ${YELLOW}⚠️  No runs yet${NC}"
    fi
    
    # Total ETL runs
    TOTAL_RUNS=$(ls "$LOG_DIR"/etl_*.log 2>/dev/null | wc -l)
    echo -e "  ${BOLD}Total Runs:${NC}    $TOTAL_RUNS"
    
    # Check cron schedule
    if crontab -l 2>/dev/null | grep -q "daily_etl"; then
        CRON_SCHEDULE=$(crontab -l | grep "daily_etl" | head -1 | awk '{print $1" "$2" "$3" "$4" "$5}')
        echo -e "  ${BOLD}Cron:${NC}         ${GREEN}✅ Active${NC} ($CRON_SCHEDULE)"
    else
        echo -e "  ${BOLD}Cron:${NC}         ${RED}❌ Not scheduled${NC}"
    fi
    echo ""
    
    # ------------------------------------------------------------------------
    # SECTION 2: WAREHOUSE STATISTICS
    # ------------------------------------------------------------------------
    echo -e "${BOLD}${WHITE}📦 WAREHOUSE STATISTICS${NC}"
    echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
    
    # Get table counts
    CUSTOMER_COUNT=$(get_db_count "SELECT COUNT(*) FROM warehouse.dim_customer;")
    PRODUCT_COUNT=$(get_db_count "SELECT COUNT(*) FROM warehouse.dim_product;")
    STORE_COUNT=$(get_db_count "SELECT COUNT(*) FROM warehouse.dim_store;")
    FACT_SALES_COUNT=$(get_db_count "SELECT COUNT(*) FROM warehouse.fact_sales;")
    FACT_DAILY_COUNT=$(get_db_count "SELECT COUNT(*) FROM warehouse.fact_daily_sales_summary;")
    
    # Calculate total rows
    TOTAL_ROWS=$((CUSTOMER_COUNT + PRODUCT_COUNT + STORE_COUNT + FACT_SALES_COUNT + FACT_DAILY_COUNT))
    
    # Get revenue
    TOTAL_REVENUE=$(get_db_count "SELECT COALESCE(SUM(net_revenue), 0)::numeric(12,2) FROM warehouse.fact_sales;")
    
    # Display in columns
    printf "  ${BOLD}%-20s${NC} %10s\n" "Table" "Rows"
    echo -e "  ${CYAN}────────────────────────────────${NC}"
    printf "  %-20s ${GREEN}%'10d${NC}\n" "dim_customer:" "${CUSTOMER_COUNT:-0}"
    printf "  %-20s ${GREEN}%'10d${NC}\n" "dim_product:" "${PRODUCT_COUNT:-0}"
    printf "  %-20s ${GREEN}%'10d${NC}\n" "dim_store:" "${STORE_COUNT:-0}"
    printf "  %-20s ${GREEN}%'10d${NC}\n" "fact_sales:" "${FACT_SALES_COUNT:-0}"
    printf "  %-20s ${GREEN}%'10d${NC}\n" "daily_summary:" "${FACT_DAILY_COUNT:-0}"
    echo -e "  ${CYAN}────────────────────────────────${NC}"
    printf "  ${BOLD}%-20s${NC} ${YELLOW}%'10d${NC}\n" "TOTAL ROWS:" "${TOTAL_ROWS:-0}"
    printf "  ${BOLD}%-20s${NC} ${GREEN}%'15s${NC}\n" "TOTAL REVENUE:" "\$${TOTAL_REVENUE:-0}"
    echo ""
    
    # ------------------------------------------------------------------------
    # SECTION 3: DATA QUALITY CHECKS
    # ------------------------------------------------------------------------
    echo -e "${BOLD}${WHITE}✅ DATA QUALITY CHECKS${NC}"
    echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
    
    # Check for NULL foreign keys
    NULL_KEYS=$(get_db_count "
        SELECT COUNT(*) FROM warehouse.fact_sales 
        WHERE customer_key IS NULL OR product_key IS NULL OR date_key IS NULL;
    ")
    
    # Check for duplicates
    DUPLICATES=$(get_db_count "
        SELECT COUNT(*) FROM (
            SELECT order_id, order_line_number 
            FROM warehouse.fact_sales 
            GROUP BY order_id, order_line_number 
            HAVING COUNT(*) > 1
        ) dupes;
    ")
    
    # Check for negative quantities
    NEGATIVE_QTY=$(get_db_count "
        SELECT COUNT(*) FROM warehouse.fact_sales WHERE quantity < 0;
    ")
    
    # Check for cost > price
    COST_EXCEEDS=$(get_db_count "
        SELECT COUNT(*) FROM warehouse.fact_sales WHERE unit_cost > unit_price;
    ")
    
    # Check for invalid profit margins
    INVALID_MARGIN=$(get_db_count "
        SELECT COUNT(*) FROM warehouse.fact_sales 
        WHERE profit_margin > 100 OR profit_margin < -100;
    ")
    
    # Display quality checks with appropriate colors
    display_check() {
        local name="$1"
        local value="$2"
        local threshold="${3:-0}"
        
        printf "  %-25s" "$name:"
        if [ "$value" -eq "$threshold" ]; then
            echo -e " ${GREEN}✅ PASS${NC} (0)"
        else
            echo -e " ${RED}❌ FAIL${NC} ($value)"
        fi
    }
    
    display_check "NULL Foreign Keys" "$NULL_KEYS" 0
    display_check "Duplicate Orders" "$DUPLICATES" 0
    display_check "Negative Quantities" "$NEGATIVE_QTY" 0
    display_check "Cost > Price" "$COST_EXCEEDS" 0
    display_check "Invalid Margins" "$INVALID_MARGIN" 0
    echo ""
    
    # ------------------------------------------------------------------------
    # SECTION 4: BATCH PROCESSING STATUS
    # ------------------------------------------------------------------------
    echo -e "${BOLD}${WHITE}📁 BATCH PROCESSING STATUS${NC}"
    echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
    
    DATA_DIR="$PROJECT_ROOT/data"
    TRACKING_FILE="$PROJECT_ROOT/logs/batch_tracking/processed_batches.log"
    
    # Count total batches
    TOTAL_BATCHES=$(ls -d "$DATA_DIR"/batch_* 2>/dev/null | wc -l)
    
    # Count processed batches
    PROCESSED_BATCHES=0
    if [ -f "$TRACKING_FILE" ]; then
        PROCESSED_BATCHES=$(grep -c "SUCCESS" "$TRACKING_FILE" 2>/dev/null || echo 0)
    fi
    PENDING_BATCHES=$((TOTAL_BATCHES - PROCESSED_BATCHES))
    
    # Show batch stats
    echo -e "  ${BOLD}Total Batches:${NC}   $TOTAL_BATCHES"
    echo -e "  ${BOLD}Processed:${NC}       ${GREEN}$PROCESSED_BATCHES${NC}"
    echo -e "  ${BOLD}Pending:${NC}         ${YELLOW}$PENDING_BATCHES${NC}"
    
    # Progress bar
    if [ $TOTAL_BATCHES -gt 0 ]; then
        PROGRESS=$((PROCESSED_BATCHES * 100 / TOTAL_BATCHES))
        BAR_WIDTH=40
        FILLED=$((PROGRESS * BAR_WIDTH / 100))
        EMPTY=$((BAR_WIDTH - FILLED))
        
        echo -n "  ${BOLD}Progress:${NC}     ["
        for ((i=0; i<FILLED; i++)); do echo -n "${GREEN}█${NC}"; done
        for ((i=0; i<EMPTY; i++)); do echo -n "░"; done
        echo -e "] ${PROGRESS}%"
    fi
    
    # Show latest batch
    LATEST_BATCH=$(ls -dt "$DATA_DIR"/batch_* 2>/dev/null | head -1 | xargs basename)
    if [ ! -z "$LATEST_BATCH" ]; then
        echo -e "  ${BOLD}Latest Batch:${NC}   $LATEST_BATCH"
        
        # Check if latest batch is processed
        if [ -f "$TRACKING_FILE" ] && grep -q "$LATEST_BATCH" "$TRACKING_FILE" 2>/dev/null; then
            echo -e "  ${BOLD}Status:${NC}         ${GREEN}✅ Processed${NC}"
        else
            echo -e "  ${BOLD}Status:${NC}         ${YELLOW}⏳ Pending${NC}"
        fi
    fi
    echo ""
    
    # ------------------------------------------------------------------------
    # SECTION 5: SYSTEM HEALTH
    # ------------------------------------------------------------------------
    echo -e "${BOLD}${WHITE}💻 SYSTEM HEALTH${NC}"
    echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
    
    # Disk usage
    DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}')
    DISK_PERCENT=$(echo $DISK_USAGE | sed 's/%//')
    echo -n "  ${BOLD}Disk Usage:${NC}     "
    if [ "$DISK_PERCENT" -gt 85 ]; then
        echo -e "${RED}$DISK_USAGE${NC} (⚠️  Critical)"
    elif [ "$DISK_PERCENT" -gt 70 ]; then
        echo -e "${YELLOW}$DISK_USAGE${NC} (⚠️  Warning)"
    else
        echo -e "${GREEN}$DISK_USAGE${NC} (✓ Good)"
    fi
    
    # Memory usage
    MEM_TOTAL=$(free -h | awk 'NR==2 {print $2}')
    MEM_USED=$(free -h | awk 'NR==2 {print $3}')
    MEM_PERCENT=$(free | awk 'NR==2 {printf "%.0f", $3/$2 * 100}')
    echo -n "  ${BOLD}Memory Usage:${NC}   "
    if [ "$MEM_PERCENT" -gt 85 ]; then
        echo -e "${RED}${MEM_USED}/${MEM_TOTAL} (${MEM_PERCENT}%)${NC} (⚠️  Critical)"
    elif [ "$MEM_PERCENT" -gt 70 ]; then
        echo -e "${YELLOW}${MEM_USED}/${MEM_TOTAL} (${MEM_PERCENT}%)${NC} (⚠️  Warning)"
    else
        echo -e "${GREEN}${MEM_USED}/${MEM_TOTAL} (${MEM_PERCENT}%)${NC} (✓ Good)"
    fi
    
    # PostgreSQL status
    PG_TEST=$(PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -c "SELECT 1;" 2>&1)
    if [ $? -eq 0 ]; then
        echo -e "  ${BOLD}PostgreSQL:${NC}     ${GREEN}✅ Running${NC}"
        
        # Get database size
        DB_SIZE=$(PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -t -c "
            SELECT pg_size_pretty(pg_database_size('$DB_NAME'));
        " 2>/dev/null | xargs)
        echo -e "  ${BOLD}DB Size:${NC}        $DB_SIZE"
        
        # Get active connections
        ACTIVE_CONN=$(PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -t -c "
            SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';
        " 2>/dev/null | xargs)
        echo -e "  ${BOLD}Connections:${NC}    $ACTIVE_CONN"
    else
        echo -e "  ${BOLD}PostgreSQL:${NC}     ${RED}❌ Down${NC}"
        echo -e "  ${BOLD}Error:${NC}          $PG_TEST"
    fi
    
    # ------------------------------------------------------------------------
    # FOOTER
    # ------------------------------------------------------------------------
    draw_line
    echo -e "${CYAN}  Press ${WHITE}Ctrl+C${CYAN} to exit | Auto-refreshes every 10 seconds${NC}"
    echo -e "${CYAN}  Database: ${WHITE}$DB_USER@$DB_HOST/$DB_NAME${NC}"
    draw_line
    
    # Wait before refreshing
    sleep 10
done
