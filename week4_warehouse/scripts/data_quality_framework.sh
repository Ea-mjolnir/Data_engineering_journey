#!/bin/bash
# scripts/data_quality_framework.sh

################################################################################
# Data Quality Framework
# Tests all 6 data quality pillars for the e-commerce warehouse
# SINGLE CONNECTION - Enter password ONCE!
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}📊 DATA QUALITY FRAMEWORK${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Load environment variables
source ../.env 2>/dev/null || echo -e "${YELLOW}⚠️  No .env file found${NC}"

# Create report directory
REPORT_DIR="../reports/data_quality"
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/quality_report_$(date +%Y%m%d_%H%M%S).html"
LOG_FILE="$REPORT_DIR/quality_log_$(date +%Y%m%d_%H%M%S).log"

# Initialize counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Create a temporary SQL file with ALL tests
TMP_SQL=$(mktemp)

cat > "$TMP_SQL" << 'EOF'
-- ============================================================================
-- DATA QUALITY TESTS - SINGLE CONNECTION
-- ============================================================================

SET search_path TO warehouse;

-- Create temp table to store results
DROP TABLE IF EXISTS quality_results;
CREATE TEMP TABLE quality_results (
    pillar text,
    test_name text,
    status text,
    violations bigint,
    expected bigint
);

-- ============================================================================
-- PILLAR 1: COMPLETENESS
-- ============================================================================
INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'NULL customer keys',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE customer_key IS NULL;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'NULL product keys',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE product_key IS NULL;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'NULL date keys',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE date_key IS NULL;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'Customers missing email',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM dim_customer WHERE email IS NULL AND is_current = true;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'Orders without items',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM staging.staging_orders o
LEFT JOIN staging.staging_order_items i ON o.order_id = i.order_id
WHERE i.order_id IS NULL;

-- ============================================================================
-- PILLAR 2: ACCURACY
-- ============================================================================
INSERT INTO quality_results 
SELECT 'ACCURACY', 'Negative quantities',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE quantity < 0;

INSERT INTO quality_results 
SELECT 'ACCURACY', 'Negative profit margins',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE profit_margin < 0;

INSERT INTO quality_results 
SELECT 'ACCURACY', 'Cost exceeds price',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE unit_cost > unit_price;

INSERT INTO quality_results 
SELECT 'ACCURACY', 'Gross revenue mismatch',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE ABS(gross_revenue - (quantity * unit_price)) > 0.01;

-- ============================================================================
-- PILLAR 3: CONSISTENCY
-- ============================================================================
INSERT INTO quality_results 
SELECT 'CONSISTENCY', 'Orphan customers',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

INSERT INTO quality_results 
SELECT 'CONSISTENCY', 'Orphan products',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f LEFT JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

INSERT INTO quality_results 
SELECT 'CONSISTENCY', 'Orphan stores',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f LEFT JOIN dim_store s ON f.store_key = s.store_key
WHERE f.store_key IS NOT NULL AND s.store_key IS NULL;

-- ============================================================================
-- PILLAR 4: VALIDITY
-- ============================================================================
INSERT INTO quality_results 
SELECT 'VALIDITY', 'Invalid email format',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM dim_customer WHERE email NOT LIKE '%@%.%' AND email IS NOT NULL;

INSERT INTO quality_results 
SELECT 'VALIDITY', 'Future dates',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_id
WHERE d.date > CURRENT_DATE;

INSERT INTO quality_results 
SELECT 'VALIDITY', 'Profit margin out of range',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE profit_margin > 100 OR profit_margin < -100;

-- ============================================================================
-- PILLAR 5: UNIQUENESS
-- ============================================================================
INSERT INTO quality_results 
SELECT 'UNIQUENESS', 'Duplicate order lines',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM (
    SELECT order_id, order_line_number, COUNT(*)
    FROM fact_sales
    GROUP BY order_id, order_line_number
    HAVING COUNT(*) > 1
) dupes;

INSERT INTO quality_results 
SELECT 'UNIQUENESS', 'Duplicate current customers',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM (
    SELECT customer_id, COUNT(*)
    FROM dim_customer
    WHERE is_current = true
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) dupes;

-- ============================================================================
-- PILLAR 6: TIMELINESS
-- ============================================================================
INSERT INTO quality_results 
SELECT 'TIMELINESS', 'Stale data (>24 hours)',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
       COUNT(*), 0
FROM fact_table_metadata
WHERE last_refresh < NOW() - INTERVAL '24 hours'
AND table_name IN ('fact_sales', 'fact_daily_sales_summary');

INSERT INTO quality_results 
SELECT 'TIMELINESS', 'Unprocessed staging data',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
       COUNT(*), 0
FROM staging.staging_orders
WHERE load_status != 'WAREHOUSE_LOADED'
AND loaded_at < NOW() - INTERVAL '24 hours';

-- ============================================================================
-- RESULTS SUMMARY
-- ============================================================================
SELECT '📊 TEST RESULTS BY PILLAR' as section;
SELECT pillar, 
       COUNT(*) as tests,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
       SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) as warnings
FROM quality_results
GROUP BY pillar
ORDER BY pillar;

SELECT '📋 DETAILED RESULTS' as section;
SELECT pillar, test_name, status, violations, expected
FROM quality_results
ORDER BY pillar, 
         CASE status 
            WHEN 'FAIL' THEN 1 
            WHEN 'WARN' THEN 2 
            ELSE 3 
         END;

SELECT '📈 SUMMARY' as section;
SELECT 
    COUNT(*) as total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) as warnings,
    ROUND(100.0 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM quality_results;
EOF

# Run ALL tests with ONE connection
echo -e "\n${YELLOW}🔍 Running all data quality tests (single connection)...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

export PGPASSWORD="$DB_PASSWORD"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -f "$TMP_SQL" | tee "$LOG_FILE"
PSQL_EXIT=$?
unset PGPASSWORD

# Clean up
rm -f "$TMP_SQL"

if [ $PSQL_EXIT -eq 0 ]; then
    echo -e "\n${GREEN}✅ Tests completed successfully${NC}"
    
    # Parse results for summary
    PASSED=$(grep -c "PASS" "$LOG_FILE" || true)
    FAILED=$(grep -c "FAIL" "$LOG_FILE" || true)
    WARNINGS=$(grep -c "WARN" "$LOG_FILE" || true)
    TOTAL=$((PASSED + FAILED + WARNINGS))
    
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}📊 FINAL SUMMARY${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "Total Tests: $TOTAL"
    echo -e "${GREEN}✅ Passed: $PASSED${NC}"
    echo -e "${RED}❌ Failed: $FAILED${NC}"
    echo -e "${YELLOW}⚠️  Warnings: $WARNINGS${NC}"
    
    PASS_RATE=$((PASSED * 100 / TOTAL))
    echo -e "Pass Rate: ${PASS_RATE}%"
    
    if [ $FAILED -eq 0 ]; then
        echo -e "\n${GREEN}✅ ALL CRITICAL TESTS PASSED!${NC}"
    else
        echo -e "\n${RED}❌ $FAILED TESTS FAILED - Check $LOG_FILE${NC}"
    fi
else
    echo -e "\n${RED}❌ Tests failed to run${NC}"
    exit 1
fi

echo -e "\n${CYAN}📝 Log file: $LOG_FILE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
