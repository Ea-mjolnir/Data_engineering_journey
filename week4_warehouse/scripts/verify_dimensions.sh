#!/bin/bash

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🔍 Verifying Dimension Tables${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Load environment
if [ -f ../.env ]; then
    source ../.env
else
    DB_USER="data_engineer"
    DB_NAME="ecommerce_warehouse"
    DB_HOST="localhost"
    echo -e "${YELLOW}⚠️  Using default values${NC}"
fi

export PGPASSWORD=$DB_PASSWORD

# Check tables
echo -e "\n${YELLOW}📋 Tables in warehouse schema:${NC}"
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -c "\dt warehouse.*"

# Check row counts
echo -e "\n${YELLOW}📊 Row counts:${NC}"
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -c "
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM warehouse.dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM warehouse.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM warehouse.dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM warehouse.dim_store
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM warehouse.dim_payment;"

unset PGPASSWORD

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Verification complete!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
