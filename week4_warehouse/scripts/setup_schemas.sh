#!/bin/bash

################################################################################
# Schema Setup Script for Data Warehouse
# Creates staging and warehouse schemas in PostgreSQL
################################################################################

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🏗️  Setting up Data Warehouse Schemas${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Load environment variables
if [ -f .env ]; then
    echo -e "${YELLOW}📦 Loading configuration from .env${NC}"
    source .env
else
    echo -e "${RED}❌ .env file not found!${NC}"
    exit 1
fi

# Create the SQL file
echo -e "\n${YELLOW}📝 Creating schema SQL file...${NC}"

cat > sql/schema/01_create_schemas.sql << 'EOF'
-- ============================================================================
-- Data Warehouse Schema Setup
-- Creates staging and warehouse schemas
-- ============================================================================

-- Drop schemas if they exist (for development)
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS warehouse CASCADE;

-- Create schemas
CREATE SCHEMA staging;
CREATE SCHEMA warehouse;

-- Grant permissions
GRANT ALL ON SCHEMA staging TO data_engineer;
GRANT ALL ON SCHEMA warehouse TO data_engineer;

-- Set search path (so we don't have to prefix table names)
ALTER DATABASE ecommerce_warehouse SET search_path TO warehouse, staging, public;

-- Verify
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('staging', 'warehouse');
EOF

echo -e "${GREEN}✅ SQL file created at: sql/schema/01_create_schemas.sql${NC}"

# Run the SQL file
echo -e "\n${YELLOW}⚙️  Executing schema setup...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

# Set password for psql
export PGPASSWORD=$DB_PASSWORD

# Execute the SQL file
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -f sql/schema/01_create_schemas.sql

# Clear password from environment
unset PGPASSWORD

echo -e "${BLUE}--------------------------------------------------${NC}"
echo -e "${GREEN}✅ Schema setup completed!${NC}"

# Verify
echo -e "\n${YELLOW}🔍 Current schemas in database:${NC}"
export PGPASSWORD=$DB_PASSWORD
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -c "\dn"
unset PGPASSWORD

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}📊 Schemas ready for dimension and fact tables!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
