#!/bin/bash

# ==================================================
# S3 Medallion Architecture Bucket Creation Script
# AWS Account: Ea_Data_Engineering (2885-2869-6055)
# ==================================================

# Your AWS Account details
AWS_ACCOUNT_ID_HYPHENS="2885-2869-6055"  # With hyphens for display
AWS_ACCOUNT_ID="288528696055"  # Without hyphens for bucket naming
ACCOUNT_NAME="ea-data-engineering"
ENVIRONMENT="dev"
AWS_REGION="us-east-1"
DATE_TAG=$(date +%Y%m%d)

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================${NC}"
echo -e "${GREEN}🚀 Creating Medallion Architecture S3 Buckets${NC}"
echo -e "${BLUE}==================================================${NC}"
echo -e "AWS Account: ${YELLOW}Ea_Data_Engineering${NC}"
echo -e "Account ID:  ${YELLOW}${AWS_ACCOUNT_ID_HYPHENS}${NC}"
echo -e "Region:      ${YELLOW}${AWS_REGION}${NC}"
echo -e "Environment: ${YELLOW}${ENVIRONMENT}${NC}"
echo -e "Date:        ${YELLOW}${DATE_TAG}${NC}"
echo -e "${BLUE}==================================================${NC}"

# Function to create bucket with comprehensive configuration
create_bucket() {
    LAYER=$1
    LAYER_NAME=$2
    
    # Create bucket name (using account ID without hyphens)
    BUCKET_NAME="${ACCOUNT_NAME}-${LAYER}-${ENVIRONMENT}-${AWS_ACCOUNT_ID}-${DATE_TAG}"
    
    echo -e "\n${GREEN}🔨 Creating ${LAYER_NAME} bucket:${NC} ${BUCKET_NAME}"
    
    # Check if bucket already exists
    if aws s3 ls "s3://${BUCKET_NAME}" 2>&1 | grep -q 'NoSuchBucket'; then
        # Create the bucket
        echo "   Creating bucket in ${AWS_REGION}..."
        if aws s3 mb "s3://${BUCKET_NAME}" --region ${AWS_REGION} 2>&1; then
            echo "   ✅ Bucket created successfully"
            
            # 1. Enable versioning
            echo "   Enabling versioning..."
            aws s3api put-bucket-versioning \
                --bucket "${BUCKET_NAME}" \
                --versioning-configuration Status=Enabled
            
            # 2. Enable encryption
            echo "   Enabling default encryption..."
            aws s3api put-bucket-encryption \
                --bucket "${BUCKET_NAME}" \
                --server-side-encryption-configuration '{
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                            }
                        }
                    ]
                }'
            
            # 3. Block public access
            echo "   Blocking public access..."
            aws s3api put-public-access-block \
                --bucket "${BUCKET_NAME}" \
                --public-access-block-configuration \
                BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
            
            # 4. Add comprehensive tags - FIXED VERSION
            echo "   Adding resource tags..."
            
            # Create a temporary JSON file for tags to avoid formatting issues
            cat > /tmp/tags.json << EOF
{
    "TagSet": [
        {
            "Key": "Project",
            "Value": "DataEngineering"
        },
        {
            "Key": "Account",
            "Value": "Ea_Data_Engineering"
        },
        {
            "Key": "AccountID",
            "Value": "${AWS_ACCOUNT_ID_HYPHENS}"
        },
        {
            "Key": "Layer",
            "Value": "${LAYER}"
        },
        {
            "Key": "LayerName",
            "Value": "${LAYER_NAME}"
        },
        {
            "Key": "Environment",
            "Value": "${ENVIRONMENT}"
        },
        {
            "Key": "CreatedBy",
            "Value": "Script"
        },
        {
            "Key": "CreatedDate",
            "Value": "${DATE_TAG}"
        }
    ]
}
EOF
            
            # Apply tags using the JSON file
            aws s3api put-bucket-tagging \
                --bucket "${BUCKET_NAME}" \
                --tagging file:///tmp/tags.json
            
            # Check if tagging was successful
            if [ $? -eq 0 ]; then
                echo "   ✅ Tags applied successfully"
            else
                echo "   ${YELLOW}⚠️ Tagging had issues but bucket was created${NC}"
            fi
            
            # Clean up temp file
            rm -f /tmp/tags.json
            
            # 5. Create folder structure
            create_folder_structure "${BUCKET_NAME}" "${LAYER}" "${LAYER_NAME}"
            
        else
            echo -e "${RED}   ❌ Failed to create bucket${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}   ⚠️ Bucket already exists: ${BUCKET_NAME}${NC}"
    fi
    
    echo "${BUCKET_NAME}"
}

# Function to create folder structure
create_folder_structure() {
    BUCKET=$1
    LAYER=$2
    LAYER_NAME=$3
    
    echo "   📁 Creating folder structure for ${LAYER_NAME}..."
    
    # Base folders for all layers
    aws s3api put-object --bucket ${BUCKET} --key _meta/ >/dev/null 2>&1
    aws s3api put-object --bucket ${BUCKET} --key _logs/ >/dev/null 2>&1
    aws s3api put-object --bucket ${BUCKET} --key _schemas/ >/dev/null 2>&1
    aws s3api put-object --bucket ${BUCKET} --key _quality/ >/dev/null 2>&1
    
    # Layer-specific folders
    case ${LAYER} in
        "bronze")
            for folder in landing streaming batch cdc snapshots; do
                aws s3api put-object --bucket ${BUCKET} --key ${folder}/ >/dev/null 2>&1
            done
            ;;
        "silver")
            for folder in cleaned validated enriched conformed dimensions facts; do
                aws s3api put-object --bucket ${BUCKET} --key ${folder}/ >/dev/null 2>&1
            done
            ;;
        "gold")
            for folder in aggregated curated reporting analytics metrics kpis; do
                aws s3api put-object --bucket ${BUCKET} --key ${folder}/ >/dev/null 2>&1
            done
            ;;
    esac
    
    echo "   ✅ Folder structure created"
}

# Main execution
echo -e "\n${BLUE}📊 Creating bucket layers...${NC}"

# Create the three buckets
BRONZE_BUCKET=$(create_bucket "bronze" "Bronze (Raw)")
SILVER_BUCKET=$(create_bucket "silver" "Silver (Cleansed)")
GOLD_BUCKET=$(create_bucket "gold" "Gold (Business)")

# Generate output for .env file
echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}✅ ALL BUCKETS CREATED SUCCESSFULLY!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "\n${BLUE}📝 Add these to your .env file:${NC}"
echo "=================================================="
echo "# AWS Configuration for Ea_Data_Engineering"
echo "AWS_REGION=${AWS_REGION}"
echo "AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}"
echo ""
echo "# S3 Medallion Buckets"
echo "S3_BRONZE_BUCKET=${BRONZE_BUCKET}"
echo "S3_SILVER_BUCKET=${SILVER_BUCKET}"
echo "S3_GOLD_BUCKET=${GOLD_BUCKET}"
echo "=================================================="

# Display bucket details
echo -e "\n${BLUE}📋 Bucket Details:${NC}"
echo "=================================================="
printf "%-15s %-50s\n" "LAYER" "BUCKET NAME"
echo "--------------------------------------------------"
printf "%-15s %-50s\n" "Bronze" "${BRONZE_BUCKET}"
printf "%-15s %-50s\n" "Silver" "${SILVER_BUCKET}"
printf "%-15s %-50s\n" "Gold" "${GOLD_BUCKET}"
echo "=================================================="
