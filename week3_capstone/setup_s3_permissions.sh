#!/bin/bash

# ==================================================
# Setup S3 Permissions for Data_engineering_Dev User
# ==================================================

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================${NC}"
echo -e "${GREEN}🔐 Setting up S3 permissions for Data_engineering_Dev${NC}"
echo -e "${BLUE}==================================================${NC}"

# Get AWS Account ID
echo -e "\n${YELLOW}📋 Getting AWS Account ID...${NC}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to get AWS Account ID. Make sure AWS CLI is configured.${NC}"
    exit 1
fi

echo -e "   Account ID: ${GREEN}${AWS_ACCOUNT_ID}${NC}"

# Check if user exists
echo -e "\n${YELLOW}👤 Checking if user Data_engineering_Dev exists...${NC}"
if aws iam get-user --user-name Data_engineering_Dev &>/dev/null; then
    echo -e "   ${GREEN}✅ User exists${NC}"
else
    echo -e "   ${RED}❌ User Data_engineering_Dev does not exist!${NC}"
    echo -e "   Please create the user first or check the username."
    exit 1
fi

# Create policy file
echo -e "\n${YELLOW}📄 Creating S3 policy document...${NC}"

cat > /tmp/s3-medallion-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketVersioning",
                "s3:PutBucketEncryption",
                "s3:PutBucketPublicAccessBlock",
                "s3:PutBucketTagging",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::ea-data-engineering-*",
                "arn:aws:s3:::ea-data-engineering-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListAllMyBuckets"
            ],
            "Resource": "*"
        }
    ]
}
EOF

echo -e "   ${GREEN}✅ Policy document created${NC}"

# Check if policy already exists
POLICY_NAME="S3MedallionAccess-DataEngineering"
echo -e "\n${YELLOW}🔍 Checking if policy ${POLICY_NAME} already exists...${NC}"

if aws iam get-policy --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME} &>/dev/null; then
    echo -e "   ${YELLOW}⚠️ Policy already exists${NC}"
    
    # Ask user what to do
    read -p "   Do you want to delete and recreate it? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Get all versions of the policy
        VERSIONS=$(aws iam list-policy-versions \
            --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME} \
            --query 'Versions[?IsDefaultVersion==`false`].[VersionId]' \
            --output text)
        
        # Delete non-default versions
        for VERSION in $VERSIONS; do
            aws iam delete-policy-version \
                --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME} \
                --version-id $VERSION
        done
        
        # Delete the policy
        aws iam delete-policy \
            --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}
        
        echo -e "   ${GREEN}✅ Old policy deleted${NC}"
    else
        echo -e "   ${YELLOW}⚠️ Keeping existing policy${NC}"
    fi
fi

# Create new policy (if it doesn't exist or was deleted)
if ! aws iam get-policy --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME} &>/dev/null; then
    echo -e "\n${YELLOW}➕ Creating new policy: ${POLICY_NAME}...${NC}"
    
    POLICY_ARN=$(aws iam create-policy \
        --policy-name ${POLICY_NAME} \
        --policy-document file:///tmp/s3-medallion-policy.json \
        --query 'Policy.Arn' \
        --output text)
    
    if [ $? -eq 0 ]; then
        echo -e "   ${GREEN}✅ Policy created successfully${NC}"
        echo -e "   Policy ARN: ${BLUE}${POLICY_ARN}${NC}"
    else
        echo -e "   ${RED}❌ Failed to create policy${NC}"
        rm -f /tmp/s3-medallion-policy.json
        exit 1
    fi
else
    POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"
    echo -e "   ${GREEN}✅ Using existing policy: ${POLICY_ARN}${NC}"
fi

# Check if policy is already attached to user
echo -e "\n${YELLOW}🔍 Checking if policy is attached to user...${NC}"
ATTACHED_POLICIES=$(aws iam list-attached-user-policies \
    --user-name Data_engineering_Dev \
    --query 'AttachedPolicies[?PolicyName==`'${POLICY_NAME}'`].PolicyName' \
    --output text)

if [ -z "$ATTACHED_POLICIES" ]; then
    # Attach policy to user
    echo -e "${YELLOW}➕ Attaching policy to user Data_engineering_Dev...${NC}"
    
    aws iam attach-user-policy \
        --user-name Data_engineering_Dev \
        --policy-arn ${POLICY_ARN}
    
    if [ $? -eq 0 ]; then
        echo -e "   ${GREEN}✅ Policy attached successfully${NC}"
    else
        echo -e "   ${RED}❌ Failed to attach policy${NC}"
        rm -f /tmp/s3-medallion-policy.json
        exit 1
    fi
else
    echo -e "   ${GREEN}✅ Policy already attached to user${NC}"
fi

# List current access keys
echo -e "\n${YELLOW}🔑 Current access keys for Data_engineering_Dev:${NC}"
aws iam list-access-keys --user-name Data_engineering_Dev --output table

# Optional: Create new access keys
echo -e "\n${YELLOW}⚠️  Do you want to create new access keys?${NC}"
echo "   Only create new keys if:"
echo "   - You don't have existing keys"
echo "   - You want to rotate keys for security"
echo "   - You've lost your existing secret key"
echo
read -p "   Create new access keys? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "\n${YELLOW}➕ Creating new access keys...${NC}"
    
    # Create new access keys
    KEYS=$(aws iam create-access-key --user-name Data_engineering_Dev)
    
    # Extract keys
    ACCESS_KEY=$(echo $KEYS | jq -r '.AccessKey.AccessKeyId')
    SECRET_KEY=$(echo $KEYS | jq -r '.AccessKey.SecretAccessKey')
    
    echo -e "\n${GREEN}✅ New access keys created!${NC}"
    echo -e "${RED}⚠️  IMPORTANT: Save these immediately - you won't see them again!${NC}"
    echo -e "${BLUE}==================================================${NC}"
    echo -e "Access Key ID:     ${GREEN}${ACCESS_KEY}${NC}"
    echo -e "Secret Access Key: ${GREEN}${SECRET_KEY}${NC}"
    echo -e "${BLUE}==================================================${NC}"
    
    # Save to a file (optional)
    read -p "   Save keys to a file? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        KEY_FILE="access_keys_$(date +%Y%m%d_%H%M%S).txt"
        cat > ${KEY_FILE} << EOF
AWS Access Keys for Data_engineering_Dev
Created: $(date)
========================================
Access Key ID: ${ACCESS_KEY}
Secret Access Key: ${SECRET_KEY}
========================================
EOF
        echo -e "   ${GREEN}✅ Keys saved to: ${KEY_FILE}${NC}"
        echo -e "   ${RED}⚠️  Keep this file secure and delete after use!${NC}"
    fi
fi

# Clean up
rm -f /tmp/s3-medallion-policy.json

echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}✅ Permission setup complete!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "\n${YELLOW}Next steps:${NC}"
echo "1. Update your .env file with the access keys"
echo "2. Test the permissions with:"
echo "   aws s3 ls  # Should work"
echo "   aws ec2 describe-instances  # Should fail with AccessDenied"
echo "3. Run your bucket creation script"
echo -e "${BLUE}==================================================${NC}"
