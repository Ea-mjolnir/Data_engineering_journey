#!/bin/bash

################################################################################
# Deployment Script for EC2 - Works with VPN/IP Changes
# Uses your existing connection logic to handle dynamic IPs
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Configuration (matching your connect-ec2.sh)
REGION="us-east-1"
KEY_PATH="$HOME/.ssh/aws-ec2-key"
SG_NAME="data-engineering-sg"
TAG_NAME="data-pipeline-server"
EC2_USER="ubuntu"

log_step "🚀 Starting deployment to EC2..."

# Get instance ID from tag
log_info "🔍 Finding EC2 instance with tag: $TAG_NAME"
INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$TAG_NAME" "Name=instance-state-name,Values=running,stopped" \
    --query 'Reservations[0].Instances[0].InstanceId' \
    --output text --region $REGION)

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
    log_error "Could not find instance with tag: $TAG_NAME"
    exit 1
fi
log_info "✅ Found instance: $INSTANCE_ID"

# Check and start instance if needed
STATE=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text --region $REGION)

log_info "📊 Instance state: $STATE"

if [ "$STATE" = "stopped" ]; then
    log_warning "Instance is stopped. Starting it now..."
    aws ec2 start-instances --instance-ids $INSTANCE_ID --region $REGION > /dev/null
    log_info "⏳ Waiting for instance to start..."
    aws ec2 wait instance-running --instance-ids $INSTANCE_ID --region $REGION
    log_info "✅ Instance started!"
    sleep 10  # Give it a moment for services to initialize
elif [ "$STATE" = "running" ]; then
    log_info "✅ Instance is already running"
else
    log_error "Unexpected instance state: $STATE"
    exit 1
fi

# Get public IP
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text --region $REGION)

log_info "📡 Public IP: $PUBLIC_IP"

# Update security group with current IP (VPN handling)
MY_IP=$(curl -s https://checkip.amazonaws.com)
SG_ID=$(aws ec2 describe-security-groups \
    --group-names $SG_NAME \
    --query 'SecurityGroups[0].GroupId' \
    --output text --region $REGION 2>/dev/null || true)

if [ ! -z "$SG_ID" ]; then
    log_info "🔒 Updating security group $SG_NAME with your current IP: $MY_IP"
    
    # Remove old SSH rules (optional - to keep it clean)
    aws ec2 revoke-security-group-ingress \
        --group-id $SG_ID \
        --protocol tcp \
        --port 22 \
        --cidr 0.0.0.0/0 \
        --region $REGION 2>/dev/null || true
    
    # Add current IP
    aws ec2 authorize-security-group-ingress \
        --group-id $SG_ID \
        --protocol tcp \
        --port 22 \
        --cidr $MY_IP/32 \
        --region $REGION 2>/dev/null && log_info "✅ SSH access added for $MY_IP" || log_warning "⚠️ IP might already be authorized"
fi

# Test SSH connection
log_info "🔑 Testing SSH connection to ubuntu@$PUBLIC_IP..."
if ! ssh -i $KEY_PATH -o ConnectTimeout=10 -o StrictHostKeyChecking=no $EC2_USER@$PUBLIC_IP "echo 'SSH connection successful'"; then
    log_error "Cannot connect to EC2 instance"
    exit 1
fi
log_info "✅ SSH connection successful"

# Get the project folder name
PROJECT_DIR=$(basename "$(pwd)")
log_info "📁 Deploying project: $PROJECT_DIR"

# Method 1: Using rsync (fast, incremental)
log_step "📦 Copying entire project folder to EC2..."
if command -v rsync &> /dev/null; then
    log_info "Using rsync for efficient transfer..."
    rsync -avz --progress \
        -e "ssh -i $KEY_PATH -o StrictHostKeyChecking=no" \
        --exclude="venv" \
        --exclude="__pycache__" \
        --exclude="*.pyc" \
        --exclude=".git" \
        --exclude="*.log" \
        --exclude="*.tmp" \
        --exclude=".env.local" \
        ../$PROJECT_DIR/ $EC2_USER@$PUBLIC_IP:~/$PROJECT_DIR/
else
    # Fallback to tar method
    log_info "rsync not found, using tar method..."
    cd ..
    tar -czf /tmp/${PROJECT_DIR}.tar.gz $PROJECT_DIR/ \
        --exclude="venv" \
        --exclude="__pycache__" \
        --exclude="*.pyc" \
        --exclude=".git"
    
    scp -i $KEY_PATH -o StrictHostKeyChecking=no /tmp/${PROJECT_DIR}.tar.gz $EC2_USER@$PUBLIC_IP:/tmp/
    
    ssh -i $KEY_PATH -o StrictHostKeyChecking=no $EC2_USER@$PUBLIC_IP << ENDSSH
        rm -rf ~/$PROJECT_DIR
        cd ~
        tar -xzf /tmp/${PROJECT_DIR}.tar.gz
        rm /tmp/${PROJECT_DIR}.tar.gz
ENDSSH
    rm /tmp/${PROJECT_DIR}.tar.gz
    cd $PROJECT_DIR
fi

log_info "✅ Project copied successfully!"

# Setup environment on EC2
log_step "🔧 Setting up Python environment on EC2..."
ssh -i $KEY_PATH -o StrictHostKeyChecking=no $EC2_USER@$PUBLIC_IP << ENDSSH
    cd ~/$PROJECT_DIR
    
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    
    echo "📚 Installing dependencies..."
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    
    echo "🔧 Making scripts executable..."
    find scripts/ -name "*.sh" -exec chmod +x {} \; 2>/dev/null || true
    chmod +x src/*.py 2>/dev/null || true
    
    echo "📂 Creating necessary directories..."
    mkdir -p logs data/local
    
    echo "✅ Environment setup complete"
ENDSSH

log_info "✅ Python environment configured"

# Test the pipeline
log_step "🧪 Testing pipeline on EC2..."
if ssh -i $KEY_PATH -o StrictHostKeyChecking=no $EC2_USER@$PUBLIC_IP "cd ~/$PROJECT_DIR && source venv/bin/activate && python3 src/main.py --test"; then
    log_info "✅ Pipeline test successful!"
else
    log_warning "⚠️ Pipeline test failed (might need data or config)"
fi

# Show deployment info
log_step "📋 DEPLOYMENT SUMMARY"
echo "================================================"
echo "✅ Project: $PROJECT_DIR"
echo "✅ EC2 IP: $PUBLIC_IP"
echo "✅ Instance ID: $INSTANCE_ID"
echo "✅ Deployed to: /home/ubuntu/$PROJECT_DIR"
echo "================================================"
echo ""
echo "📝 Commands to run pipeline manually:"
echo "  ssh -i $KEY_PATH ubuntu@$PUBLIC_IP"
echo "  cd ~/$PROJECT_DIR"
echo "  source venv/bin/activate"
echo "  python3 src/main.py"
echo ""
echo "🔄 To run pipeline on schedule:"
echo "  ssh -i $KEY_PATH ubuntu@$PUBLIC_IP"
echo "  cd ~/$PROJECT_DIR"
echo "  crontab -e  # Add: 0 2 * * * cd ~/$PROJECT_DIR && source venv/bin/activate && python3 src/main.py >> logs/cron.log 2>&1"
echo "================================================"

# Optional: Keep connection open or just exit
read -p "🔌 Do you want to SSH into the instance now? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "Connecting to EC2..."
    ssh -i $KEY_PATH ubuntu@$PUBLIC_IP
fi

log_info "🎉 Deployment complete!"
