#!/bin/bash
# Connect to your EC2 instance - handles start, IP change, and SSH

REGION="us-east-1"
KEY_PATH="$HOME/.ssh/aws-ec2-key"
SG_NAME="data-engineering-sg"
TAG_NAME="data-pipeline-server"

echo "🚀 Connecting to EC2 instance..."

# Get instance ID
INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$TAG_NAME" \
    --query 'Reservations[0].Instances[0].InstanceId' \
    --output text --region $REGION)

# Check state
STATE=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text --region $REGION)

# Start if stopped
if [ "$STATE" = "stopped" ]; then
    echo "🔄 Instance is stopped. Starting it now..."
    aws ec2 start-instances --instance-ids $INSTANCE_ID --region $REGION > /dev/null
    aws ec2 wait instance-running --instance-ids $INSTANCE_ID --region $REGION
    echo "✅ Instance started!"
    sleep 5
elif [ "$STATE" = "running" ]; then
    echo "✅ Instance is already running"
else
    echo "⚠️  Instance state: $STATE"
fi

# Get public IP
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text --region $REGION)

echo "📡 Public IP: $PUBLIC_IP"

# Update security group with current IP
MY_IP=$(curl -s https://checkip.amazonaws.com)
SG_ID=$(aws ec2 describe-security-groups \
    --group-names $SG_NAME \
    --query 'SecurityGroups[0].GroupId' \
    --output text --region $REGION)

aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID \
    --protocol tcp \
    --port 22 \
    --cidr $MY_IP/32 \
    --region $REGION 2>/dev/null && echo "✅ SSH access added for $MY_IP"

# SSH into instance
echo "🔑 Connecting to ubuntu@$PUBLIC_IP ..."
ssh -i $KEY_PATH ubuntu@$PUBLIC_IP
