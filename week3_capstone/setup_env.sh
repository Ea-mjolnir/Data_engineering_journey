#!/bin/bash

# Script to set up environment files for the project
# Run this from INSIDE your week3_capstone folder

echo "🔧 Setting up environment configuration..."

# Create .env.example template
echo "📝 Creating .env.example template..."
cat > .env.example << 'EOF'
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
S3_BRONZE_BUCKET=your-bronze-bucket-name
S3_SILVER_BUCKET=your-silver-bucket-name
S3_GOLD_BUCKET=your-gold-bucket-name

# Data Sources
API_BASE_URL=https://jsonplaceholder.typicode.com
API_TIMEOUT=30
API_MAX_RETRIES=3

# Database Configuration (if needed)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_db
DB_USER=postgres
DB_PASSWORD=your-password

# Pipeline Configuration
PIPELINE_NAME=ecommerce-analytics
ENVIRONMENT=development
LOG_LEVEL=INFO

# Notification (Optional)
ALERT_EMAIL=your-email@example.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
EOF

echo "✅ .env.example created"

# Copy to .env
echo "📋 Copying to .env..."
cp .env.example .env
echo "✅ .env created"

# Open .env in nano for editing
echo ""
echo "✏️  Opening .env in nano for you to edit..."
echo "   Replace the placeholder values with your actual configuration."
echo "   When done: Ctrl+O to save, Enter to confirm, Ctrl+X to exit."
echo ""
read -p "Press Enter to continue and open nano..."

nano .env

# Confirmation
echo ""
echo "✅ Environment setup complete!"
echo "📄 Files created:"
ls -la .env .env.example 2>/dev/null || echo "Files not found"
echo ""
echo "🔒 Remember: .env contains your secrets and should NEVER be committed to Git"
echo "   (It's already in your .gitignore)"
