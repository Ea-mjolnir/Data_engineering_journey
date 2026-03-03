#!/bin/bash

# Script to create all Python utility modules for the data pipeline
# Run this from INSIDE your week3_capstone folder

echo "🔧 Creating Python utility modules in existing src/ structure..."

# Create logger.py
echo "📝 Creating logger.py..."
cat > src/utils/logger.py << 'EOF'
"""
Logging utility for the data pipeline
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Set up logger with console and file handlers
    
    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file provided)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_logger(name):
    """Get existing logger by name"""
    return logging.getLogger(name)
EOF
echo "   ✅ logger.py created"

# Create config.py
echo "📝 Creating config.py..."
cat > src/utils/config.py << 'EOF'
"""
Configuration management
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Application configuration"""
    
    # AWS
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    
    # API
    API_BASE_URL = os.getenv('API_BASE_URL', 'https://jsonplaceholder.typicode.com')
    API_TIMEOUT = int(os.getenv('API_TIMEOUT', '30'))
    API_MAX_RETRIES = int(os.getenv('API_MAX_RETRIES', '3'))
    
    # Pipeline
    PIPELINE_NAME = os.getenv('PIPELINE_NAME', 'ecommerce-analytics')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_DIR = PROJECT_ROOT / 'data'
    LOGS_DIR = PROJECT_ROOT / 'logs'
    
    # S3 Paths
    S3_BRONZE_PREFIX = 'bronze'
    S3_SILVER_PREFIX = 'silver'
    S3_GOLD_PREFIX = 'gold'
    S3_LOGS_PREFIX = 'logs'
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = ['S3_BUCKET_NAME']
        missing = [key for key in required if not getattr(cls, key)]
        
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        return True

# Validate on import
Config.validate()
EOF
echo "   ✅ config.py created"

# Create s3_helper.py
echo "📝 Creating s3_helper.py..."
cat > src/utils/s3_helper.py << 'EOF'
"""
S3 Helper utilities
"""

import boto3
import json
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError
from .logger import get_logger

logger = get_logger(__name__)

class S3Helper:
    """Helper class for S3 operations"""
    
    def __init__(self, bucket_name, region='us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3', region_name=region)
        
    def upload_json(self, data, s3_key, metadata=None):
        """Upload JSON data to S3"""
        try:
            content = json.dumps(data, indent=2, default=str)
            
            extra_args = {
                'ContentType': 'application/json',
                'Metadata': metadata or {}
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content.encode('utf-8'),
                **extra_args
            )
            
            logger.info(f"Uploaded JSON to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload JSON: {e}")
            return False
    
    def upload_file(self, local_path, s3_key, metadata=None):
        """Upload file to S3"""
        try:
            extra_args = {'Metadata': metadata or {}}
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Uploaded file to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            return False
    
    def download_file(self, s3_key, local_path):
        """Download file from S3"""
        try:
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_path)
            )
            
            logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            return False
    
    def list_objects(self, prefix=''):
        """List objects in S3 bucket"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            objects = []
            for page in pages:
                for obj in page.get('Contents', []):
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            return objects
            
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            return []
    
    def delete_object(self, s3_key):
        """Delete object from S3"""
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            logger.info(f"Deleted s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete object: {e}")
            return False
    
    def object_exists(self, s3_key):
        """Check if object exists in S3"""
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            return True
        except ClientError:
            return False
EOF
echo "   ✅ s3_helper.py created"

# Create __init__.py files (if they don't exist)
echo "📝 Creating __init__.py files..."
touch src/__init__.py
touch src/utils/__init__.py
touch src/extractors/__init__.py
touch src/transformers/__init__.py
touch src/loaders/__init__.py
echo "   ✅ __init__.py files created/verified"

# Show summary
echo ""
echo "✅ All Python utility modules created successfully!"
echo ""
echo "📂 Files created in src/utils/:"
ls -la src/utils/ | grep -E 'logger|config|s3_helper' || echo "   No files found"

echo ""
echo "🎉 Utility setup complete! Your src/utils/ folder now contains:"
echo "   - logger.py    (Logging functionality)"
echo "   - config.py    (Configuration management)"
echo "   - s3_helper.py (S3 helper utilities)"
echo "   - __init__.py  (Makes it a Python package)"
