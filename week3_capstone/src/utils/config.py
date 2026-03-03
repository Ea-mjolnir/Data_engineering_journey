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
    
    # AWS - CORRECTED: Use environment variable names, not bucket names
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BRONZE_BUCKET = os.getenv('S3_BRONZE_BUCKET')  # ← Fixed!
    S3_SILVER_BUCKET = os.getenv('S3_SILVER_BUCKET')  # ← Fixed!
    S3_GOLD_BUCKET = os.getenv('S3_GOLD_BUCKET')      # ← Fixed!
    
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
    
    # S3 Paths (these are prefixes/folders, not bucket names)
    S3_BRONZE_PREFIX = 'bronze'
    S3_SILVER_PREFIX = 'silver'
    S3_GOLD_PREFIX = 'gold'
    S3_LOGS_PREFIX = 'logs'
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        # CORRECTED: Check the right variable names
        required = ['S3_BRONZE_BUCKET', 'S3_SILVER_BUCKET', 'S3_GOLD_BUCKET']
        missing = [key for key in required if not getattr(cls, key, None)]
        
        if missing:
            print(f"⚠️  Warning: Missing environment variables: {', '.join(missing)}")
            print("Please check your .env file contains these variables")
            # Don't raise error in development, just warn
            return False
        
        print("✅ Configuration validated successfully")
        return True

# Optional: Print config on load for debugging
if __name__ == "__main__":
    print("📋 Current Configuration:")
    print(f"AWS Region: {Config.AWS_REGION}")
    print(f"Bronze Bucket: {Config.S3_BRONZE_BUCKET}")
    print(f"Silver Bucket: {Config.S3_SILVER_BUCKET}")
    print(f"Gold Bucket: {Config.S3_GOLD_BUCKET}")
    print(f"Environment: {Config.ENVIRONMENT}")
    Config.validate()
