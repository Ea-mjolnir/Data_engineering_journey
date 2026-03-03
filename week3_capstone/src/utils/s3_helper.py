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
