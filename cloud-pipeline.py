
"""
Cloud Data Pipeline
Extracts data from an API, transforms it,
and loads it into Amazon S3
"""

import boto3
import json
import csv
import os
import requests
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_REGION  = os.getenv('AWS_REGION', 'us-east-1')
API_URL     = "https://jsonplaceholder.typicode.com"

class S3Handler:
    """Handle all S3 operations"""

    def __init__(self, bucket_name, region='us-east-1'):
        self.bucket_name = bucket_name
        self.region      = region
        self.s3_client   = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3', region_name=region)

    def upload_string(self, content, s3_key, content_type='text/csv'):
        """Upload string content directly to S3"""
        try:
            self.s3_client.put_object(
                Bucket      = self.bucket_name,
                Key         = s3_key,
                Body        = content.encode('utf-8'),
                ContentType = content_type,
                Metadata    = {
                    'uploaded-at': datetime.utcnow().isoformat(),
                    'source'     : 'cloud-pipeline'
                }
            )
            print(f"  ✓ Uploaded to s3://{self.bucket_name}/{s3_key}")
            return True

        except Exception as e:
            print(f"  ✗ Error uploading to S3: {e}")
            return False

    def upload_json(self, data, s3_key):
        """Upload JSON data to S3"""
        content = json.dumps(data, indent=2, default=str)
        return self.upload_string(content, s3_key, 'application/json')

    def download_string(self, s3_key):
        """Download file content from S3 as string"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            content = response['Body'].read().decode('utf-8')
            print(f"  ✓ Downloaded s3://{self.bucket_name}/{s3_key}")
            return content

        except Exception as e:
            print(f"  ✗ Error downloading from S3: {e}")
            return None

    def file_exists(self, s3_key):
        """Check if a file exists in S3"""
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            return True
        except:
            return False

    def list_files(self, prefix=''):
        """List files in S3 bucket with optional prefix"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages     = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            files = []
            for page in pages:
                for obj in page.get('Contents', []):
                    files.append({
                        'key'           : obj['Key'],
                        'size'          : obj['Size'],
                        'last_modified' : obj['LastModified'].isoformat()
                    })
            return files

        except Exception as e:
            print(f"  ✗ Error listing S3 files: {e}")
            return []

    def move_file(self, source_key, dest_key):
        """Move file within S3 (copy then delete)"""
        try:
            # Copy to new location
            self.s3_client.copy_object(
                CopySource={'Bucket': self.bucket_name, 'Key': source_key},
                Bucket=self.bucket_name,
                Key=dest_key
            )
            # Delete original
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=source_key
            )
            print(f"  ✓ Moved {source_key} → {dest_key}")
            return True

        except Exception as e:
            print(f"  ✗ Error moving S3 file: {e}")
            return False


class DataExtractor:
    """Extract data from various sources"""

    def fetch_users(self):
        """Fetch user data from API"""
        print("  Fetching users from API...")
        try:
            response = requests.get(f"{API_URL}/users", timeout=10)
            response.raise_for_status()
            users = response.json()
            print(f"  ✓ Fetched {len(users)} users")
            return users
        except Exception as e:
            print(f"  ✗ Error fetching users: {e}")
            return []

    def fetch_posts(self):
        """Fetch posts data from API"""
        print("  Fetching posts from API...")
        try:
            response = requests.get(f"{API_URL}/posts", timeout=10)
            response.raise_for_status()
            posts = response.json()
            print(f"  ✓ Fetched {len(posts)} posts")
            return posts
        except Exception as e:
            print(f"  ✗ Error fetching posts: {e}")
            return []


class DataTransformer:
    """Transform raw data into clean structures"""

    def transform_users(self, raw_users):
        """Flatten and clean user data"""
        print("  Transforming users...")
        transformed = []

        for user in raw_users:
            transformed.append({
                'user_id'       : user['id'],
                'username'      : user['username'],
                'full_name'     : user['name'],
                'email'         : user['email'],
                'phone'         : user.get('phone', ''),
                'website'       : user.get('website', ''),
                'city'          : user.get('address', {}).get('city', ''),
                'state'         : user.get('address', {}).get('state', ''),
                'zip'           : user.get('address', {}).get('zipcode', ''),
                'company'       : user.get('company', {}).get('name', ''),
                'loaded_at'     : datetime.utcnow().isoformat()
            })

        print(f"  ✓ Transformed {len(transformed)} users")
        return transformed

    def transform_posts(self, raw_posts):
        """Add metadata to posts"""
        print("  Transforming posts...")
        transformed = []

        for post in raw_posts:
            word_count   = len(post['body'].split())
            char_count   = len(post['body'])
            transformed.append({
                'post_id'           : post['id'],
                'user_id'           : post['userId'],
                'title'             : post['title'],
                'body'              : post['body'],
                'word_count'        : word_count,
                'char_count'        : char_count,
                'title_length'      : len(post['title']),
                'is_long_post'      : word_count > 50,
                'loaded_at'         : datetime.utcnow().isoformat()
            })

        print(f"  ✓ Transformed {len(transformed)} posts")
        return transformed

    def create_user_post_summary(self, users, posts):
        """Join users and posts to create summary"""
        print("  Creating user-post summary...")

        # Create users lookup
        user_lookup = {user['user_id']: user for user in users}

        # Group posts by user
        user_posts = {}
        for post in posts:
            uid = post['user_id']
            if uid not in user_posts:
                user_posts[uid] = []
            user_posts[uid].append(post)

        # Build summary
        summary = []
        for uid, user in user_lookup.items():
            user_post_list   = user_posts.get(uid, [])
            total_words      = sum(p['word_count'] for p in user_post_list)
            summary.append({
                'user_id'         : uid,
                'username'        : user['username'],
                'full_name'       : user['full_name'],
                'email'           : user['email'],
                'city'            : user['city'],
                'company'         : user['company'],
                'total_posts'     : len(user_post_list),
                'total_words'     : total_words,
                'avg_words_post'  : round(total_words / max(len(user_post_list), 1), 2),
                'loaded_at'       : datetime.utcnow().isoformat()
            })

        print(f"  ✓ Created summary for {len(summary)} users")
        return summary


class DataLoader:
    """Load data to S3 in different formats"""

    def __init__(self, s3_handler):
        self.s3 = s3_handler

    def to_csv_string(self, data):
        """Convert list of dicts to CSV string"""
        if not data:
            return ""

        output    = StringIO()
        fieldnames = list(data[0].keys())
        writer    = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    def load_to_raw(self, data, filename):
        """Load raw JSON to S3"""
        today   = datetime.utcnow().strftime('%Y/%m/%d')
        s3_key  = f"data/raw/{today}/{filename}.json"
        return self.s3.upload_json(data, s3_key), s3_key

    def load_to_staging(self, data, filename):
        """Load transformed CSV to S3 staging"""
        today      = datetime.utcnow().strftime('%Y/%m/%d')
        s3_key     = f"data/staging/{today}/{filename}.csv"
        csv_content = self.to_csv_string(data)
        return self.s3.upload_string(csv_content, s3_key), s3_key

    def load_to_processed(self, data, filename):
        """Load final data to S3 processed zone"""
        today      = datetime.utcnow().strftime('%Y/%m/%d')
        s3_key     = f"data/processed/{today}/{filename}.csv"
        csv_content = self.to_csv_string(data)
        return self.s3.upload_string(csv_content, s3_key), s3_key


def log(message):
    """Simple logging function"""
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")


def run_pipeline():
    """Execute the full cloud ETL pipeline"""
    start_time = datetime.utcnow()
    log("=" * 60)
    log("Starting Cloud Data Pipeline")
    log("=" * 60)

    # Validate config
    if not BUCKET_NAME:
        log("ERROR: S3_BUCKET_NAME not set in .env file")
        return False

    # Initialize components
    s3        = S3Handler(BUCKET_NAME, AWS_REGION)
    extractor = DataExtractor()
    transform = DataTransformer()
    loader    = DataLoader(s3)

    pipeline_log = {
        'pipeline_start'   : start_time.isoformat(),
        'bucket'           : BUCKET_NAME,
        'steps'            : []
    }

    try:
        # ─── EXTRACT ───────────────────────────────────────────────
        log("\nStep 1: EXTRACT")
        raw_users = extractor.fetch_users()
        raw_posts = extractor.fetch_posts()

        if not raw_users or not raw_posts:
            log("ERROR: Extraction failed")
            return False

        # Load raw data to S3
        success, users_raw_key = loader.load_to_raw(raw_users, 'users')
        success, posts_raw_key = loader.load_to_raw(raw_posts, 'posts')

        pipeline_log['steps'].append({
            'step'   : 'extract',
            'status' : 'success',
            'records': {'users': len(raw_users), 'posts': len(raw_posts)}
        })

        # ─── TRANSFORM ─────────────────────────────────────────────
        log("\nStep 2: TRANSFORM")
        clean_users   = transform.transform_users(raw_users)
        clean_posts   = transform.transform_posts(raw_posts)
        user_summary  = transform.create_user_post_summary(clean_users, clean_posts)

        # Load staging data to S3
        loader.load_to_staging(clean_users, 'users')
        loader.load_to_staging(clean_posts, 'posts')

        pipeline_log['steps'].append({
            'step'   : 'transform',
            'status' : 'success',
            'records': {'users': len(clean_users), 'posts': len(clean_posts)}
        })

        # ─── LOAD ──────────────────────────────────────────────────
        log("\nStep 3: LOAD")
        loader.load_to_processed(user_summary, 'user_post_summary')
        loader.load_to_processed(clean_users, 'users_final')
        loader.load_to_processed(clean_posts, 'posts_final')

        pipeline_log['steps'].append({
            'step'   : 'load',
            'status' : 'success',
            'files'  : ['user_post_summary', 'users_final', 'posts_final']
        })

        # ─── PIPELINE SUMMARY ──────────────────────────────────────
        end_time     = datetime.utcnow()
        duration     = (end_time - start_time).total_seconds()
        pipeline_log['pipeline_end']    = end_time.isoformat()
        pipeline_log['duration_seconds'] = duration
        pipeline_log['status']           = 'success'

        # Save pipeline log to S3
        log_key = f"logs/pipeline_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        s3.upload_json(pipeline_log, log_key)

        log("\n" + "=" * 60)
        log("Pipeline Execution Summary")
        log("=" * 60)
        log(f"Status         : SUCCESS")
        log(f"Duration       : {duration:.2f} seconds")
        log(f"Users processed: {len(clean_users)}")
        log(f"Posts processed: {len(clean_posts)}")
        log(f"Summaries      : {len(user_summary)}")
        log(f"Log saved to   : s3://{BUCKET_NAME}/{log_key}")
        log("=" * 60)

        return True

    except Exception as e:
        log(f"ERROR: Pipeline failed: {e}")
        pipeline_log['status'] = 'failed'
        pipeline_log['error']  = str(e)
        s3.upload_json(pipeline_log, f"logs/failed_pipeline_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
        return False


if __name__ == "__main__":
    success = run_pipeline()
    exit(0 if success else 1)
