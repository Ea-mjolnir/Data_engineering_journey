#!/bin/bash

# Script to create extractor files in src/extractors/
# Run this from INSIDE your week3_capstone folder

echo "🔧 Creating extractor files in src/extractors/..."
echo "Current directory: $(pwd)"

# Create api_extractor.py
echo "📝 Creating api_extractor.py..."
cat > src/extractors/api_extractor.py << 'EOF'
"""
API Data Extractor
Extracts data from REST APIs with retry logic
"""

import requests
import time
from typing import Dict, List, Optional
from ..utils.logger import get_logger
from ..utils.config import Config

logger = get_logger(__name__)

class APIExtractor:
    """Extract data from REST APIs"""
    
    def __init__(self, base_url=None, timeout=None, max_retries=None):
        self.base_url = base_url or Config.API_BASE_URL
        self.timeout = timeout or Config.API_TIMEOUT
        self.max_retries = max_retries or Config.API_MAX_RETRIES
        self.session = requests.Session()
        
    def _request_with_retry(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Attempt {attempt + 1}/{self.max_retries}: {method} {url}")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.timeout,
                    **kwargs
                )
                response.raise_for_status()
                
                return response
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code >= 500:
                    logger.warning(f"Server error on attempt {attempt + 1}: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                else:
                    logger.error(f"Client error: {e}")
                    raise
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"All retry attempts failed for {url}")
        return None
    
    def fetch_users(self) -> List[Dict]:
        """Fetch user data"""
        logger.info("Fetching users from API...")
        
        url = f"{self.base_url}/users"
        response = self._request_with_retry(url)
        
        if response:
            users = response.json()
            logger.info(f"Fetched {len(users)} users")
            return users
        
        return []
    
    def fetch_posts(self) -> List[Dict]:
        """Fetch posts data"""
        logger.info("Fetching posts from API...")
        
        url = f"{self.base_url}/posts"
        response = self._request_with_retry(url)
        
        if response:
            posts = response.json()
            logger.info(f"Fetched {len(posts)} posts")
            return posts
        
        return []
    
    def fetch_comments(self) -> List[Dict]:
        """Fetch comments data"""
        logger.info("Fetching comments from API...")
        
        url = f"{self.base_url}/comments"
        response = self._request_with_retry(url)
        
        if response:
            comments = response.json()
            logger.info(f"Fetched {len(comments)} comments")
            return comments
        
        return []
    
    def extract_all(self) -> Dict[str, List[Dict]]:
        """Extract all data sources"""
        logger.info("Starting full data extraction...")
        
        data = {
            'users': self.fetch_users(),
            'posts': self.fetch_posts(),
            'comments': self.fetch_comments()
        }
        
        total_records = sum(len(v) for v in data.values())
        logger.info(f"Extraction complete: {total_records} total records")
        
        return data
EOF

# Check if api_extractor.py was created successfully
if [ -f "src/extractors/api_extractor.py" ]; then
    echo "   ✅ api_extractor.py created successfully"
else
    echo "   ❌ Failed to create api_extractor.py"
fi

# Create file_extractor.py
echo "📝 Creating file_extractor.py..."
cat > src/extractors/file_extractor.py << 'EOF'
"""
File Data Extractor
Extracts data from CSV and JSON files
"""

import csv
import json
from pathlib import Path
from typing import List, Dict
from ..utils.logger import get_logger

logger = get_logger(__name__)

class FileExtractor:
    """Extract data from files"""
    
    def read_csv(self, filepath: str) -> List[Dict]:
        """Read CSV file"""
        try:
            filepath = Path(filepath)
            
            if not filepath.exists():
                logger.error(f"File not found: {filepath}")
                return []
            
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
            
            logger.info(f"Read {len(data)} records from {filepath.name}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            return []
    
    def read_json(self, filepath: str) -> Dict:
        """Read JSON file"""
        try:
            filepath = Path(filepath)
            
            if not filepath.exists():
                logger.error(f"File not found: {filepath}")
                return {}
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Read JSON from {filepath.name}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read JSON: {e}")
            return {}
    
    def read_json_lines(self, filepath: str) -> List[Dict]:
        """Read JSON Lines file (one JSON object per line)"""
        try:
            filepath = Path(filepath)
            
            if not filepath.exists():
                logger.error(f"File not found: {filepath}")
                return []
            
            data = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        data.append(json.loads(line))
            
            logger.info(f"Read {len(data)} records from {filepath.name}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read JSON Lines: {e}")
            return []
EOF

# Check if file_extractor.py was created successfully
if [ -f "src/extractors/file_extractor.py" ]; then
    echo "   ✅ file_extractor.py created successfully"
else
    echo "   ❌ Failed to create file_extractor.py"
fi

# Show the files created
echo ""
echo "📂 Files created in src/extractors/:"
ls -la src/extractors/ | grep -E 'api_extractor|file_extractor'

echo ""
echo "✅ Extractor files created successfully!"
echo "📍 Location: $(pwd)/src/extractors/"
echo ""
echo "To verify:"
echo "  ls -la src/extractors/"
echo "  head -5 src/extractors/api_extractor.py"
echo "  head -5 src/extractors/file_extractor.py"
