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
