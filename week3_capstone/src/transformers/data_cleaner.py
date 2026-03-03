"""
Data Cleaning and Transformation
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataCleaner:
    """Clean and transform raw data"""
    
    def clean_users(self, raw_users: List[Dict]) -> pd.DataFrame:
        """Clean and flatten user data"""
        logger.info(f"Cleaning {len(raw_users)} users...")
        
        cleaned = []
        for user in raw_users:
            cleaned.append({
                'user_id': user['id'],
                'username': user.get('username', ''),
                'full_name': user.get('name', ''),
                'email': user.get('email', '').lower(),
                'phone': self._clean_phone(user.get('phone', '')),
                'website': user.get('website', ''),
                'city': user.get('address', {}).get('city', ''),
                'street': user.get('address', {}).get('street', ''),
                'suite': user.get('address', {}).get('suite', ''),
                'zipcode': user.get('address', {}).get('zipcode', ''),
                'latitude': user.get('address', {}).get('geo', {}).get('lat', None),
                'longitude': user.get('address', {}).get('geo', {}).get('lng', None),
                'company_name': user.get('company', {}).get('name', ''),
                'company_catchphrase': user.get('company', {}).get('catchPhrase', ''),
                'company_bs': user.get('company', {}).get('bs', ''),
                'processed_at': datetime.utcnow().isoformat()
            })
        
        df = pd.DataFrame(cleaned)
        
        # Convert types
        df['user_id'] = df['user_id'].astype(int)
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['user_id'])
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} duplicate users")
        
        logger.info(f"Cleaned {len(df)} users")
        return df
    
    def clean_posts(self, raw_posts: List[Dict]) -> pd.DataFrame:
        """Clean and enrich post data"""
        logger.info(f"Cleaning {len(raw_posts)} posts...")
        
        cleaned = []
        for post in raw_posts:
            title = post.get('title', '')
            body = post.get('body', '')
            
            cleaned.append({
                'post_id': post['id'],
                'user_id': post['userId'],
                'title': title,
                'body': body,
                'title_length': len(title),
                'body_length': len(body),
                'word_count': len(body.split()),
                'char_count': len(body),
                'is_long_post': len(body.split()) > 50,
                'processed_at': datetime.utcnow().isoformat()
            })
        
        df = pd.DataFrame(cleaned)
        
        # Convert types
        df['post_id'] = df['post_id'].astype(int)
        df['user_id'] = df['user_id'].astype(int)
        df['is_long_post'] = df['is_long_post'].astype(bool)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['post_id'])
        
        logger.info(f"Cleaned {len(df)} posts")
        return df
    
    def clean_comments(self, raw_comments: List[Dict]) -> pd.DataFrame:
        """Clean comment data"""
        logger.info(f"Cleaning {len(raw_comments)} comments...")
        
        cleaned = []
        for comment in raw_comments:
            cleaned.append({
                'comment_id': comment['id'],
                'post_id': comment['postId'],
                'commenter_name': comment.get('name', ''),
                'commenter_email': comment.get('email', '').lower(),
                'comment_body': comment.get('body', ''),
                'comment_length': len(comment.get('body', '')),
                'processed_at': datetime.utcnow().isoformat()
            })
        
        df = pd.DataFrame(cleaned)
        
        # Convert types
        df['comment_id'] = df['comment_id'].astype(int)
        df['post_id'] = df['post_id'].astype(int)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['comment_id'])
        
        logger.info(f"Cleaned {len(df)} comments")
        return df
    
    @staticmethod
    def _clean_phone(phone: str) -> str:
        """Clean phone number"""
        if not phone:
            return ''
        # Remove non-numeric characters except + and -
        return ''.join(c for c in phone if c.isdigit() or c in ['+', '-', ' ', '(', ')'])
