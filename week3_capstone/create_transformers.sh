#!/bin/bash

# Script to create transformer files
# RUN THIS FROM INSIDE THE week3_capstone FOLDER (you are here now)

echo "🔧 Creating transformer files in src/transformers/..."
echo "Current directory: $(pwd)"

# Create data_cleaner.py
echo "📝 Creating data_cleaner.py..."
cat > src/transformers/data_cleaner.py << 'EOF'
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
EOF

if [ $? -eq 0 ]; then
    echo "   ✅ data_cleaner.py created successfully"
else
    echo "   ❌ Failed to create data_cleaner.py"
fi

# Create data_aggregator.py
echo "📝 Creating data_aggregator.py..."
cat > src/transformers/data_aggregator.py << 'EOF'
"""
Data Aggregation
Generate analytics-ready aggregated data
"""

import pandas as pd
from typing import Dict
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataAggregator:
    """Aggregate data for analytics"""
    
    def create_user_activity_summary(
        self, 
        users_df: pd.DataFrame, 
        posts_df: pd.DataFrame, 
        comments_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Create user activity summary"""
        logger.info("Creating user activity summary...")
        
        # Count posts per user
        post_counts = posts_df.groupby('user_id').size().reset_index(name='total_posts')
        
        # Count words per user
        word_counts = posts_df.groupby('user_id')['word_count'].sum().reset_index(name='total_words')
        
        # Count long posts per user
        long_posts = posts_df[posts_df['is_long_post']].groupby('user_id').size().reset_index(name='long_posts')
        
        # Merge all metrics
        summary = users_df[['user_id', 'username', 'full_name', 'email', 'city', 'company_name']].copy()
        summary = summary.merge(post_counts, on='user_id', how='left')
        summary = summary.merge(word_counts, on='user_id', how='left')
        summary = summary.merge(long_posts, on='user_id', how='left')
        
        # Fill NaN with 0
        summary['total_posts'] = summary['total_posts'].fillna(0).astype(int)
        summary['total_words'] = summary['total_words'].fillna(0).astype(int)
        summary['long_posts'] = summary['long_posts'].fillna(0).astype(int)
        
        # Calculate average words per post
        summary['avg_words_per_post'] = (
            summary['total_words'] / summary['total_posts'].replace(0, 1)
        ).round(2)
        
        # Categorize users by activity
        summary['user_segment'] = pd.cut(
            summary['total_posts'],
            bins=[-1, 0, 5, 10, float('inf')],
            labels=['Inactive', 'Light', 'Moderate', 'Heavy']
        )
        
        logger.info(f"Created summary for {len(summary)} users")
        return summary
    
    def create_post_engagement_summary(
        self, 
        posts_df: pd.DataFrame, 
        comments_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Create post engagement metrics"""
        logger.info("Creating post engagement summary...")
        
        # Count comments per post
        comment_counts = comments_df.groupby('post_id').size().reset_index(name='comment_count')
        
        # Merge with posts
        engagement = posts_df.merge(comment_counts, on='post_id', how='left')
        engagement['comment_count'] = engagement['comment_count'].fillna(0).astype(int)
        
        # Calculate engagement rate (comments per 100 words)
        engagement['engagement_rate'] = (
            engagement['comment_count'] / (engagement['word_count'] / 100)
        ).round(2)
        
        # Categorize engagement
        engagement['engagement_level'] = pd.cut(
            engagement['comment_count'],
            bins=[-1, 0, 3, 5, float('inf')],
            labels=['No Engagement', 'Low', 'Medium', 'High']
        )
        
        logger.info(f"Created engagement metrics for {len(engagement)} posts")
        return engagement
    
    def create_summary_statistics(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """Create overall summary statistics"""
        logger.info("Creating summary statistics...")
        
        stats = {
            'total_users': len(data.get('users', [])),
            'total_posts': len(data.get('posts', [])),
            'total_comments': len(data.get('comments', [])),
            'avg_posts_per_user': 0,
            'avg_comments_per_post': 0,
            'avg_words_per_post': 0
        }
        
        if 'users' in data and 'posts' in data and len(data['users']) > 0:
            stats['avg_posts_per_user'] = round(len(data['posts']) / len(data['users']), 2)
        
        if 'posts' in data and 'comments' in data and len(data['posts']) > 0:
            stats['avg_comments_per_post'] = round(len(data['comments']) / len(data['posts']), 2)
        
        if 'posts' in data and len(data['posts']) > 0:
            stats['avg_words_per_post'] = round(data['posts']['word_count'].mean(), 2)
        
        logger.info("Summary statistics created")
        return stats
EOF

if [ $? -eq 0 ]; then
    echo "   ✅ data_aggregator.py created successfully"
else
    echo "   ❌ Failed to create data_aggregator.py"
fi

# Show results
echo ""
echo "✅ Transformer files created successfully!"
echo ""
echo "📂 Files created in: src/transformers/"
ls -la src/transformers/

echo ""
echo "📍 Your current location: $(pwd)"
echo "💡 Tip: You're inside the week3_capstone folder, so files are in the right place!"
