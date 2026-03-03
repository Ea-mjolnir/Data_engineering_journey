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
