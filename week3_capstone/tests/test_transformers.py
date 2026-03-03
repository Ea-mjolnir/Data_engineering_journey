"""
Unit tests for data transformers
"""

import pytest
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.transformers.data_cleaner import DataCleaner


def test_clean_users():
    """Test user data cleaning"""
    raw_users = [
        {
            'id': 1,
            'name': 'John Doe',
            'username': 'johnd',
            'email': 'JOHN@EMAIL.COM',
            'address': {
                'city': 'New York',
                'street': 'Main St',
                'geo': {'lat': '40.7128', 'lng': '-74.0060'}
            },
            'company': {'name': 'Tech Corp'}
        }
    ]
    
    cleaner = DataCleaner()
    df = cleaner.clean_users(raw_users)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df['user_id'].iloc[0] == 1
    assert df['full_name'].iloc[0] == 'John Doe'
    assert df['email'].iloc[0] == 'john@email.com'  # Should be lowercase
    assert df['city'].iloc[0] == 'New York'
    assert 'username' in df.columns
    assert 'company_name' in df.columns
    assert pd.notna(df['latitude'].iloc[0])
    assert pd.notna(df['longitude'].iloc[0])


def test_clean_posts():
    """Test post data cleaning"""
    raw_posts = [
        {
            'id': 1,
            'userId': 1,
            'title': 'Test Post Title',
            'body': 'This is a test post with some content that has multiple words for testing'
        }
    ]
    
    cleaner = DataCleaner()
    df = cleaner.clean_posts(raw_posts)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df['post_id'].iloc[0] == 1
    assert df['user_id'].iloc[0] == 1
    assert df['title'].iloc[0] == 'Test Post Title'
    assert df['body'].iloc[0] is not None
    assert df['word_count'].iloc[0] > 0
    assert df['title_length'].iloc[0] == len('Test Post Title')


def test_clean_comments():
    """Test comment data cleaning"""
    raw_comments = [
        {
            'id': 1,
            'postId': 1,
            'name': 'Test Comment',
            'email': 'TEST@EMAIL.COM',
            'body': 'This is a test comment body'
        }
    ]
    
    cleaner = DataCleaner()
    df = cleaner.clean_comments(raw_comments)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df['comment_id'].iloc[0] == 1
    assert df['post_id'].iloc[0] == 1
    assert df['commenter_name'].iloc[0] == 'Test Comment'
    assert df['commenter_email'].iloc[0] == 'test@email.com'  # Should be lowercase
    assert df['comment_body'].iloc[0] == 'This is a test comment body'
