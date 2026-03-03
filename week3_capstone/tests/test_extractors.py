"""
Unit tests for data extractors
"""

import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extractors.api_extractor import APIExtractor


def test_api_extractor_initialization():
    """Test API extractor initialization"""
    extractor = APIExtractor()
    assert extractor.base_url is not None
    assert extractor.timeout > 0
    assert extractor.max_retries > 0


def test_fetch_users():
    """Test fetching users from API"""
    extractor = APIExtractor()
    users = extractor.fetch_users()
    
    assert isinstance(users, list)
    assert len(users) > 0
    assert 'id' in users[0]
    assert 'name' in users[0]
    assert 'email' in users[0]


def test_fetch_posts():
    """Test fetching posts from API"""
    extractor = APIExtractor()
    posts = extractor.fetch_posts()
    
    assert isinstance(posts, list)
    assert len(posts) > 0
    assert 'id' in posts[0]
    assert 'userId' in posts[0]
    assert 'title' in posts[0]
    assert 'body' in posts[0]


def test_fetch_comments():
    """Test fetching comments from API"""
    extractor = APIExtractor()
    comments = extractor.fetch_comments()
    
    assert isinstance(comments, list)
    assert len(comments) > 0
    assert 'id' in comments[0]
    assert 'postId' in comments[0]
    assert 'name' in comments[0]
    assert 'email' in comments[0]
    assert 'body' in comments[0]


def test_extract_all():
    """Test extracting all data"""
    extractor = APIExtractor()
    data = extractor.extract_all()
    
    assert 'users' in data
    assert 'posts' in data
    assert 'comments' in data
    assert len(data['users']) > 0
    assert len(data['posts']) > 0
    assert len(data['comments']) > 0
