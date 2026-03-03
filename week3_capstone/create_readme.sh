#!/bin/bash

# Script to create README.md in the current directory
# Run this from INSIDE your week3_capstone folder

echo "📖 Creating README.md in $(pwd)..."

cat > README.md << 'EOF'
# E-Commerce Analytics Pipeline

A production-grade data engineering pipeline that processes e-commerce data through a medallion architecture (Bronze → Silver → Gold) on AWS S3.

## Features

- **Multi-source data extraction**: REST APIs, CSV files, JSON files
- **Robust error handling**: Retry logic, validation, logging
- **Medallion architecture**: Bronze (raw), Silver (cleaned), Gold (analytics)
- **Automated deployment**: Runs on AWS EC2 with cron scheduling
- **Comprehensive monitoring**: Logging, metrics, alerts
- **Full test coverage**: Unit tests for all components

## Architecture

See `docs/ARCHITECTURE.md` for detailed system design.

## Quick Start

1. Clone repository
2. Copy `.env.example` to `.env` and configure
3. Install dependencies: `pip install -r requirements.txt`
4. Run pipeline: `python src/main.py`

## Project Structure
