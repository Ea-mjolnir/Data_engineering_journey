#!/usr/bin/env python3
"""
E-Commerce Analytics Pipeline
Main orchestrator for the complete ETL pipeline
"""

import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.api_extractor import APIExtractor
from src.extractors.file_extractor import FileExtractor
from src.transformers.data_cleaner import DataCleaner
from src.transformers.data_aggregator import DataAggregator
from src.loaders.s3_loader import S3Loader
from src.utils.logger import setup_logger
from src.utils.config import Config

# Setup logging
log_file = Config.LOGS_DIR / f"pipeline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
logger = setup_logger('main', log_file=log_file, level=Config.LOG_LEVEL)


class PipelineOrchestrator:
    """Main pipeline orchestrator"""
    
    def __init__(self):
        self.api_extractor = APIExtractor()
        self.file_extractor = FileExtractor()
        self.cleaner = DataCleaner()
        self.aggregator = DataAggregator()
        self.loader = S3Loader()
        
        self.pipeline_metadata = {
            'pipeline_name': Config.PIPELINE_NAME,
            'environment': Config.ENVIRONMENT,
            'start_time': datetime.utcnow().isoformat(),
            'status': 'running',
            'steps': []
        }
    
    def extract(self):
        """Extract data from all sources"""
        logger.info("=" * 60)
        logger.info("STEP 1: EXTRACTION")
        logger.info("=" * 60)
        
        step_start = datetime.utcnow()
        
        try:
            # Extract from API
            api_data = self.api_extractor.extract_all()
            
            # Load raw data to Bronze layer
            bronze_files = self.loader.load_to_bronze(api_data, 'api')
            
            self.pipeline_metadata['steps'].append({
                'step': 'extract',
                'status': 'success',
                'duration_seconds': (datetime.utcnow() - step_start).total_seconds(),
                'records_extracted': {
                    'users': len(api_data.get('users', [])),
                    'posts': len(api_data.get('posts', [])),
                    'comments': len(api_data.get('comments', []))
                },
                'bronze_files': bronze_files
            })
            
            logger.info(f"✓ Extraction complete: {sum(len(v) for v in api_data.values())} total records")
            return api_data
            
        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            self.pipeline_metadata['steps'].append({
                'step': 'extract',
                'status': 'failed',
                'error': str(e)
            })
            raise
    
    def transform(self, raw_data):
        """Transform and clean data"""
        logger.info("=" * 60)
        logger.info("STEP 2: TRANSFORMATION")
        logger.info("=" * 60)
        
        step_start = datetime.utcnow()
        
        try:
            # Clean individual datasets
            users_df = self.cleaner.clean_users(raw_data['users'])
            posts_df = self.cleaner.clean_posts(raw_data['posts'])
            comments_df = self.cleaner.clean_comments(raw_data['comments'])
            
            # Load to Silver layer
            silver_files = {
                'users': self.loader.load_to_silver(users_df, 'users'),
                'posts': self.loader.load_to_silver(posts_df, 'posts'),
                'comments': self.loader.load_to_silver(comments_df, 'comments')
            }
            
            cleaned_data = {
                'users': users_df,
                'posts': posts_df,
                'comments': comments_df
            }
            
            self.pipeline_metadata['steps'].append({
                'step': 'transform',
                'status': 'success',
                'duration_seconds': (datetime.utcnow() - step_start).total_seconds(),
                'records_cleaned': {
                    'users': len(users_df),
                    'posts': len(posts_df),
                    'comments': len(comments_df)
                },
                'silver_files': silver_files
            })
            
            logger.info(f"✓ Transformation complete")
            return cleaned_data
            
        except Exception as e:
            logger.error(f"✗ Transformation failed: {e}")
            self.pipeline_metadata['steps'].append({
                'step': 'transform',
                'status': 'failed',
                'error': str(e)
            })
            raise
    
    def aggregate(self, cleaned_data):
        """Aggregate data for analytics"""
        logger.info("=" * 60)
        logger.info("STEP 3: AGGREGATION")
        logger.info("=" * 60)
        
        step_start = datetime.utcnow()
        
        try:
            # Create aggregated views
            user_activity = self.aggregator.create_user_activity_summary(
                cleaned_data['users'],
                cleaned_data['posts'],
                cleaned_data['comments']
            )
            
            post_engagement = self.aggregator.create_post_engagement_summary(
                cleaned_data['posts'],
                cleaned_data['comments']
            )
            
            summary_stats = self.aggregator.create_summary_statistics(cleaned_data)
            
            # Load to Gold layer
            gold_files = {
                'user_activity': self.loader.load_to_gold(user_activity, 'user_activity_summary', 'csv'),
                'post_engagement': self.loader.load_to_gold(post_engagement, 'post_engagement_summary', 'csv'),
            }
            
            # Save summary statistics as JSON
            stats_key = f"{Config.S3_GOLD_PREFIX}/reports/{self.loader.date_partition}/summary_statistics.json"
            self.loader.gold_s3.upload_json(summary_stats, stats_key)  # ← FIXED: Use gold_s3
            gold_files['summary_stats'] = stats_key
            
            self.pipeline_metadata['steps'].append({
                'step': 'aggregate',
                'status': 'success',
                'duration_seconds': (datetime.utcnow() - step_start).total_seconds(),
                'reports_generated': list(gold_files.keys()),
                'gold_files': gold_files,
                'summary_statistics': summary_stats
            })
            
            logger.info(f"✓ Aggregation complete: {len(gold_files)} reports generated")
            return gold_files, summary_stats
            
        except Exception as e:
            logger.error(f"✗ Aggregation failed: {e}")
            self.pipeline_metadata['steps'].append({
                'step': 'aggregate',
                'status': 'failed',
                'error': str(e)
            })
            raise
    
    def run(self):
        """Execute the complete pipeline"""
        logger.info("=" * 60)
        logger.info(f"STARTING PIPELINE: {Config.PIPELINE_NAME}")
        logger.info(f"Environment: {Config.ENVIRONMENT}")
        # FIXED: Show all three buckets instead of single S3_BUCKET_NAME
        logger.info(f"S3 Bronze Bucket: {Config.S3_BRONZE_BUCKET}")
        logger.info(f"S3 Silver Bucket: {Config.S3_SILVER_BUCKET}")
        logger.info(f"S3 Gold Bucket: {Config.S3_GOLD_BUCKET}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Extract
            raw_data = self.extract()
            
            # Step 2: Transform
            cleaned_data = self.transform(raw_data)
            
            # Step 3: Aggregate
            gold_files, summary_stats = self.aggregate(cleaned_data)
            
            # Mark pipeline as successful
            end_time = datetime.utcnow()
            start_time = datetime.fromisoformat(self.pipeline_metadata['start_time'])
            duration = (end_time - start_time).total_seconds()
            
            self.pipeline_metadata['status'] = 'success'
            self.pipeline_metadata['end_time'] = end_time.isoformat()
            self.pipeline_metadata['duration_seconds'] = duration
            
            # Save metadata
            metadata_key = self.loader.save_pipeline_metadata(self.pipeline_metadata)
            
            # Print summary
            logger.info("=" * 60)
            logger.info("PIPELINE EXECUTION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Status: SUCCESS ✓")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Total Users: {summary_stats['total_users']}")
            logger.info(f"Total Posts: {summary_stats['total_posts']}")
            logger.info(f"Total Comments: {summary_stats['total_comments']}")
            logger.info(f"Reports Generated: {len(gold_files)}")
            # FIXED: Show metadata location without using S3_BUCKET_NAME
            logger.info(f"Metadata saved to S3")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            # Mark pipeline as failed
            self.pipeline_metadata['status'] = 'failed'
            self.pipeline_metadata['end_time'] = datetime.utcnow().isoformat()
            self.pipeline_metadata['error'] = str(e)
            
            # Save metadata even on failure
            self.loader.save_pipeline_metadata(self.pipeline_metadata)
            
            logger.error("=" * 60)
            logger.error(f"PIPELINE FAILED: {e}")
            logger.error("=" * 60)
            
            return False


def main():
    """Main entry point"""
    try:
        # Validate configuration
        Config.validate()
        
        # Create necessary directories
        Config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        Config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # Run pipeline
        orchestrator = PipelineOrchestrator()
        success = orchestrator.run()
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
