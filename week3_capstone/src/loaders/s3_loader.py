"""
S3 Data Loader
Loads data to S3 in medallion architecture (Bronze, Silver, Gold)
"""

import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from ..utils.s3_helper import S3Helper
from ..utils.logger import get_logger
from ..utils.config import Config

logger = get_logger(__name__)

class S3Loader:
    """Load data to S3 data lake"""
    
    def __init__(self):
        # Create separate S3 helpers for each layer
        self.bronze_s3 = S3Helper(Config.S3_BRONZE_BUCKET, Config.AWS_REGION)
        self.silver_s3 = S3Helper(Config.S3_SILVER_BUCKET, Config.AWS_REGION)
        self.gold_s3 = S3Helper(Config.S3_GOLD_BUCKET, Config.AWS_REGION)
        self.date_partition = datetime.utcnow().strftime('%Y/%m/%d')
    
    def load_to_bronze(self, data: Dict[str, List[Dict]], source: str) -> Dict[str, str]:
        """Load raw data to Bronze layer"""
        logger.info(f"Loading {source} data to Bronze layer...")
        
        uploaded_files = {}
        
        for data_type, records in data.items():
            if not records:
                logger.warning(f"No records to upload for {data_type}")
                continue
            
            s3_key = f"{Config.S3_BRONZE_PREFIX}/{source}/{self.date_partition}/{data_type}.json"
            
            metadata = {
                'source': source,
                'data_type': data_type,
                'record_count': str(len(records)),
                'loaded_at': datetime.utcnow().isoformat()
            }
            
            if self.bronze_s3.upload_json(records, s3_key, metadata):
                uploaded_files[data_type] = s3_key
                logger.info(f"Uploaded {len(records)} {data_type} records to Bronze")
        
        return uploaded_files
    
    def load_to_silver(self, df: pd.DataFrame, table_name: str) -> str:
        """Load cleaned data to Silver layer (Parquet format)"""
        logger.info(f"Loading {table_name} to Silver layer...")
        
        # Save to local parquet first
        local_path = Config.DATA_DIR / 'local' / f"{table_name}.parquet"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(local_path, index=False, engine='pyarrow')
        
        # Upload to S3
        s3_key = f"{Config.S3_SILVER_PREFIX}/{table_name}/{self.date_partition}/{table_name}.parquet"
        
        metadata = {
            'table_name': table_name,
            'record_count': str(len(df)),
            'columns': ','.join(df.columns),
            'loaded_at': datetime.utcnow().isoformat()
        }
        
        if self.silver_s3.upload_file(local_path, s3_key, metadata):
            logger.info(f"Uploaded {len(df)} records to Silver: {table_name}")
            # Clean up local file
            local_path.unlink()
            return s3_key
        
        return ''
    
    def load_to_gold(self, df: pd.DataFrame, report_name: str, format: str = 'csv') -> str:
        """Load aggregated data to Gold layer"""
        logger.info(f"Loading {report_name} report to Gold layer...")
        
        # Save to local file
        local_path = Config.DATA_DIR / 'local' / f"{report_name}.{format}"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == 'csv':
            df.to_csv(local_path, index=False)
        elif format == 'parquet':
            df.to_parquet(local_path, index=False, engine='pyarrow')
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        # Upload to S3
        s3_key = f"{Config.S3_GOLD_PREFIX}/reports/{self.date_partition}/{report_name}.{format}"
        
        metadata = {
            'report_name': report_name,
            'record_count': str(len(df)),
            'format': format,
            'loaded_at': datetime.utcnow().isoformat()
        }
        
        if self.gold_s3.upload_file(local_path, s3_key, metadata):
            logger.info(f"Uploaded {report_name} report to Gold")
            # Clean up local file
            local_path.unlink()
            return s3_key
        
        return ''
    
    def save_pipeline_metadata(self, metadata: Dict) -> str:
        """Save pipeline execution metadata"""
        logger.info("Saving pipeline metadata...")
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{Config.S3_LOGS_PREFIX}/metadata/pipeline_run_{timestamp}.json"
        
        # Save metadata to Bronze bucket (or whichever bucket you prefer for logs)
        if self.bronze_s3.upload_json(metadata, s3_key):
            logger.info("Pipeline metadata saved")
            return s3_key
        
        return ''
