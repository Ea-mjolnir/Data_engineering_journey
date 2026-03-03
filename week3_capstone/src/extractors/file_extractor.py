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
