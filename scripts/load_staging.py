"""
Load CSV files to staging tables in PostgreSQL.
Task 2: Extract and load movies.csv and ratings.csv to staging tables
"""

import os
import sys
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DATABASE_URL, DATA_RAW_PATH, LOGS_PATH

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOGS_PATH}/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_engine_connection():
    """Create database engine connection."""
    try:
        engine = create_engine(DATABASE_URL)
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise


def load_csv_to_staging(engine, csv_path, table_name, chunksize=100000):
    """
    Load a CSV file to a staging table.
    
    Args:
        engine: SQLAlchemy engine
        csv_path: Path to the CSV file
        table_name: Name of the staging table
        chunksize: Number of rows to load at a time
    
    Returns:
        Number of rows loaded
    """
    try:
        logger.info(f"Loading {csv_path} to {table_name}")
        
        # Check if file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"File not found: {csv_path}")
        
        # Get total rows for progress tracking
        total_rows = sum(1 for _ in open(csv_path)) - 1  # Subtract header
        logger.info(f"Total rows to load: {total_rows:,}")
        
        # Load in chunks
        rows_loaded = 0
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
            # First chunk replaces table, subsequent chunks append
            if_exists = 'replace' if i == 0 else 'append'
            with engine.connect() as connection:
                chunk.to_sql(table_name, connection, if_exists=if_exists, index=False)
            
            rows_loaded += len(chunk)
            progress = (rows_loaded / total_rows) * 100
            logger.info(f"Progress: {progress:.1f}% ({rows_loaded:,} / {total_rows:,} rows)")
        
        logger.info(f"Successfully loaded {rows_loaded:,} rows to {table_name}")
        return rows_loaded
        
    except Exception as e:
        logger.error(f"Failed to load {csv_path}: {e}")
        raise


def verify_staging_tables(engine):
    """Verify that staging tables were created and have data."""
    try:
        with engine.connect() as conn:
            # Check movies staging
            result = conn.execute(text("SELECT COUNT(*) FROM staging_movies"))
            movies_count = result.scalar()
            logger.info(f"staging_movies row count: {movies_count:,}")
            
            # Check ratings staging
            result = conn.execute(text("SELECT COUNT(*) FROM staging_ratings"))
            ratings_count = result.scalar()
            logger.info(f"staging_ratings row count: {ratings_count:,}")
            
            return movies_count, ratings_count
            
    except Exception as e:
        logger.error(f"Failed to verify staging tables: {e}")
        raise


def main():
    """Main function to load data to staging tables."""
    logger.info("=" * 50)
    logger.info("TASK 2: Load Data to Staging Tables")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    # Create database connection
    engine = create_engine_connection()
    
    # Define file paths
    movies_path = os.path.join(DATA_RAW_PATH, "ml-32m", "movies.csv")
    ratings_path = os.path.join(DATA_RAW_PATH, "ml-32m", "ratings.csv")
    
    # Load movies to staging
    logger.info("-" * 30)
    logger.info("Loading movies...")
    movies_rows = load_csv_to_staging(engine, movies_path, "staging_movies")
    
    # Load ratings to staging (this is a large file)
    logger.info("-" * 30)
    logger.info("Loading ratings (this may take a few minutes)...")
    ratings_rows = load_csv_to_staging(engine, ratings_path, "staging_ratings")
    
    # Verify the data was loaded
    logger.info("-" * 30)
    logger.info("Verifying staging tables...")
    verify_staging_tables(engine)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info("=" * 50)
    logger.info(f"Task 2 completed in {duration:.2f} seconds")
    logger.info(f"Total rows loaded: {movies_rows + ratings_rows:,}")
    
    return movies_rows, ratings_rows


if __name__ == "__main__":
    main()