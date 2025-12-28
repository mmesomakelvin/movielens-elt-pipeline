"""
Clean and transform staging tables.
Task 3: Clean/transform staging tables
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DATABASE_URL, LOGS_PATH

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


def clean_movies_table(engine):
    """
    Clean the staging_movies table:
    - Remove duplicates
    - Handle NULL values
    - Extract year from title
    - Trim whitespace
    """
    try:
        logger.info("Cleaning staging_movies table...")
        
        with engine.connect() as conn:
            # Check for NULL values before cleaning
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN "movieId" IS NULL THEN 1 ELSE 0 END) as null_movieid,
                    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) as null_title,
                    SUM(CASE WHEN genres IS NULL THEN 1 ELSE 0 END) as null_genres
                FROM staging_movies
            """))
            row = result.fetchone()
            logger.info(f"Movies - Total rows: {row[0]:,}, NULL movieId: {row[1]}, NULL title: {row[2]}, NULL genres: {row[3]}")
            
            # Check for duplicates
            result = conn.execute(text("""
                SELECT COUNT(*) - COUNT(DISTINCT "movieId") as duplicate_count
                FROM staging_movies
            """))
            duplicates = result.scalar()
            logger.info(f"Movies - Duplicate movieIds: {duplicates}")
            
            # Create cleaned movies table
            conn.execute(text("""
                DROP TABLE IF EXISTS cleaned_movies
            """))
            
            conn.execute(text("""
                CREATE TABLE cleaned_movies AS
                SELECT DISTINCT
                    "movieId",
                    TRIM(title) as title,
                    -- Extract year from title (format: "Movie Name (YYYY)")
                    CASE 
                        WHEN title ~ '\\(\\d{4}\\)$' 
                        THEN CAST(SUBSTRING(title FROM '\\(([0-9]{4})\\)$') AS INTEGER)
                        ELSE NULL 
                    END as release_year,
                    -- Clean title without year
                    CASE 
                        WHEN title ~ '\\(\\d{4}\\)$' 
                        THEN TRIM(REGEXP_REPLACE(title, '\\s*\\([0-9]{4}\\)$', ''))
                        ELSE TRIM(title)
                    END as clean_title,
                    COALESCE(TRIM(genres), 'Unknown') as genres
                FROM staging_movies
                WHERE "movieId" IS NOT NULL
            """))
            
            conn.commit()
            
            # Verify cleaned table
            result = conn.execute(text("SELECT COUNT(*) FROM cleaned_movies"))
            count = result.scalar()
            logger.info(f"Created cleaned_movies table with {count:,} rows")
            
            return count
            
    except Exception as e:
        logger.error(f"Failed to clean movies table: {e}")
        raise


def clean_ratings_table(engine):
    """
    Clean the staging_ratings table:
    - Remove duplicates
    - Handle NULL values
    - Convert timestamp to datetime
    - Validate rating range (0.5 to 5.0)
    """
    try:
        logger.info("Cleaning staging_ratings table...")
        
        with engine.connect() as conn:
            # Check for NULL values and invalid ratings
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN "userId" IS NULL THEN 1 ELSE 0 END) as null_userid,
                    SUM(CASE WHEN "movieId" IS NULL THEN 1 ELSE 0 END) as null_movieid,
                    SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) as null_rating,
                    SUM(CASE WHEN rating < 0.5 OR rating > 5.0 THEN 1 ELSE 0 END) as invalid_rating
                FROM staging_ratings
            """))
            row = result.fetchone()
            logger.info(f"Ratings - Total: {row[0]:,}, NULL userId: {row[1]}, NULL movieId: {row[2]}, NULL rating: {row[3]}, Invalid rating: {row[4]}")
            
            # Check for duplicate ratings (same user rating same movie multiple times)
            result = conn.execute(text("""
                SELECT COUNT(*) as dup_count FROM (
                    SELECT "userId", "movieId", COUNT(*) 
                    FROM staging_ratings 
                    GROUP BY "userId", "movieId" 
                    HAVING COUNT(*) > 1
                ) subq
            """))
            duplicates = result.scalar()
            logger.info(f"Ratings - User-Movie duplicate pairs: {duplicates}")
            
            # Create cleaned ratings table
            conn.execute(text("""
                DROP TABLE IF EXISTS cleaned_ratings
            """))
            
            # For duplicates, keep the most recent rating (highest timestamp)
            conn.execute(text("""
                CREATE TABLE cleaned_ratings AS
                SELECT DISTINCT ON ("userId", "movieId")
                    "userId",
                    "movieId",
                    rating,
                    timestamp as rating_timestamp,
                    -- Convert Unix timestamp to datetime
                    TO_TIMESTAMP(timestamp) as rating_datetime
                FROM staging_ratings
                WHERE "userId" IS NOT NULL 
                    AND "movieId" IS NOT NULL
                    AND rating IS NOT NULL
                    AND rating >= 0.5 
                    AND rating <= 5.0
                ORDER BY "userId", "movieId", timestamp DESC
            """))
            
            conn.commit()
            
            # Verify cleaned table
            result = conn.execute(text("SELECT COUNT(*) FROM cleaned_ratings"))
            count = result.scalar()
            logger.info(f"Created cleaned_ratings table with {count:,} rows")
            
            return count
            
    except Exception as e:
        logger.error(f"Failed to clean ratings table: {e}")
        raise


def show_sample_data(engine):
    """Show sample data from cleaned tables."""
    try:
        with engine.connect() as conn:
            # Sample movies
            logger.info("Sample cleaned movies:")
            result = conn.execute(text("""
                SELECT "movieId", clean_title, release_year, genres 
                FROM cleaned_movies 
                LIMIT 5
            """))
            for row in result:
                logger.info(f"  Movie {row[0]}: {row[1]} ({row[2]}) - {row[3]}")
            
            # Sample ratings
            logger.info("Sample cleaned ratings:")
            result = conn.execute(text("""
                SELECT "userId", "movieId", rating, rating_datetime 
                FROM cleaned_ratings 
                LIMIT 5
            """))
            for row in result:
                logger.info(f"  User {row[0]} rated Movie {row[1]}: {row[2]} stars on {row[3]}")
                
    except Exception as e:
        logger.error(f"Failed to show sample data: {e}")
        raise


def main():
    """Main function to clean and transform data."""
    logger.info("=" * 50)
    logger.info("TASK 3: Clean/Transform Staging Tables")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    # Create database connection
    engine = create_engine_connection()
    
    # Clean movies table
    logger.info("-" * 30)
    movies_count = clean_movies_table(engine)
    
    # Clean ratings table
    logger.info("-" * 30)
    ratings_count = clean_ratings_table(engine)
    
    # Show sample data
    logger.info("-" * 30)
    show_sample_data(engine)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 50)
    logger.info(f"Task 3 completed in {duration:.2f} seconds")
    logger.info(f"Cleaned movies: {movies_count:,} rows")
    logger.info(f"Cleaned ratings: {ratings_count:,} rows")
    
    return movies_count, ratings_count


if __name__ == "__main__":
    main()