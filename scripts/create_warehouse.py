"""
Create dimension and fact tables for the data warehouse.
Task 5: Create dimensions and facts tables from the staging tables
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


def create_dim_movies(engine):
    """Create dimension table for movies."""
    try:
        logger.info("Creating dim_movies table...")
        
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS dim_movies CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE dim_movies (
                    movie_key SERIAL PRIMARY KEY,
                    movie_id INTEGER UNIQUE NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    clean_title VARCHAR(500),
                    release_year INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                INSERT INTO dim_movies (movie_id, title, clean_title, release_year)
                SELECT 
                    "movieId",
                    title,
                    clean_title,
                    release_year
                FROM cleaned_movies
            """))
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM dim_movies"))
            count = result.scalar()
            logger.info(f"Created dim_movies with {count:,} rows")
            return count
            
    except Exception as e:
        logger.error(f"Failed to create dim_movies: {e}")
        raise


def create_dim_genres(engine):
    """Create dimension table for genres."""
    try:
        logger.info("Creating dim_genres table...")
        
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS dim_genres CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE dim_genres (
                    genre_key SERIAL PRIMARY KEY,
                    genre_name VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                INSERT INTO dim_genres (genre_name)
                SELECT DISTINCT TRIM(unnest(string_to_array(genres, '|'))) as genre_name
                FROM cleaned_movies
                WHERE genres IS NOT NULL AND genres != ''
                ORDER BY genre_name
            """))
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM dim_genres"))
            count = result.scalar()
            logger.info(f"Created dim_genres with {count:,} rows")
            
            result = conn.execute(text("SELECT genre_name FROM dim_genres ORDER BY genre_name"))
            genres = [row[0] for row in result]
            logger.info(f"Genres found: {', '.join(genres)}")
            return count
            
    except Exception as e:
        logger.error(f"Failed to create dim_genres: {e}")
        raise


def create_dim_users(engine):
    """Create dimension table for users."""
    try:
        logger.info("Creating dim_users table...")
        
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS dim_users CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE dim_users (
                    user_key SERIAL PRIMARY KEY,
                    user_id INTEGER UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                INSERT INTO dim_users (user_id)
                SELECT DISTINCT "userId"
                FROM cleaned_ratings
                ORDER BY "userId"
            """))
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM dim_users"))
            count = result.scalar()
            logger.info(f"Created dim_users with {count:,} rows")
            return count
            
    except Exception as e:
        logger.error(f"Failed to create dim_users: {e}")
        raise


def create_bridge_movie_genres(engine):
    """Create bridge table linking movies to genres."""
    try:
        logger.info("Creating bridge_movie_genres table...")
        
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS bridge_movie_genres CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE bridge_movie_genres (
                    movie_id INTEGER NOT NULL,
                    genre_key INTEGER NOT NULL,
                    PRIMARY KEY (movie_id, genre_key)
                )
            """))
            
            conn.execute(text("""
                INSERT INTO bridge_movie_genres (movie_id, genre_key)
                SELECT DISTINCT
                    m."movieId" as movie_id,
                    g.genre_key
                FROM cleaned_movies m
                CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) as gn
                JOIN dim_genres g ON TRIM(gn) = g.genre_name
            """))
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM bridge_movie_genres"))
            count = result.scalar()
            logger.info(f"Created bridge_movie_genres with {count:,} rows")
            return count
            
    except Exception as e:
        logger.error(f"Failed to create bridge_movie_genres: {e}")
        raise


def create_fact_ratings(engine):
    """Create fact table for ratings."""
    try:
        logger.info("Creating fact_ratings table...")
        
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_ratings CASCADE"))
            
            conn.execute(text("""
                CREATE TABLE fact_ratings (
                    rating_key SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    movie_id INTEGER NOT NULL,
                    rating DECIMAL(2,1) NOT NULL,
                    rating_timestamp BIGINT,
                    rating_datetime TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                INSERT INTO fact_ratings (user_id, movie_id, rating, rating_timestamp, rating_datetime)
                SELECT 
                    "userId",
                    "movieId",
                    rating,
                    rating_timestamp,
                    rating_datetime
                FROM cleaned_ratings
            """))
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fact_ratings"))
            count = result.scalar()
            logger.info(f"Created fact_ratings with {count:,} rows")
            return count
            
    except Exception as e:
        logger.error(f"Failed to create fact_ratings: {e}")
        raise


def create_indexes(engine):
    """Create indexes for better query performance."""
    try:
        logger.info("Creating indexes...")
        
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_ratings_user ON fact_ratings(user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_ratings_movie ON fact_ratings(movie_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_ratings_datetime ON fact_ratings(rating_datetime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_bridge_movie ON bridge_movie_genres(movie_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_bridge_genre ON bridge_movie_genres(genre_key)"))
        
        logger.info("Indexes created successfully")
            
    except Exception as e:
        logger.error(f"Failed to create indexes: {e}")
        raise


def verify_warehouse(engine):
    """Verify the data warehouse structure."""
    try:
        logger.info("-" * 30)
        logger.info("DATA WAREHOUSE SUMMARY")
        logger.info("-" * 30)
        
        with engine.connect() as conn:
            tables = ['dim_movies', 'dim_genres', 'dim_users', 'bridge_movie_genres', 'fact_ratings']
            
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                logger.info(f"{table}: {count:,} rows")
                
    except Exception as e:
        logger.error(f"Failed to verify warehouse: {e}")
        raise


def main():
    """Main function to create data warehouse."""
    logger.info("=" * 50)
    logger.info("TASK 5: Create Dimension and Fact Tables")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    engine = create_engine_connection()
    
    logger.info("-" * 30)
    logger.info("Creating Dimension Tables...")
    create_dim_movies(engine)
    create_dim_genres(engine)
    create_dim_users(engine)
    
    logger.info("-" * 30)
    logger.info("Creating Bridge Table...")
    create_bridge_movie_genres(engine)
    
    logger.info("-" * 30)
    logger.info("Creating Fact Table...")
    create_fact_ratings(engine)
    
    logger.info("-" * 30)
    create_indexes(engine)
    
    verify_warehouse(engine)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 50)
    logger.info(f"Task 5 completed in {duration:.2f} seconds")
    logger.info("Data warehouse created successfully!")


if __name__ == "__main__":
    main()