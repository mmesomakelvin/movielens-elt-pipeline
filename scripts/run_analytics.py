"""
Run SQL analytics and save results as CSV files.
Task 6: Run SQL analytics and save results as separate csv files
"""

import os
import sys
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DATABASE_URL, DATA_OUTPUT_PATH, LOGS_PATH

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


def run_query_to_csv(engine, query, output_filename, description):
    """
    Run a SQL query and save results to CSV.
    
    Args:
        engine: SQLAlchemy engine
        query: SQL query to execute
        output_filename: Name of output CSV file
        description: Description of the analysis
    
    Returns:
        DataFrame with results
    """
    try:
        logger.info(f"Running analysis: {description}")
        
        # Execute query and load into DataFrame
        df = pd.read_sql(query, engine)
        
        # Create output directory if it doesn't exist
        os.makedirs(DATA_OUTPUT_PATH, exist_ok=True)
        
        # Save to CSV
        output_path = os.path.join(DATA_OUTPUT_PATH, output_filename)
        df.to_csv(output_path, index=False)
        
        logger.info(f"Saved results to {output_path}")
        logger.info(f"Results preview:")
        logger.info(f"\n{df.to_string()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to run analysis '{description}': {e}")
        raise


def top_10_movies_by_avg_rating(engine):
    """
    Task 6a: Top 10 movies by average rating
    Only include movies with at least 100 ratings for statistical significance.
    """
    query = """
        SELECT 
            m.movie_id,
            m.title,
            m.release_year,
            ROUND(AVG(f.rating)::numeric, 2) as avg_rating,
            COUNT(f.rating) as num_ratings
        FROM fact_ratings f
        JOIN dim_movies m ON f.movie_id = m.movie_id
        GROUP BY m.movie_id, m.title, m.release_year
        HAVING COUNT(f.rating) >= 100
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT 10
    """
    
    return run_query_to_csv(
        engine, 
        query, 
        "top_10_movies_by_avg_rating.csv",
        "Top 10 movies by average rating (min 100 ratings)"
    )


def least_10_movies_by_avg_rating(engine):
    """
    Task 6b: Least 10 movies by average rating
    Only include movies with at least 100 ratings for statistical significance.
    """
    query = """
        SELECT 
            m.movie_id,
            m.title,
            m.release_year,
            ROUND(AVG(f.rating)::numeric, 2) as avg_rating,
            COUNT(f.rating) as num_ratings
        FROM fact_ratings f
        JOIN dim_movies m ON f.movie_id = m.movie_id
        GROUP BY m.movie_id, m.title, m.release_year
        HAVING COUNT(f.rating) >= 100
        ORDER BY avg_rating ASC, num_ratings DESC
        LIMIT 10
    """
    
    return run_query_to_csv(
        engine, 
        query, 
        "least_10_movies_by_avg_rating.csv",
        "Least 10 movies by average rating (min 100 ratings)"
    )


def top_5_genres_by_num_ratings(engine):
    """
    Task 6c: Top 5 genres by number of ratings
    """
    query = """
        SELECT 
            g.genre_name,
            COUNT(f.rating) as num_ratings,
            ROUND(AVG(f.rating)::numeric, 2) as avg_rating
        FROM fact_ratings f
        JOIN bridge_movie_genres bmg ON f.movie_id = bmg.movie_id
        JOIN dim_genres g ON bmg.genre_key = g.genre_key
        GROUP BY g.genre_name
        ORDER BY num_ratings DESC
        LIMIT 5
    """
    
    return run_query_to_csv(
        engine, 
        query, 
        "top_5_genres_by_num_ratings.csv",
        "Top 5 genres by number of ratings"
    )


def least_5_genres_by_num_ratings(engine):
    """
    Task 6d: Least 5 genres by number of ratings
    """
    query = """
        SELECT 
            g.genre_name,
            COUNT(f.rating) as num_ratings,
            ROUND(AVG(f.rating)::numeric, 2) as avg_rating
        FROM fact_ratings f
        JOIN bridge_movie_genres bmg ON f.movie_id = bmg.movie_id
        JOIN dim_genres g ON bmg.genre_key = g.genre_key
        GROUP BY g.genre_name
        ORDER BY num_ratings ASC
        LIMIT 5
    """
    
    return run_query_to_csv(
        engine, 
        query, 
        "least_5_genres_by_num_ratings.csv",
        "Least 5 genres by number of ratings"
    )


def main():
    """Main function to run all analytics."""
    logger.info("=" * 50)
    logger.info("TASK 6: Run Analytics and Export to CSV")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    # Create database connection
    engine = create_engine_connection()
    
    # Run all analytics
    logger.info("-" * 30)
    logger.info("Analysis 6a: Top 10 Movies by Average Rating")
    logger.info("-" * 30)
    top_10_movies_by_avg_rating(engine)
    
    logger.info("-" * 30)
    logger.info("Analysis 6b: Least 10 Movies by Average Rating")
    logger.info("-" * 30)
    least_10_movies_by_avg_rating(engine)
    
    logger.info("-" * 30)
    logger.info("Analysis 6c: Top 5 Genres by Number of Ratings")
    logger.info("-" * 30)
    top_5_genres_by_num_ratings(engine)
    
    logger.info("-" * 30)
    logger.info("Analysis 6d: Least 5 Genres by Number of Ratings")
    logger.info("-" * 30)
    least_5_genres_by_num_ratings(engine)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 50)
    logger.info(f"Task 6 completed in {duration:.2f} seconds")
    logger.info(f"CSV files saved to: {DATA_OUTPUT_PATH}/")
    logger.info("Files created:")
    logger.info("  - top_10_movies_by_avg_rating.csv")
    logger.info("  - least_10_movies_by_avg_rating.csv")
    logger.info("  - top_5_genres_by_num_ratings.csv")
    logger.info("  - least_5_genres_by_num_ratings.csv")


if __name__ == "__main__":
    main()