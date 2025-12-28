"""
Data quality and validation checks.
Task 4: Run data quality and validation checks on the data
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


class DataQualityChecker:
    """Class to run data quality checks."""
    
    def __init__(self, engine):
        self.engine = engine
        self.checks_passed = 0
        self.checks_failed = 0
        self.results = []
    
    def run_check(self, check_name, query, expected_condition, description):
        """
        Run a single data quality check.
        
        Args:
            check_name: Name of the check
            query: SQL query to run
            expected_condition: Function that takes query result and returns True if passed
            description: Description of what we're checking
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                value = result.scalar()
                
                passed = expected_condition(value)
                status = "✓ PASSED" if passed else "✗ FAILED"
                
                if passed:
                    self.checks_passed += 1
                else:
                    self.checks_failed += 1
                
                self.results.append({
                    'check': check_name,
                    'status': 'PASSED' if passed else 'FAILED',
                    'value': value,
                    'description': description
                })
                
                logger.info(f"{status}: {check_name}")
                logger.info(f"         {description}")
                logger.info(f"         Result: {value}")
                
                return passed
                
        except Exception as e:
            logger.error(f"✗ ERROR: {check_name} - {e}")
            self.checks_failed += 1
            return False
    
    def get_summary(self):
        """Get summary of all checks."""
        return {
            'total': self.checks_passed + self.checks_failed,
            'passed': self.checks_passed,
            'failed': self.checks_failed,
            'results': self.results
        }


def run_movies_quality_checks(checker):
    """Run quality checks on cleaned_movies table."""
    logger.info("-" * 30)
    logger.info("MOVIES TABLE QUALITY CHECKS")
    logger.info("-" * 30)
    
    # Check 1: Table exists and has data
    checker.run_check(
        "Movies - Table Not Empty",
        "SELECT COUNT(*) FROM cleaned_movies",
        lambda x: x > 0,
        "Table should have at least 1 row"
    )
    
    # Check 2: No NULL movieIds
    checker.run_check(
        "Movies - No NULL movieId",
        "SELECT COUNT(*) FROM cleaned_movies WHERE \"movieId\" IS NULL",
        lambda x: x == 0,
        "movieId should never be NULL"
    )
    
    # Check 3: No duplicate movieIds
    checker.run_check(
        "Movies - No Duplicate movieId",
        "SELECT COUNT(*) - COUNT(DISTINCT \"movieId\") FROM cleaned_movies",
        lambda x: x == 0,
        "Each movieId should be unique"
    )
    
    # Check 4: All movies have titles
    checker.run_check(
        "Movies - No NULL Titles",
        "SELECT COUNT(*) FROM cleaned_movies WHERE title IS NULL OR title = ''",
        lambda x: x == 0,
        "All movies should have titles"
    )
    
    # Check 5: All movies have genres
    checker.run_check(
        "Movies - No NULL Genres",
        "SELECT COUNT(*) FROM cleaned_movies WHERE genres IS NULL OR genres = ''",
        lambda x: x == 0,
        "All movies should have genres"
    )
    
    # Check 6: Release years are reasonable
    checker.run_check(
        "Movies - Valid Release Years",
        "SELECT COUNT(*) FROM cleaned_movies WHERE release_year IS NOT NULL AND (release_year < 1800 OR release_year > 2030)",
        lambda x: x == 0,
        "Release years should be between 1800 and 2030"
    )
    
    # Check 7: Expected row count (approximately)
    checker.run_check(
        "Movies - Expected Row Count",
        "SELECT COUNT(*) FROM cleaned_movies",
        lambda x: x > 80000 and x < 100000,
        "Should have approximately 87,000 movies"
    )


def run_ratings_quality_checks(checker):
    """Run quality checks on cleaned_ratings table."""
    logger.info("-" * 30)
    logger.info("RATINGS TABLE QUALITY CHECKS")
    logger.info("-" * 30)
    
    # Check 1: Table exists and has data
    checker.run_check(
        "Ratings - Table Not Empty",
        "SELECT COUNT(*) FROM cleaned_ratings",
        lambda x: x > 0,
        "Table should have at least 1 row"
    )
    
    # Check 2: No NULL userIds
    checker.run_check(
        "Ratings - No NULL userId",
        "SELECT COUNT(*) FROM cleaned_ratings WHERE \"userId\" IS NULL",
        lambda x: x == 0,
        "userId should never be NULL"
    )
    
    # Check 3: No NULL movieIds
    checker.run_check(
        "Ratings - No NULL movieId",
        "SELECT COUNT(*) FROM cleaned_ratings WHERE \"movieId\" IS NULL",
        lambda x: x == 0,
        "movieId should never be NULL"
    )
    
    # Check 4: No NULL ratings
    checker.run_check(
        "Ratings - No NULL Ratings",
        "SELECT COUNT(*) FROM cleaned_ratings WHERE rating IS NULL",
        lambda x: x == 0,
        "rating should never be NULL"
    )
    
    # Check 5: Ratings in valid range (0.5 to 5.0)
    checker.run_check(
        "Ratings - Valid Rating Range",
        "SELECT COUNT(*) FROM cleaned_ratings WHERE rating < 0.5 OR rating > 5.0",
        lambda x: x == 0,
        "Ratings should be between 0.5 and 5.0"
    )
    
    # Check 6: Valid rating increments (0.5 steps)
    checker.run_check(
        "Ratings - Valid Rating Increments",
        "SELECT COUNT(*) FROM cleaned_ratings WHERE rating * 2 != FLOOR(rating * 2)",
        lambda x: x == 0,
        "Ratings should be in 0.5 increments"
    )
    
    # Check 7: Expected row count (approximately)
    checker.run_check(
        "Ratings - Expected Row Count",
        "SELECT COUNT(*) FROM cleaned_ratings",
        lambda x: x > 30000000 and x < 35000000,
        "Should have approximately 32 million ratings"
    )
    
    # Check 8: All rated movies exist in movies table
    checker.run_check(
        "Ratings - Referential Integrity",
        """
        SELECT COUNT(*) FROM cleaned_ratings r 
        LEFT JOIN cleaned_movies m ON r."movieId" = m."movieId" 
        WHERE m."movieId" IS NULL
        """,
        lambda x: x == 0,
        "All rated movies should exist in movies table"
    )


def run_cross_table_checks(checker):
    """Run quality checks across tables."""
    logger.info("-" * 30)
    logger.info("CROSS-TABLE QUALITY CHECKS")
    logger.info("-" * 30)
    
    # Check 1: Movies with ratings
    checker.run_check(
        "Cross - Movies With Ratings",
        """
        SELECT COUNT(DISTINCT m."movieId") 
        FROM cleaned_movies m 
        INNER JOIN cleaned_ratings r ON m."movieId" = r."movieId"
        """,
        lambda x: x > 0,
        "At least some movies should have ratings"
    )
    
    # Check 2: Average rating is reasonable
    checker.run_check(
        "Cross - Reasonable Average Rating",
        "SELECT AVG(rating) FROM cleaned_ratings",
        lambda x: x > 2.0 and x < 4.5,
        "Average rating should be between 2.0 and 4.5"
    )


def main():
    """Main function to run data quality checks."""
    logger.info("=" * 50)
    logger.info("TASK 4: Data Quality and Validation Checks")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    # Create database connection
    engine = create_engine_connection()
    
    # Create checker
    checker = DataQualityChecker(engine)
    
    # Run all checks
    run_movies_quality_checks(checker)
    run_ratings_quality_checks(checker)
    run_cross_table_checks(checker)
    
    # Get summary
    summary = checker.get_summary()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 50)
    logger.info("DATA QUALITY SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total Checks: {summary['total']}")
    logger.info(f"Passed: {summary['passed']}")
    logger.info(f"Failed: {summary['failed']}")
    logger.info(f"Success Rate: {(summary['passed']/summary['total'])*100:.1f}%")
    logger.info(f"Task 4 completed in {duration:.2f} seconds")
    
    if summary['failed'] > 0:
        logger.warning("Some data quality checks failed! Review the results above.")
        return False
    else:
        logger.info("All data quality checks passed!")
        return True


if __name__ == "__main__":
    main()