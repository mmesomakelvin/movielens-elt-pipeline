"""
Configuration settings for the MovieLens ELT Pipeline.
"""

import os

# Get the project root directory (absolute path)
PROJECT_ROOT = '/home/mmesoma/movielens_elt_pipeline'

# Database Configuration
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "movielens_db"
DB_USER = "postgres"
DB_PASSWORD = "Admin"

# Database URL for SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# File Paths (absolute paths)
DATA_RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "output")
LOGS_PATH = os.path.join(PROJECT_ROOT, "logs")

# MovieLens Dataset URL
MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"