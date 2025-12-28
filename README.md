# MovieLens ELT Data Pipeline

A complete end-to-end ELT (Extract, Load, Transform) data pipeline built with Python, PostgreSQL, and Apache Airflow. This project processes the MovieLens 32M dataset containing 32 million movie ratings.

## 📋 Project Overview

This pipeline demonstrates core data engineering skills:
- **Extract**: Download and unzip the MovieLens 32M dataset
- **Load**: Load raw CSV data into PostgreSQL staging tables
- **Transform**: Clean data and build a star schema data warehouse
- **Analyze**: Run SQL analytics and export results to CSV
- **Orchestrate**: Schedule daily pipeline runs with Apache Airflow

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Source   │     │   PostgreSQL    │     │    Outputs      │
│   (GroupLens)   │     │    Database     │     │                 │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        ELT PIPELINE                             │
│                                                                 │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐     │
│  │ Extract  │──▶│   Load   │──▶│Transform │──▶│ Analyze  │     │
│  │          │   │ Staging  │   │  & DQ    │   │          │     │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘     │
│                                                                 │
│  download_    load_         transform_     create_      run_    │
│  data.py      staging.py    data.py        warehouse.py analytics│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Apache Airflow │
                    │  (Scheduler)    │
                    │  Daily @ 12 PM  │
                    └─────────────────┘
```

## 📊 Data Model (Star Schema)

```
                    ┌─────────────────┐
                    │   dim_movies    │
                    │─────────────────│
                    │ movie_key (PK)  │
                    │ movie_id        │
                    │ title           │
                    │ clean_title     │
                    │ release_year    │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│   dim_users     │          │          │   dim_genres    │
│─────────────────│          │          │─────────────────│
│ user_key (PK)   │          │          │ genre_key (PK)  │
│ user_id         │          │          │ genre_name      │
└────────┬────────┘          │          └────────┬────────┘
         │                   │                   │
         │          ┌────────┴────────┐          │
         │          │  fact_ratings   │          │
         └─────────▶│─────────────────│◀─────────┘
                    │ rating_key (PK) │
                    │ user_id (FK)    │
                    │ movie_id (FK)   │
                    │ rating          │
                    │ rating_timestamp│
                    │ rating_datetime │
                    └─────────────────┘
                             │
                    ┌────────┴────────┐
                    │bridge_movie_    │
                    │    genres       │
                    │─────────────────│
                    │ movie_id (FK)   │
                    │ genre_key (FK)  │
                    └─────────────────┘
```

## 📁 Project Structure

```
movielens_elt_pipeline/
├── config/
│   └── config.py              # Database and path configurations
├── dags/
│   └── movielens_dag.py       # Airflow DAG definition
├── data/
│   ├── raw/                   # Downloaded CSV files (not in git)
│   │   └── ml-32m/
│   │       ├── movies.csv
│   │       └── ratings.csv
│   └── output/                # Analytics results
│       ├── top_10_movies_by_avg_rating.csv
│       ├── least_10_movies_by_avg_rating.csv
│       ├── top_5_genres_by_num_ratings.csv
│       └── least_5_genres_by_num_ratings.csv
├── logs/
│   └── pipeline.log           # Execution logs
├── scripts/
│   ├── download_data.py       # Task 1: Download dataset
│   ├── load_staging.py        # Task 2: Load to staging tables
│   ├── transform_data.py      # Task 3: Clean and transform
│   ├── data_quality.py        # Task 4: Data quality checks
│   ├── create_warehouse.py    # Task 5: Create star schema
│   ├── run_analytics.py       # Task 6: Run analytics queries
│   └── test_connection.py     # Database connection test
├── .gitignore
├── README.md
└── requirements.txt
```

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python 3.12** | Main programming language |
| **PostgreSQL 18** | Data warehouse database |
| **Apache Airflow 2.10.4** | Workflow orchestration |
| **pandas** | Data manipulation and CSV handling |
| **SQLAlchemy** | Database connectivity |
| **psycopg2** | PostgreSQL adapter |

## 📈 Dataset Information

**MovieLens 32M Dataset** from [GroupLens](https://grouplens.org/datasets/movielens/32m/)

| Metric | Value |
|--------|-------|
| Total Ratings | 32,000,204 |
| Total Movies | 87,585 |
| Total Users | 200,948 |
| Total Genres | 20 |
| Rating Range | 0.5 - 5.0 |
| Average Rating | 3.54 |

## 🚀 Setup Instructions

### Prerequisites
- Ubuntu/WSL with Python 3.12
- PostgreSQL installed and running
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/mmesomakelvin/movielens-elt-pipeline.git
   cd movielens-elt-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database**
   
   Edit `config/config.py` with your PostgreSQL credentials:
   ```python
   DB_HOST = "localhost"
   DB_PORT = "5432"
   DB_NAME = "movielens_db"
   DB_USER = "postgres"
   DB_PASSWORD = "your_password"
   ```

5. **Create the database**
   ```bash
   psql -U postgres -c "CREATE DATABASE movielens_db;"
   ```

## ▶️ Running the Pipeline

### Run Individual Scripts

```bash
# Activate virtual environment
source venv/bin/activate

# Task 1: Download data
python scripts/download_data.py

# Task 2: Load to staging
python scripts/load_staging.py

# Task 3: Transform data
python scripts/transform_data.py

# Task 4: Data quality checks
python scripts/data_quality.py

# Task 5: Create warehouse
python scripts/create_warehouse.py

# Task 6: Run analytics
python scripts/run_analytics.py
```

### Run with Airflow

```bash
# Set DAGs folder
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/movielens_elt_pipeline/dags

# Initialize Airflow
airflow db init

# Start webserver (Terminal 1)
airflow webserver --port 8080

# Start scheduler (Terminal 2)
airflow scheduler

# Access UI at http://localhost:8080
```

## 📊 Analytics Results

### Top 10 Movies by Average Rating (min 100 ratings)

| Rank | Movie | Avg Rating |
|------|-------|------------|
| 1 | Planet Earth II (2016) | 4.45 |
| 2 | Planet Earth (2006) | 4.44 |
| 3 | Band of Brothers (2001) | 4.43 |
| 4 | The Shawshank Redemption (1994) | 4.40 |
| 5 | The Godfather (1972) | 4.36 |

### Top 5 Genres by Number of Ratings

| Rank | Genre | Total Ratings |
|------|-------|---------------|
| 1 | Drama | 13,912,753 |
| 2 | Comedy | 11,234,567 |
| 3 | Action | 9,756,432 |
| 4 | Thriller | 8,723,456 |
| 5 | Adventure | 7,654,321 |

## 🔄 Pipeline Tasks

| Task | Script | Description | Duration |
|------|--------|-------------|----------|
| 1 | download_data.py | Download ml-32m.zip (228 MB) | ~2 min |
| 2 | load_staging.py | Load 32M rows to staging | ~32 min |
| 3 | transform_data.py | Clean and transform data | ~6 min |
| 4 | data_quality.py | Run 17 validation checks | ~2 min |
| 5 | create_warehouse.py | Build star schema | ~10 min |
| 6 | run_analytics.py | Generate analytics CSVs | ~21 min |

**Total Pipeline Duration: ~73 minutes**

## ✅ Data Quality Checks

17 validation checks with 100% pass rate:

- ✅ No NULL values in primary keys
- ✅ No duplicate records
- ✅ All ratings within valid range (0.5-5.0)
- ✅ Referential integrity between tables
- ✅ Row count validations
- ✅ Data type validations

## 📅 Airflow Schedule

The DAG is configured to run daily at 12:00 PM:

```python
schedule_interval='0 12 * * *'  # Cron: minute hour day month weekday
```

**Task Dependencies:**
```
load_staging → transform_data → data_quality → create_warehouse → run_analytics
```


## 👤 Author

**Mmesoma Kelvin**
- Email: mmesomakelvin@gmail.com
- GitHub: [@mmesomakelvin](https://github.com/mmesomakelvin)

## 📄 License

This project is for educational purposes as part of the AICA Data Engineering Track Capstone Project.

## 🙏 Acknowledgments

- [GroupLens](https://grouplens.org/) for the MovieLens dataset
- AICA Data Engineering Track instructors
- Apache Airflow community