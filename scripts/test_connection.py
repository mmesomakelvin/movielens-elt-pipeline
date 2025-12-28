"""Test database connection"""
import psycopg2

try:
    conn = psycopg2.connect(
        host='localhost',
        port='5432',
        database='movielens_db',
        user='postgres',
        password='Admin'
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")