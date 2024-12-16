import psycopg2

try:
    conn = psycopg2.connect(
        host="reddit-scraper-db.cbkuy486ce24.ap-south-1.rds.amazonaws.com",
        port=5432,
        # Add other connection parameters (database, user, password)
        database="reddit-scraper-db",
        user="postgres",
        password="tushar10"
    )
    print("Connection successful")
except Exception as e:
    print(f"Connection failed: {e}")