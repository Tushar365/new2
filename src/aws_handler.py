import mysql.connector
from mysql.connector import Error
import os
import pandas as pd
from dotenv import load_dotenv


class DatabaseConnection:
    def __init__(self):
        # AWS RDS configuration
        self.config = {
            'host': 'redditdb.cbkuy486ce24.ap-south-1.rds.amazonaws.com',
            'user': 'admin',     # Change this to your RDS master username (usually 'admin')
            'password': 'admin123',  # Replace with your actual RDS password
            'database': 'reddit01',
            'port': 3306
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish connection to the database"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                self.cursor = self.connection.cursor(dictionary=True)
                print(f"Successfully connected to MySQL database version {db_info}")
                return True
        except Error as e:
            print(f"Error connecting to MySQL Database: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()
            print("Database connection closed")


def import_scraped_data_to_db(db, scraped_data):
    try:
        # Convert the scraped data to DataFrame
        df = pd.DataFrame(scraped_data)
        
        # Convert created_utc to datetime if it's in unix timestamp format
        if 'created_utc' in df.columns:
            df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
        
        # Prepare insert query
        insert_query = """
        INSERT INTO reddit_posts 
        (id, author, title, text, url, created_utc, score, num_comments, subreddit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert DataFrame to list of tuples for batch insert
        values = df.fillna('').apply(tuple, axis=1).tolist()
        
        # Batch insert records
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            try:
                db.cursor.executemany(insert_query, batch)
                db.connection.commit()
                print(f"Inserted records {i} to {i + len(batch)}")
            except Error as e:
                print(f"Error inserting batch: {e}")
                db.connection.rollback()
                
        print(f"Data import completed successfully! Total records inserted: {len(df)}")
        
    except Exception as e:
        print(f"Error during import: {e}")

def create_reddit_table(db):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS reddit_posts (
        id VARCHAR(255) PRIMARY KEY,
        author VARCHAR(255) NOT NULL,
        title TEXT NOT NULL,
        text LONGTEXT,
        url TEXT,
        created_utc TIMESTAMP,
        score INT DEFAULT 0,
        num_comments INT DEFAULT 0,
        subreddit VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    """
    
    try:
        db.cursor.execute(create_table_query)
        db.connection.commit()
        print("Table 'reddit_posts' created successfully!")
        
        # Create indexes for better query performance
        indexes = [
            "CREATE INDEX idx_author ON reddit_posts(author);",
            "CREATE INDEX idx_subreddit ON reddit_posts(subreddit);",
            "CREATE INDEX idx_created_utc ON reddit_posts(created_utc);",
            "CREATE INDEX idx_score ON reddit_posts(score);"
        ]
        
        for index_query in indexes:
            try:
                db.cursor.execute(index_query)
                db.connection.commit()
            except Error as e:
                # Skip if index already exists
                if e.errno != 1061:  # 1061 is MySQL error for duplicate index
                    print(f"Error creating index: {e}")
                
        print("Indexes created successfully!")
        
    except Error as e:
        print(f"Error creating table: {e}")



def fetch_reddit_data(db_connection, query, params=None):
    """Fetches data from the reddit_posts table.

    Args:
        db_connection: An instance of the DatabaseConnection class.
        query: The SQL query string.
        params: Optional parameters for the query (to prevent SQL injection).

    Returns:
        A list of dictionaries, where each dictionary represents a row.
        Returns None if there's an error.
    """
    try:
        cursor = db_connection.cursor
        cursor.execute(query, params)
        results = cursor.fetchall()
        return results
    except Error as e:
        print(f"Error fetching data: {e}")
        return None

      
db_conn = DatabaseConnection()
try:
    if db_conn.connect():
        limit = 20  # Set your desired limit

        query = "SELECT * FROM reddit_posts LIMIT %s;" # Limit in the query
        params = (limit,) # Limit passed as a parameter

        all_posts = fetch_reddit_data(db_conn, query, params)
        if all_posts:
            print(f"Retrieved {len(all_posts)} posts (limit was {limit}):")
            for post in all_posts:
                print(post)
        else:
             print("No posts found or an error occurred.") # Handle the case where no posts are found

finally:  # Ensure the connection is closed even if errors occur
    db_conn.disconnect()

    import pandas as pd
from mysql.connector import Error

def import_scraped_data_to_db(db, scraped_data):
    try:
        df = pd.DataFrame(scraped_data)

        if 'created_utc' in df.columns:
            df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')

        # ---  Changes to prevent duplicates and updates ---
        placeholders = ", ".join(["%s"] * len(df.columns))  # Dynamic placeholders
        columns = ", ".join(df.columns)

        insert_query = f"""
        INSERT IGNORE INTO reddit_posts ({columns}) 
        VALUES ({placeholders})
        """

        # Convert DataFrame rows to tuples
        values = [tuple(row) for row in df.fillna('').to_records(index=False)]

        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            try:
                db.cursor.executemany(insert_query, batch)
                db.connection.commit()
                print(f"Inserted/Ignored records {i} to {i + len(batch)}")  # Indicate some might be ignored
            except Error as e:
                print(f"Error inserting batch: {e}")
                db.connection.rollback()

        print(f"Data import completed. {len(values)} records processed. Some might have been ignored due to duplicates.")

    except Exception as e:
        print(f"Error during import: {e}")


def append_data_to_db(db, df):
    """
    Append data to a database table
    
    Args:
        db: Database connection object
        df: Pandas DataFrame to append
        table_name: Name of the table to append to
    """
    try:
        # Prepare insert query
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        
        insert_query = f"""
        INSERT INTO {"reddit_posts"} ({column_names})
        VALUES ({placeholders})
        """
        
        # Convert DataFrame to list of tuples
        values = df.fillna('').to_records(index=False).tolist()
        
        # Batch insert
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            
            try:
                db.cursor.executemany(insert_query, batch)
                db.connection.commit()
                print(f"Inserted batch from {i} to {i+len(batch)}")
            except Exception as batch_error:
                print(f"Error inserting batch: {batch_error}")
                db.connection.rollback()
        
        print(f"Successfully appended {len(values)} records to {"reddit_posts"}")
    
    except Exception as e:
        print(f"Error during append: {e}")

def get_posts_by_subreddit(db_connection, subreddit, limit):  # Added limit parameter
    """Fetches posts from a specific subreddit with a limit.

    Args:
        db_connection: An instance of the DatabaseConnection class.
        subreddit: The name of the subreddit.
        limit: The maximum number of posts to retrieve (default is 10).

    Returns:
        A list of dictionaries (posts) or None if an error occurs.
    """

    query = "SELECT * FROM reddit_posts WHERE subreddit = %s LIMIT %s;" # Limit added to query
    params = (subreddit, limit) # Limit passed as a parameter
    posts = fetch_reddit_data(db_connection, query, params)
    return posts



# Example usage (with limit):

db_conn = DatabaseConnection()
if db_conn.connect():
    try:
        target_subreddit = "nonduality"  # Or any subreddit you want
        limit = 1 # Set your desired limit


        subreddit_posts = get_posts_by_subreddit(db_conn, target_subreddit, limit)


        if subreddit_posts:
            print(f"Posts from r/{target_subreddit} (limit {limit}):")
            for post in subreddit_posts:
                for field, value in post.items(): # Iterate through all fields/values
                    print(f"{field.capitalize()}: {value}") # Print field name and value
                print("-" * 20)  # Separator between posts
        else:
            print(f"No posts found for r/{target_subreddit} or an error occurred.")

    finally:
        db_conn.disconnect()