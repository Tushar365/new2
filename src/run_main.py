import streamlit as st
import pandas as pd
import mysql.connector
from reddit import RedditScraper
from settings import Settings
from aws_handler import DatabaseConnection

def create_streamlit_app():
    # Set page title and icon
    st.set_page_config(page_title="Reddit Data Scraper", page_icon=":rocket:")
    
    # Title and description
    st.title("🔍 Reddit Data Scraper")
    st.write("Scrape Reddit posts and save them to your database")
    
    # Sidebar for configuration
    st.sidebar.header("Scraper Configuration")
    
    # Subreddit input
    subreddit_input = st.sidebar.text_input(
        "Enter Subreddits (comma-separated)", 
        placeholder="technology, programming, science"
    )
    
    # Post limit slider
    post_limit = st.sidebar.slider(
        "Number of Posts per Subreddit", 
        min_value=1, 
        max_value=100, 
        value=10
    )
    
    # Months slider
    months = st.sidebar.slider(
        "Months of Historical Data", 
        min_value=1, 
        max_value=12, 
        value=3
    )
    
    # Scrape button
    if st.sidebar.button("Scrape Reddit Data"):
        # Validate input
        if not subreddit_input:
            st.error("Please enter at least one subreddit")
            return
        
        # Process subreddit input
        subreddits = [sub.strip() for sub in subreddit_input.split(',')]
        
        try:
            # Scrape data
            with st.spinner('Scraping Reddit posts...'):
                scraper = RedditScraper()
                df = scraper.scrape_subreddit(
                    subreddit_names=subreddits,
                    post_limit=post_limit,
                    months=months
                )
            
            # Display scraped data
            st.success(f"Scraped {len(df)} posts")
            st.dataframe(df)
            
            # Database connection section
            st.header("Database Operations")
            
            # Option to save to database
            if st.button("Save to Database"):
                try:
                    # Establish database connection
                    db = DatabaseConnection()
                    connect = db.connect()
                    
                    if connect:
                        # Function to append data
                        def append_data_to_db(connection, df, table_name):
                            try:
                                # Prepare cursor
                                cursor = connection.cursor()
                                
                                # Prepare insert query
                                columns = df.columns.tolist()
                                placeholders = ', '.join(['%s'] * len(columns))
                                column_names = ', '.join(columns)
                                
                                insert_query = f"""
                                INSERT INTO {table_name} ({column_names})
                                VALUES ({placeholders})
                                """
                                
                                # Convert DataFrame to list of tuples
                                values = df.fillna('').to_records(index=False).tolist()
                                
                                # Execute batch insert
                                cursor.executemany(insert_query, values)
                                connection.commit()
                                
                                st.success(f"Successfully inserted {len(values)} records")
                                return True
                            
                            except Exception as e:
                                st.error(f"Error inserting data: {e}")
                                connection.rollback()
                                return False
                        
                        # Attempt to save to database
                        result = append_data_to_db(connect, df, 'reddit_posts')
                    
                    else:
                        st.error("Failed to establish database connection")
                
                except Exception as e:
                    st.error(f"Database error: {e}")
        
        except Exception as e:
            st.error(f"Scraping error: {e}")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("Reddit Data Scraper v1.0")

# Run the Streamlit app
if __name__ == "__main__":
    create_streamlit_app()