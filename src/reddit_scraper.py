import praw
import prawcore
import pandas as pd
from datetime import datetime, timezone
import logging
import os
from typing import List, Optional
from src.settings import Settings
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging() -> logging.Logger:
    """Set up and configure logging"""
    logger = logging.getLogger('RedditScraper')
    logger.setLevel(logging.INFO)
    
    # Create console handler with formatting
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

class RedditScraper:
    """Comprehensive Reddit post scraper"""
    
    def __init__(self, logger=None):
        """
        Initialize Reddit scraper
        
        Args:
            logger: Optional logger instance
        """
        self.logger = logger or setup_logging()
        self.reddit_client = self._setup_reddit_client()

    def _setup_reddit_client(self):
        """
        Set up Reddit API client with error handling
        
        Returns:
            Configured Reddit API client
        
        Raises:
            ValueError: If client setup fails
        """
        # Validate credentials
        if not Settings.validate_reddit_credentials():
            self.logger.error("Missing Reddit API credentials")
            raise ValueError(
                "Missing Reddit API credentials. Please set REDDIT_CLIENT_ID, "
                "REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT environment variables."
            )

        try:
            self.logger.info("Attempting to create Reddit client")
            
            reddit = praw.Reddit(
                client_id=Settings.REDDIT_CLIENT_ID,
                client_secret=Settings.REDDIT_CLIENT_SECRET,
                user_agent=Settings.REDDIT_USER_AGENT
            )

            # Verify API access
            test_subreddit = reddit.subreddit('test')
            _ = test_subreddit.display_name
            
            self.logger.info("Reddit client successfully created and verified")
            return reddit

        except prawcore.exceptions.PrawcoreException as e:
            self.logger.error(f"PRAW Core Exception: {e}")
            raise

    def scrape_subreddit(
        self, 
        subreddit_names: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        post_limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Scrape posts from specified subreddits
        
        Args:
            subreddit_names: List of subreddit names
            start_date: Start date for posts
            end_date: End date for posts
            post_limit: Maximum number of posts per subreddit
        
        Returns:
            DataFrame of scraped posts
        """
        # Use default limit if not specified
        post_limit = post_limit or Settings.DEFAULT_POST_LIMIT
        
        # Use default subreddits if not specified
        subreddit_names = subreddit_names or Settings.DEFAULT_SUBREDDITS

        all_posts_data = []

        for subreddit_name in subreddit_names:
            try:
                subreddit = self.reddit_client.subreddit(subreddit_name)
                
                posts_collected = 0
                for post in subreddit.new(limit=None):
                    # Convert post creation time
                    post_time = datetime.fromtimestamp(
                        post.created_utc, 
                        tz=timezone.utc
                    )
                    
                    # Apply date filtering
                    if start_date and post_time < start_date:
                        break
                    if end_date and post_time > end_date:
                        continue
                    
                    all_posts_data.append({
                        'id': post.id,
                        'author': str(post.author),
                        'title': post.title,
                        'text': post.selftext,
                        'url': f"https://reddit.com{post.permalink}",
                        'created_utc': post_time,
                        'score': post.score,
                        'num_comments': post.num_comments,
                        'subreddit': subreddit_name
                    })
                    
                    posts_collected += 1
                    # Stop if we've reached the limit
                    if posts_collected >= post_limit:
                        break

                self.logger.info(
                    f"Collected {posts_collected} posts from r/{subreddit_name}"
                )
            
            except prawcore.exceptions.NotFound:
                self.logger.error(f"Subreddit r/{subreddit_name} not found")
            except prawcore.exceptions.Forbidden:
                self.logger.error(f"Access forbidden to r/{subreddit_name}")
            except Exception as e:
                self.logger.error(f"Error collecting posts from {subreddit_name}: {e}")

        # Convert to DataFrame
        return pd.DataFrame(all_posts_data)

if __name__ == '__main__':
    # Initialize the scraper
    scraper = RedditScraper()
    
    # Example: Scrape posts from specific subreddits
  
    df = scraper.scrape_subreddit(
        subreddit_names=Settings.DEFAULT_SUBREDDITS,
        post_limit=Settings.DEFAULT_POST_LIMIT
          # Limit to 50 posts per subreddit
    )
    
    # Save results to CSV
    df.to_csv('reddit_posts.csv', index=False)
    print(f"Saved {len(df)} posts to reddit_posts.csv")