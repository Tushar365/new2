from dotenv import load_dotenv
import os

class Settings:
    # Load environment variables
    load_dotenv()
    
    # Reddit API settings
    REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
    REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
    REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
    
    # Scraper settings
    DEFAULT_POST_LIMIT = 10
    DEFAULT_SUBREDDITS = ['python', 'learnpython']
    
    @classmethod
    def get_database_url(cls):
        return (
            f"postgresql://{cls.DB_USERNAME}:{cls.DB_PASSWORD}@"
            f"{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        )
    
    @classmethod
    def validate_reddit_credentials(cls):
        return all([
            cls.REDDIT_CLIENT_ID,
            cls.REDDIT_CLIENT_SECRET,
            cls.REDDIT_USER_AGENT
        ])
