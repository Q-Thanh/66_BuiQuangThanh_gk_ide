import psycopg2
from psycopg2 import sql
import os

def get_db_connection_params():
    """
    Return database connection parameters based on environment
    In Docker: use 'postgres' as host
    Local development: use 'localhost' as host
    """
    # Check if we're running in Docker
    in_docker = os.environ.get('IN_DOCKER', False)
    
    if in_docker:
        host = "postgres"
    else:
        host = "localhost"  # Use localhost for local development
    
    return {
        "host": host,
        "database": "news_db",
        "user": "postgres",
        "password": "postgres"
    }

def create_table():
    """Create the news articles table if it doesn't exist"""
    conn = None
    try:
        # Get connection parameters
        db_params = get_db_connection_params()
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        
        # Create a cursor
        cur = conn.cursor()
        
        # SQL to create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            summary TEXT,
            article_time TEXT,
            author TEXT,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute the query
        cur.execute(create_table_query)
        
        # Commit the changes
        conn.commit()
        
        # Close the cursor
        cur.close()
        
        print("Table created successfully")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while creating table: {error}")
    finally:
        if conn is not None:
            conn.close()

def save_to_postgres(articles):
    """Save the cleaned articles to PostgreSQL"""
    conn = None
    try:
        # Get connection parameters
        db_params = get_db_connection_params()
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        
        # Create a cursor
        cur = conn.cursor()
        
        # Insert each article
        for article in articles:
            # Check if article already exists
            cur.execute("SELECT url FROM news_articles WHERE url = %s", (article["url"],))
            if cur.fetchone() is None:
                # Article doesn't exist, insert it
                insert_query = """
                INSERT INTO news_articles (title, url, summary, article_time, author, content)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                cur.execute(insert_query, (
                    article.get("title", ""),
                    article.get("url", ""),
                    article.get("summary", ""),
                    article.get("time", ""),
                    article.get("author", ""),
                    article.get("content", "")
                ))
        
        # Commit the changes
        conn.commit()
        
        # Close the cursor
        cur.close()
        
        print(f"Successfully saved {len(articles)} articles to PostgreSQL")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while saving to PostgreSQL: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    from app.crawler import crawl_vnexpress_ai
    from app.data_cleaner import clean_data
    
    # Create table
    create_table()
    
    # Crawl and clean data
    articles = crawl_vnexpress_ai()
    cleaned_articles = clean_data(articles)
    
    # Save to PostgreSQL
    save_to_postgres(cleaned_articles)