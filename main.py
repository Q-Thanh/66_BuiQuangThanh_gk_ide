from app.crawler import crawl_vnexpress_ai
from app.data_cleaner import clean_data
from app.db_handler import create_table, save_to_postgres

def run_pipeline():
    print("Starting news pipeline...")
    
    # Step 1: Create table if it doesn't exist
    create_table()
    
    # Step 2: Crawl VnExpress AI articles
    print("Crawling articles...")
    articles = crawl_vnexpress_ai()
    print(f"Crawled {len(articles)} articles")
    
    # Step 3: Clean the data
    print("Cleaning data...")
    cleaned_articles = clean_data(articles)
    
    # Step 4: Save to PostgreSQL
    print("Saving to database...")
    save_to_postgres(cleaned_articles)
    
    print("Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()