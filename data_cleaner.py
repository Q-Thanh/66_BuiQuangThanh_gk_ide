import re
from bs4 import BeautifulSoup
import requests

def clean_data(articles):
    cleaned_articles = []
    
    for article in articles:
        # Make a deep copy to avoid modifying the original
        cleaned_article = article.copy()
        
        # Clean title - remove HTML tags if any
        if cleaned_article.get("title"):
            cleaned_article["title"] = BeautifulSoup(cleaned_article["title"], "html.parser").text.strip()
        
        # Clean summary - remove HTML tags if any
        if cleaned_article.get("summary"):
            cleaned_article["summary"] = BeautifulSoup(cleaned_article["summary"], "html.parser").text.strip()
        
        # Clean time - normalize format
        if cleaned_article.get("time"):
            # Remove extra spaces and normalize format
            cleaned_article["time"] = re.sub(r'\s+', ' ', cleaned_article["time"]).strip()
        
        # Clean author - remove any prefixes like "By" if present
        if cleaned_article.get("author"):
            cleaned_article["author"] = re.sub(r'^By\s+', '', cleaned_article["author"]).strip()
        
        # Fetch full content if needed
        if cleaned_article.get("url"):
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                response = requests.get(cleaned_article["url"], headers=headers)
                if response.status_code == 200:
                    content_soup = BeautifulSoup(response.text, "html.parser")
                    
                    # Extract main content
                    content_div = content_soup.select_one(".fck_detail")
                    if content_div:
                        # Remove unwanted elements like ads, related news, etc.
                        for unwanted in content_div.select(".box-taitro, .Image, .related-news"):
                            unwanted.decompose()
                        
                        # Get clean text content
                        cleaned_article["content"] = content_div.get_text(separator="\n", strip=True)
            except Exception as e:
                print(f"Error fetching full content: {e}")
        
        cleaned_articles.append(cleaned_article)
    
    return cleaned_articles

if __name__ == "__main__":
    from crawler import crawl_vnexpress_ai
    
    articles = crawl_vnexpress_ai()
    cleaned_articles = clean_data(articles)
    
    for i, article in enumerate(cleaned_articles, 1):
        print(f"Cleaned Article {i}:")
        print(f"Title: {article['title']}")
        print(f"Summary: {article['summary']}")
        print(f"Time: {article['time']}")
        print(f"Author: {article['author']}")
        if "content" in article:
            print(f"Content Preview: {article['content'][:200]}...")
        print("-" * 50)