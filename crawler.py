import requests
from bs4 import BeautifulSoup
import time

def crawl_vnexpress_ai():
    url = "https://vnexpress.net/cong-nghe/ai"
    
    # Make request with headers to mimic a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch the page: {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find article containers
    articles = soup.select(".item-news.item-news-common")
    
    results = []
    count = 0
    
    for article in articles:
        if count >= 5:
            break
            
        # Extract data
        title_element = article.select_one(".title-news a")
        if not title_element:
            continue
            
        title = title_element.text.strip()
        article_url = title_element["href"]
        
        # Extract summary
        summary_element = article.select_one(".description a")
        summary = summary_element.text.strip() if summary_element else ""
        
        # Fetch article page to get author and time
        article_response = requests.get(article_url, headers=headers)
        if article_response.status_code == 200:
            article_soup = BeautifulSoup(article_response.text, "html.parser")
            
            # Extract time
            time_element = article_soup.select_one(".date")
            article_time = time_element.text.strip() if time_element else ""
            
            # Extract author
            author_element = article_soup.select_one(".author_mail") or article_soup.select_one(".author")
            author = author_element.text.strip() if author_element else ""
            
            results.append({
                "title": title,
                "url": article_url,
                "summary": summary,
                "time": article_time,
                "author": author
            })
            
            count += 1
            
            # Be nice to the server
            time.sleep(1)
    
    return results

if __name__ == "__main__":
    articles = crawl_vnexpress_ai()
    for i, article in enumerate(articles, 1):
        print(f"Article {i}:")
        print(f"Title: {article['title']}")
        print(f"URL: {article['url']}")
        print(f"Summary: {article['summary']}")
        print(f"Time: {article['time']}")
        print(f"Author: {article['author']}")
        print("-" * 50)