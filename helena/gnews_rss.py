import feedparser
import pandas as pd
import nltk
from newspaper import Article
from googlenewsdecoder import gnewsdecoder
nltk.download('punkt_tab')

def convert_google_news_link(google_news_url):
    try:
        decoded_url = gnewsdecoder(google_news_url, interval=1)

        if decoded_url.get("status"):
            return decoded_url["decoded_url"]
        else:
            return "ERROR"
           
    except Exception as e:
        print(f"Error occurred: {e}")
        return "ERROR"
    
def process_google_news_feed(rss_url: str, id_base: int):
    
    feed = feedparser.parse(rss_url)
    articles = []
    for i in range(10):
        entry = feed.entries[i]
        article_info = {}
        if 'link' in entry:
            link = convert_google_news_link(entry.link)
        else:
            continue
        
        article = Article(link)
        try:
            article.download()
            article.parse()
            article_info['id'] = id_base
            id_base += 1
            article_info['link'] = link
            article_info['title'] = article.title
            article_info['text'] = article.text.replace('\n', ' ')
            article_info['year'] = entry.published_parsed.tm_year
            article_info['month'] = entry.published_parsed.tm_mon
            article_info['day'] = entry.published_parsed.tm_mday
            article.nlp()
            article_info['summary'] = article.summary
            article_info['keywords'] = article.keywords
            print(f"Processed article: {article_info['title']}")
        except Exception as e:
            print(f"Error processing article at {link}: {e}")
            continue

        
        articles.append(article_info)

    pd.DataFrame(articles).to_csv('articles.csv', index=False)
    return articles

def create_txt_files_from_articles(articles):
    print("yay")
    for article in articles:
        id = article['id']
        month = article['month']
        year = article['year']
        filename = f"articles/{year}-{month}-{id}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"{article['text']}")

if __name__ == "__main__":
    rss_url = "https://news.google.com/rss/search?q=SEND;+education&hl=en-GB&gl=GB&ceid=GB:en"
    articles = process_google_news_feed(rss_url, id_base=0)
    print(f"\n\nTotal: {len(articles)} articles found")
    create_txt_files_from_articles(articles)