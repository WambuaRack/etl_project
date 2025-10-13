import requests
from bs4 import BeautifulSoup
import pandas as pd

def extract_quotes():
    url = "http://quotes.toscrape.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    quotes = []
    for quote in soup.find_all("div", class_="quote"):
        text = quote.find("span", class_="text").get_text()
        author = quote.find("small", class_="author").get_text()
        tags = [tag.get_text() for tag in quote.find_all("a", class_="tag")]
        quotes.append({"text": text, "author": author, "tags": ",".join(tags)})
    
    df = pd.DataFrame(quotes)
    return df

if __name__ == "__main__":
    df = extract_quotes()
    df.to_csv("quotes.csv", index=False)
