import requests
from bs4 import BeautifulSoup
import unicodedata
from urllib import parse
import pandas as pd
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("--start", required=True, help="start crawling from.")
parser.add_argument("--end", required=True, help="crawling until.")
args=parser.parse_args()

start, end = int(args.start), int(args.end)
headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"}





class WebCrawler:
    def __init__(self) -> None:
        self.base_url = f"https://www.ddanzi.com/index.php?mid=free&"
        pass

    def get_document_srl_per_page(self, params: dict) -> list:
        q = parse.urlencode(params)
        url = self.base_url + q
        result = requests.get(url, headers=headers)
        result_html = result.text

        soup = BeautifulSoup(result_html, 'html.parser')

        search_result = soup.select_one("#list_style")
        links = search_result.select("li > .titleBox > a")

        return pd.DataFrame([self._get_contents(parse.parse_qs(link['href'])['document_srl'][0]) for link in links[4:]], columns=['id', 'title', 'text', 'time'])
            

    def _get_contents(self, document_srl:int) -> dict:
        post_url = f'https://www.ddanzi.com/free/{document_srl}'
        result = requests.get(post_url, headers=headers)
        result_html = result.text


        soup = BeautifulSoup(result_html, "html.parser")

        contents = soup.select_one('.boardR')

        header = contents.select_one('.read_header > .top_title')
        title = header.select_one('h1 > a').get_text().strip()


        # user = header.select_one('div.right > a').get_text().strip()
        time = header.select_one('div.right > p.time').get_text().strip()
        text = unicodedata.normalize("NFKD",contents.select_one('.read_content').get_text().strip())

        return document_srl, title, text, time

# 189416
if __name__=="__main__":
    start_timestamp = datetime.now()
    print(f"Crawling DDANZI from {start} to {end}")
    bot = WebCrawler()
    hund_pages_docs = []
    
    for page in range(start, end):
        now_timestamp = datetime.now()
        if page % 1000 == 0:
            print(f"{page} pages scraped: {round(page/(end-start)*100, 4)}%, {now_timestamp-start_timestamp} passed.")
            pd.concat(hund_pages_docs).to_pickle(f'./ddanzi_from_{start}_to_{end}_{page}.pkl', protocol=4)
            hund_pages_docs = []
        else:
            print(f"{page} pages scraped: {round(page/(end-start)*100, 4)}%, {now_timestamp-start_timestamp} passed.")
            docs_ = bot.get_document_srl_per_page({"page":page, "m":1})
            hund_pages_docs.append(docs_)

    # print(a.get_contents(133455569))






