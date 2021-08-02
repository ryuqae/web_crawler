import requests
from bs4 import BeautifulSoup
import unicodedata
from urllib import parse
# import pandas as pd
import argparse
from datetime import datetime
import time, random
import json


parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument("--start", required=False, default=1, help="start crawling from.")
parser.add_argument("--end", required=False, default=5, help="crawling until.")
args = parser.parse_args()

start, end = int(args.start), int(args.end)
headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"
}


class WebCrawler:
    def __init__(self) -> None:
        self.base_url = f"https://www.ddanzi.com/index.php?mid=free&"
        pass

    # @profile
    def get_document_srl_per_page(self, params: dict) -> list:
        q = parse.urlencode(params)
        url = self.base_url + q
        result = requests.get(url, headers=headers)
        result_html = result.text

        soup = BeautifulSoup(result_html, "html.parser")

        search_result = soup.select_one("#list_style")
        links = search_result.select("li > .titleBox > a")

        return [
                self._get_contents(parse.parse_qs(link["href"])["document_srl"][0])
                for link in links[4:]
            ]

        

    # @profile
    def _get_contents(self, document_srl: int) -> dict:
        post_url = f"https://www.ddanzi.com/free/{document_srl}"
        result = requests.get(post_url, headers=headers)
        result_html = result.text

        soup = BeautifulSoup(result_html, "html.parser")

        contents = soup.select_one(".boardR")

        header = contents.select_one(".read_header > .top_title")
        post_title = header.select_one("h1 > a").get_text().strip()

        # user = header.select_one('div.right > a').get_text().strip()
        post_time = header.select_one("div.right > p.time").get_text().strip()
        post_text = unicodedata.normalize(
            "NFKD", contents.select_one(".read_content").get_text().strip()
        )
                
           
        output= {
                "id": document_srl,
                "title": post_title,
                "text": post_text,
                "time": post_time,
                "url": post_url,
            }

        # print(output)
        return output


# 189416
if __name__ == "__main__":
    start_timestamp = datetime.now()
    print(f"Crawling DDANZI from {start} to {end}")
    bot = WebCrawler()
    # hund_pages_docs = []

    for idx, page in enumerate(range(start, end + 1)):
        ith = idx + 1

        docs_ = bot.get_document_srl_per_page({"page": page, "m": 1})
        print(f"page{page} : {len(docs_)}")
        with open(f"/media/bcache/jeongwoo/ddanzi/ddanzi_page_{page}.json", "w", encoding="UTF-8") as f:
            json.dump(docs_, f, ensure_ascii=False)

        # if ith % 1000 == 0:
        #     pd.concat(hund_pages_docs).to_pickle(
        #         f"./ddanzi_from_{start}_to_{end}_{page}.pkl", protocol=4
        #     )
        #     hund_pages_docs = []
        # else:
        #     docs_ = bot.get_document_srl_per_page({"page": page, "m": 1})
        #     hund_pages_docs.append(docs_)
        now_timestamp = datetime.now()
        print(
            f"{ith} pages scraped: {round(ith/(end-start+1)*100, 4)}%, {now_timestamp-start_timestamp} passed."
        )
        time.sleep(random.uniform(5, 10))
