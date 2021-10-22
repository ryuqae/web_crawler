import requests
from bs4 import BeautifulSoup
import unicodedata
from urllib import parse
import numpy as np

import argparse
from datetime import datetime
import time, random
import json
import os

data_dir = '/media/bcache/jeongwoo/ddanzi'
if len(os.listdir(data_dir)) > 0:
    latest = max([int(os.path.splitext(page)[0].split('_')[-1]) for page in os.listdir(data_dir)])
else:
    latest = 130000

parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument("--start", required=False, default=latest, help="start crawling from.")
parser.add_argument("--end", required=False, default=latest+50000, help="crawling until.")
args = parser.parse_args()

start, end = int(args.start), int(args.end)
headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"
}


def logging_time(original_fn):
    def wrapper_fn(*args, **kwargs):
        start_time = time.time()
        result = original_fn(*args, **kwargs)
        end_time = time.time()
        print(
            "== WorkingTime[{}]: {} sec".format(
                original_fn.__name__, end_time - start_time
            )
        )
        return result

    return wrapper_fn


class WebCrawler:
    def __init__(self) -> None:
        self.base_url = f"https://www.ddanzi.com/index.php?mid=free&"
        pass

    @logging_time
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

    @logging_time
    def _get_contents(self, document_srl: int) -> dict:
        post_url = f"https://www.ddanzi.com/free/{document_srl}"
        try:
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

            output = {
                "id": document_srl,
                "title": post_title,
                "text": post_text,
                "time": post_time,
                "url": post_url,
            }
            # time.sleep(random.uniform(0, 3))
            return output

        except:
            print("error occur")
            pass


if __name__ == "__main__":
    start_timestamp = datetime.now()
    print(f"Crawling DDANZI from {start} to {end}")
    bot = WebCrawler()

    random_pages = sorted(np.random.choice(range(start,end), int((end-start)/5), replace=False))

    # for idx, page in enumerate(range(start, end + 1)):
    for idx, page in enumerate(random_pages):

        ith = idx + 1

        docs_ = bot.get_document_srl_per_page({"page": page, "m": 1})
        print(f"page #{page} : {len(docs_)}")
        # f"/media/bcache/jeongwoo/ddanzi/ddanzi_page_{page}.json",
        with open(
            f"{data_dir}/ddanzi_page_{page}.json",
            "w",
            encoding="UTF-8",
        ) as f:
            json.dump(docs_, f, ensure_ascii=False)

        now_timestamp = datetime.now()
        print(
            f"{ith} pages scraped: {round(ith/(end-start+1)*100, 4)}%, {now_timestamp-start_timestamp} passed."
        )
        # time.sleep(random.uniform(0, 1))
