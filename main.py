import requests
from bs4 import BeautifulSoup
import unicodedata
from urllib import parse
import pandas as pd
import argparse
from datetime import datetime
import time, random
import multiprocessing
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from fake_headers import Headers


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
        self.proxy = self.proxy_create()

    def proxy_create(self):
        """
        무작위로 프록시를 생성해서 가져오는 코드
        """
        self.req_proxy = RequestProxy()
        proxy = self.test_proxy()  # 잘 작동되는 프록시 선별
        return proxy

    def test_proxy(self):
        """
        가져온 프록시중에서 실제로 작동되는 프록시만 하나씩 가져오는 코드
        test_url : 자신의 IP를 확인하는 코드. 여기서 변경된 IP가 나오면 성공적으로 우회가된것
        """

        test_url = "http://ipv4.icanhazip.com"
        while True:  # 제대로된 프록시가 나올때까지 무한반복
            requests = self.req_proxy.generate_proxied_request(test_url)

            if requests is not None:
                print(
                    "\t Response: ip={0}".format("".join(requests.text).encode("utf-8"))
                )
                proxy = self.req_proxy.current_proxy
                break

            else:
                continue

        return proxy  # 잘작동된 proxy를 뽑아준다.

    # @profile
    def get_document_srl_per_page(self, params: dict) -> list:
        q = parse.urlencode(params)
        url = self.base_url + q
        result = requests.get(url, headers=headers)
        result_html = result.text

        soup = BeautifulSoup(result_html, "html.parser")

        search_result = soup.select_one("#list_style")
        links = search_result.select("li > .titleBox > a")

        return pd.DataFrame(
            [
                self.get_contents(parse.parse_qs(link["href"])["document_srl"][0])
                for link in links[4:]
            ],
            columns=["id", "title", "text", "time"],
        )

    # @profile
    def get_contents(self, document_srl: int) -> dict:
        post_url = f"https://www.ddanzi.com/free/{document_srl}"
        result = requests.get(post_url, headers=headers)
        result_html = result.text

        soup = BeautifulSoup(result_html, "html.parser")

        contents = soup.select_one(".boardR")

        header = contents.select_one(".read_header > .top_title")
        title = header.select_one("h1 > a").get_text().strip()

        # user = header.select_one('div.right > a').get_text().strip()
        time = header.select_one("div.right > p.time").get_text().strip()
        text = unicodedata.normalize(
            "NFKD", contents.select_one(".read_content").get_text().strip()
        )

        return document_srl, title, text, time

    def crawling(self):
        header = Headers(
            browser="chrome",  # Generate only Chrome UA
            os="win",  # Generate ony Windows platform
            headers=True,  # generate misc headers
        )
        self.headers = header.generate()  # 랜덤 유저 에이전트를 생성해주는 함수.
        _url = "https://www.ddanzi.com/free"

        self.proxies = {}  # request.get 인자에 넣어줄 딕셔너리 생성
        self.proxies["http"] = "http://%s" % self.proxy

        html = requests.get(_url, headers=self.headers, proxies=self.proxies).content
        print(html)


if __name__ == "__main__":
    start_timestamp = datetime.now()
    print(f"Crawling DDANZI from {start} to {end}")
    bot = WebCrawler()
    hund_pages_docs = []

    for idx, page in enumerate(range(start, end + 1)):
        now_timestamp = datetime.now()
        ith = idx + 1
        if ith % 1000 == 0:
            pd.concat(hund_pages_docs).to_pickle(
                f"./ddanzi_from_{start}_to_{end}_{page}.pkl", protocol=4
            )
            hund_pages_docs = []
        else:
            docs_ = bot.get_document_srl_per_page({"page": page, "m": 1})
            hund_pages_docs.append(docs_)
        print(
            f"{ith} pages scraped: {round(ith/(end-start)*100, 4)}%, {now_timestamp-start_timestamp} passed."
        )
        time.sleep(random.uniform(0, 5))
