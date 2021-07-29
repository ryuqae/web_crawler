import multiprocessing
import requests
from bs4 import BeautifulSoup
import unicodedata
import pandas as pd
import argparse
from datetime import datetime
import time, random
import os
from multiprocessing import Manager, Pool
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from fake_headers import Headers


parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument("--start", required=False, default=1, help="start crawling from.")
parser.add_argument("--end", required=False, default=5, help="crawling until.")
args = parser.parse_args()

start, end = int(args.start), int(args.end)


class WebCrawler:
    def __init__(self, header: Headers) -> None:
        self.base_url = f"https://www.ddanzi.com/index.php?mid=free&m=1"
        self.proxies = self.proxy_create()
        self.headers = header.generate()

    def proxy_create(self):
        """
        무작위로 프록시를 생성해서 가져오는 코드
        """
        self.req_proxy = RequestProxy()
        proxy = self.test_proxy()  # 잘 작동되는 프록시 선별
        proxies = {}  # requests.get 인자에 넣어줄 딕셔너리 생성
        proxies["http"] = "http://%s" % proxy
        return proxies

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

    def get_request(self, url):
        try:
            result = requests.get(url, headers=self.headers, proxies=self.proxies)
        except requests.exceptions.RequestException as error:
            print(f"Request Exception Error - Regenerating Proxies...")
            self.proxies = self.proxy_create()
            result = requests.get(url, headers=self.headers, proxies=self.proxies)
        return result.text

    # @profile
    def get_post_urls(self, page: int) -> list:
        """
        Get post urls in the given page.
        """
        url = self.base_url + f"&page={page}"

        result_html = self.get_request(url)

        soup = BeautifulSoup(result_html, "html.parser")

        search_result = soup.select_one("#list_style")
        links = search_result.select("li > .titleBox > a")

        post_urls = [link["href"] for link in links[4:]]

        return post_urls

        # return pd.DataFrame(
        #     [
        #         self.get_contents(parse.parse_qs(link["href"])["post_url"][0])
        #         for link in links[4:]
        #     ],
        #     columns=["id", "title", "text", "time"],
        # )

    # @profile
    def get_contents(self, result_list: list, post_url: str) -> dict:

        result_html = self.get_request(post_url)

        soup = BeautifulSoup(result_html, "html.parser")

        contents = soup.select_one(".boardR")

        header = contents.select_one(".read_header > .top_title")
        post_title = header.select_one("h1 > a").get_text().strip()

        # user = header.select_one('div.right > a').get_text().strip()
        post_time = header.select_one("div.right > p.time").get_text().strip()
        post_text = unicodedata.normalize(
            "NFKD", contents.select_one(".read_content").get_text().strip()
        )

        result_list.append([post_url, post_title, post_text, post_time])
        print(post_url, post_title, post_text, post_time, os.getpid())

        # return post_url, post_title, post_text, post_time


if __name__ == "__main__":
    start_timestamp = datetime.now()
    print(f"Crawling DDANZI from {start} to {end}")

    pool = Pool(8)
    manager = Manager()
    result_list = manager.list()

    header = Headers(
        browser="chrome",  # Generate only Chrome UA
        os="win",  # Generate ony Windows platform
        headers=True,  # generate misc headers
    )

    bot = WebCrawler(header=header)
    for page in range(1, 100):

        all_urls = bot.get_post_urls(page)
        pool.starmap(bot.get_contents, [(result_list, link) for link in all_urls])
        time.sleep(random.uniform(0, 5))

    # print(result_list)

    pool.close()
    pool.join()

    # bot.proxy_create()

    # hund_pages_docs = []

    # for idx, page in enumerate(range(start, end + 1)):
    #     now_timestamp = datetime.now()
    #     ith = idx + 1
    #     if ith % 1000 == 0:
    #         pd.concat(hund_pages_docs).to_pickle(
    #             f"./ddanzi_from_{start}_to_{end}_{page}.pkl", protocol=4
    #         )
    #         hund_pages_docs = []
    #     else:
    #         docs_ = bot.get_post_url_per_page({"page": page, "m": 1})
    #         hund_pages_docs.append(docs_)
    #     print(
    #         f"{ith} pages scraped: {round(ith/(end-start)*100, 4)}%, {now_timestamp-start_timestamp} passed."
    #     )
    #     # time.sleep(random.uniform(0, 5))
