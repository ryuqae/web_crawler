import requests
from urllib.parse import parse_qs
import urllib3
from bs4 import BeautifulSoup
import unicodedata
import json
import os
import argparse
from datetime import datetime
import time, random
from multiprocessing import Manager, Pool
import logging
from get_proxy import ProxyManager
from requests.exceptions import ProxyError, SSLError, ConnectTimeout, RequestException



parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument("--start", required=False, default=1, help="start crawling from.")
parser.add_argument("--end", required=False, default=1000, help="crawling until.")
args = parser.parse_args()

start_page, end_page = int(args.start), int(args.end)


class WebCrawler:
    def __init__(self, proxy_manager:ProxyManager) -> None:
        self.base_url = f"https://www.ddanzi.com/index.php?mid=free&m=1"
        self.proxy_manager = proxy_manager

    def _proxy(self):
        return {'http': f"http://{self.proxy_manager.current_proxy}", 'https':f"http://{self.proxy_manager.current_proxy}"}

    def get_request(self, url:str):
        headers = self.proxy_manager.headers
        proxies = self._proxy()
        print(f"Headers: {headers}, Proxies: {proxies}")
        
        try:
            print(f"pid: {os.getpid()}, current proxy: {self.proxy_manager.current_proxy}")
            result = requests.get(
                url=url,
                headers=headers,
                proxies=proxies,
                verify=True,
                timeout=10,
            )

        except (
            RequestException,TimeoutError,ProxyError, SSLError, ConnectTimeout
        ) as error:
            print(
                f"\n===\nRequest Exception Error - Regenerating Proxies...\n\nerror message : {error}\n===\n"
            )
            self.proxy_manager.next_proxy()
            headers = self.proxy_manager.headers
            proxies = self._proxy()
            result = requests.get(
                url=url,
                headers=headers,
                proxies=proxies,
                verify=True,
                timeout=10,
            )
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

    # @profile
    def get_contents(self, result_list: list, post_url: str) -> dict:

        result_html = self.get_request(post_url)

        soup = BeautifulSoup(result_html, "html.parser")
        contents = soup.select_one(".boardR")
        header = contents.select_one(".read_header > .top_title")

        # user = header.select_one('div.right > a').get_text().strip()
        post_title = header.select_one("h1 > a").get_text().strip()
        post_time = header.select_one("div.right > p.time").get_text().strip()
        post_text = unicodedata.normalize(
            "NFKD", contents.select_one(".read_content").get_text().strip()
        )
        post_id = parse_qs(post_url)["document_srl"][0]

        result_list.append(
            {
                "id": post_id,
                "title": post_title,
                "text": post_text,
                "time": post_time,
                "url": post_url,
            }
        )

        time.sleep(random.uniform(3, 5))


if __name__ == "__main__":
    start_timestamp = datetime.now()
    print(f"Crawling DDANZI from {start_page} to {end_page}")

    pool = Pool(1)
    manager = Manager()
    result_list = manager.list()

    proxy_manager = ProxyManager()

    proxy_manager.current_proxy
    bot = WebCrawler(proxy_manager=proxy_manager)


    for idx, page in enumerate(range(start_page, end_page + 1)):
        ith = idx + 1

        all_urls = bot.get_post_urls(page)
        pool.starmap(bot.get_contents, [(result_list, link) for link in all_urls])

        with open(
            file=f"/media/bcache/jeongwoo/ddanzi/ddanzi_{page}.json",
            mode="w",
            encoding="UTF-8",
        ) as json_file:
            json.dump(list(result_list), json_file, ensure_ascii=False)

        if ith % 10 != 0:
            now_timestamp = datetime.now()
            print(
                f"\n => {ith} page(s) scraped: {round(ith/(end_page-start_page+1)*100, 4)}%, : proxy - {bot.proxies}, {now_timestamp-start_timestamp} passed."
            )
        result_list = manager.list()

    pool.close()
    pool.join()
