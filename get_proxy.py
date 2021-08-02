from fake_headers import Headers
import requests
from scrapy import Selector
import concurrent.futures
import threading
import random

from requests.exceptions import (
    ProxyError,
    SSLError,
    ConnectTimeout,
    HTTPError,
    RequestException,
)
from urllib3.exceptions import *
from urllib3.connection import VerifiedHTTPSConnection
import os
import time


class ProxyManager:
    def __init__(self) -> None:
        self.test_url = "https://www.ddanzi.com/index.php?mid=free&m=1"
        self.test_url = "https://httpbin.org/ip"

        self.header_config = Headers(browser="chrome", os="windows", headers=False)
        self.headers = self.header_config.generate()
        self.proxy_list = []
        self.get_valid_proxies()
        self.current_proxy = random.choice(self.proxy_list)

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

    @logging_time
    def add_proxies(self):
        proxy_url = "https://free-proxy-list.net/"

        response = requests.get(proxy_url)
        selector = Selector(response)
        tr_list = selector.xpath('//*[@id="proxylisttable"]/tbody/tr')

        for tr in tr_list:
            ip = tr.xpath("td[1]/text()").extract_first()
            port = tr.xpath("td[2]/text()").extract_first()
            ptype = tr.xpath("td[5]/text()").extract_first()
            https = tr.xpath("td[7]/text()").extract_first()

            if https == "yes" and ptype == "elite proxy":
                server = f"{ip}:{port}"
                self.proxy_list.append(server)
        print(f"Added proxies: {len(self.proxy_list)}")

    def _check_proxy(self, proxy: str, timeout: int = 3):
        try:
            r = requests.get(
                self.test_url,
                headers=self.headers,
                proxies={"http": proxy, "https": proxy},
                timeout=timeout,
            )
            print(
                f"status : [{r.status_code}], proxy: {r.json()}, job : {os.getpid(), threading.get_ident()}"
            )
            return proxy

        # except (
        #     ProxyError,
        #     SSLError,
        #     ConnectTimeout,
        #     MaxRetryError,
        #     HTTPError,
        #     RequestException,
        #     ConnectionResetError
        # ) as e:
        except:
            # print(f"{proxy} was removed at job : {os.getpid(),  threading.get_ident()}")
            self.proxy_list.remove(proxy)
            pass

    @logging_time
    def get_valid_proxies(self):
        if len(self.proxy_list) == 0:
            print("No proxies")
            self.add_proxies()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self._check_proxy, self.proxy_list)

        with open("proxy_list.txt", "a") as f:
            f.writelines("\n".join(self.proxy_list)+"\n")

    def next_proxy(self):
        self.proxy_list.remove(self.current_proxy)
        if len(self.proxy_list) == 0:
            print("No proxies")
            self.add_proxies()
        print(f"Old proxy {self.current_proxy} was removed!")
        self.current_proxy = random.choice(self.proxy_list)
        print(f"New proxy : {self.current_proxy}")


if __name__ == "__main__":

    maker = ProxyManager()

    print(len(maker.proxy_list))
    page = 1
    proxy = random.choice(maker.proxy_list)

    while page < 100:
        proxy =  '189.204.242.178:8080'

        url = f"https://www.ddanzi.com/index.php?mid=free&m=1&page={page}"
        # url = "https://www.naver.com"
        try:
            print(proxy)

            resp = requests.get(
                url=url,
                proxies={"http": proxy, "https": proxy},
                headers=maker.headers,
                timeout=10,
            )

        # except (ProxyError, SSLError, ConnectTimeout) as e:
        except:
            # maker.next_proxy()
            proxy = random.choice(maker.proxy_list)
            resp = requests.get(
                url=url,
                proxies={"http": proxy, "https": proxy},
                headers=maker.headers,
                timeout=10,
            )

        print(resp.status_code, "page : ", page)

        page += 1
        time.sleep(random.uniform(5, 10))

    # while len(maker.proxy_list)>0:
    #     print("current:", maker.current_proxy)
    #     print("next : ", maker.next_proxy())
    #     print(len(maker.proxy_list))
