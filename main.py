import requests
from bs4 import BeautifulSoup
import unicodedata
from urllib import parse
import pandas as pd
import argparse
from datetime import datetime
from multiprocessing import Pool, Manager, cpu_count

parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument("--start", required=True, help="start crawling from.")
parser.add_argument("--end", required=True, help="crawling until.")
args = parser.parse_args()

start, end = int(args.start), int(args.end)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.67"
}

# manager = Manager()
# output = manager.list()
base_url = f"https://www.ddanzi.com/index.php?mid=free&"



def get_document_srl_per_page(**params) -> list:
    '''
    Get post urls in a page and return the list.
    '''
    q = parse.urlencode(params)
    url = base_url + q
    result = requests.get(url, headers=headers)

    if result.ok:
        result_html = result.text
        soup = BeautifulSoup(result_html, "html.parser")
        search_result = soup.select_one("#list_style")
        links = search_result.select("li > .titleBox > a")
        srls = [parse.parse_qs(link["href"])["document_srl"][0] for link in links[4:]]
        return srls
        # ['13414324', '234234234', '234234', '234243234','352342']
        # return pd.DataFrame([item for item in pages_docs], columns=["id", "title", "text", "time"])
    else:
        print("Error occured")
        pass

def get_contents(document_srl: int, idx:int) -> dict:
    if idx%100 == 0:
        print(f"{idx+1}th doc: #{document_srl}")

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

def crawl(start_page: int) -> list:
    

    for idx, page in enumerate(range(start_page, start_page+10)):
        order = idx+1
        per_page = get_document_srl_per_page({"page": page, "m": 1})
        urls.append(per_page)
        now_timestamp = datetime.now()
        print(f"{order} pages scraped: {round(order/10,4)}%, {now_timestamp-start_timestamp} passed.")




# 189416
if __name__ == "__main__":

    print(f"Crawling DDANZI from {start} to {end}")

    urls = []
    start_timestamp = datetime.now()
    for idx, page in enumerate(range(start, end)):
        if idx % 100==0:
            print(f"{idx+1}th url in page {page}")
        per_page = get_document_srl_per_page(page=page, m=1)
        urls.extend(per_page)

    print(f"from page {start} to {end} : {len(urls)} urls gathered, {datetime.now()-start_timestamp} passed.")
    print(f"Available number of cpu: {cpu_count()}")

    pool = Pool(processes=8)
    results = [pool.apply_async(get_contents, (url,idx)) for idx, url in enumerate(urls)]
    pool.close()
    pool.join()
    pd.DataFrame([result.get() for result in results], columns=["id", "title", "text", "time"]).to_pickle(f'./multiproc_{start}_{end}.pkl', protocol=4)
    print(f"from page {start} to {end} : {datetime.now()-start_timestamp} passed.")

    # pd.concat(output).to_pickle(f'./ddanzi_from_{start}_to_{end}_{start+10}.pkl', protocol=4)


    # for page in range(start, end):
    #     now_timestamp = datetime.now()
    #     if page % 1000 == 0:
    #         print(f"{page} pages scraped: {round(page/(end-start)*100, 4)}%, {now_timestamp-start_timestamp} passed.")
    #         pd.concat(hund_pages_docs).to_pickle(f'./ddanzi_from_{start}_to_{end}_{page}.pkl', protocol=4)
    #         hund_pages_docs = []
    #     else:
    #         print(f"{page} pages scraped: {round(page/(end-start)*100, 4)}%, {now_timestamp-start_timestamp} passed.")
    #         docs_ = bot.get_document_srl_per_page({"page":page, "m":1})
    #         hund_pages_docs.append(docs_)

    # print(bot._get_document_srl_per_page(13))
    # print(datetime.now() - start_timestamp)
