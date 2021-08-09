import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import random
import json
import os

files = os.listdir("D:\chosun")
latest = min([int(os.path.splitext(file)[0].split('_')[2]) for file in files])
start_time = datetime.now()
print(f"from : {latest}")

message_id = latest

while True:
    if message_id > 0:
        try:
            resp = requests.get(f'http://forum.chosun.com/message/messageView.forum?bbs_id=1010&message_id={message_id}')
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            post_date = " ".join(soup.select_one('#content > div.brd_view > div.brd_view_header > div.tit_box > div').text.split()[1:])
            post_cat = soup.select_one('#content > div.brd_view > div.brd_view_header > span').text
            post_title = soup.select_one('#content > div.brd_view > div.brd_view_header > div.tit_box').get_text().strip().split('\r\n\t\t\t\t\t')[0]
            post_text = soup.select_one('#content > div.brd_view > div.brd_view_body').text.strip().replace("\xa0", " ")

            json_ = {'id':message_id, 'time':post_date, 'cat':post_cat, 'title':post_title, 'text':post_text}

            with open(f"D:\chosun\chsun_{post_cat}_{message_id}.json", "w", encoding="UTF-8") as json_file:
                json.dump(json_, json_file, ensure_ascii=False, indent=4)
            
            if message_id % 1000 == 0:
                print(f"Success! Keep working - {message_id} : {datetime.now() - start_time}")


        except:
            print("no post - ", message_id)
            pass
    else:
        break
        
    # time.sleep(random.uniform(0,1))
    message_id -= 1