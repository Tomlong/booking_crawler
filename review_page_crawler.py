import os
import re
import uuid
import time
import json
import logging
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from settings import BOOKING_URL, HOT_CITY_URL, HTML_PATH

logger = logging.getLogger(__name__)
logger.setLevel('NOTSET')

URLS_PATH = './city_urls.json'

def get_city_urls(city_soup):
    city_urls = []
    problem_urls = []
    logger.info('Get city urls....')
    if os.path.exists(URLS_PATH):
        with open(URLS_PATH, 'r') as f:
            urls_json = json.load(f)
            city_urls = urls_json['urls']
            return city_urls
    print('Get city urls....')
    for city_item in tqdm(city_soup.findAll(class_ = 'block_header')):
        href = city_item.find('a')['href']
        city_page_html = requests.get(f'{BOOKING_URL}{href}')
        city_page_soup = BeautifulSoup(city_page_html.text, 'html.parser')
        try:
            city_page_href = city_page_soup.find("a", 
                            {"class": 
                             ["lp-bui-section bui-spacer--largest",
                              "bui-button bui-button--wide bui-button--primary"]
                            })['href']
            city_urls.append(f'{BOOKING_URL}{city_page_href}')
        except: 
            problem_urls.append(f'{BOOKING_URL}{href}')
        time.sleep(0.1)
    logger.info('Finish getting city urls')
    print('Finish getting city urls')
    return city_urls

def generate_id(hotel_ids):
    while(True):
        uid = str(uuid.uuid1())
        uid = uid.lower()
        uid = uid.replace('-','')
        uid = uid[:10]
        if uid not in hotel_ids:
            return uid

def get_exist_ids():
    ids = []
    for name in os.listdir('./hotel_html/'):
        if name.startswith('.') is False:
            ids.append(name)
    return ids

def crawl_review_page(url, hotel_id):
    page = 1
    path = f'{HTML_PATH}/{str(hotel_id)}'
    if not os.path.exists(path):
        os.makedirs(path)
    while(True):
        html = requests.get(url)
        soup = BeautifulSoup(html.text, 'html.parser')
        
        file = open(f'{path}/page{str(page)}.html', 'w')
        file.writelines(html.text)
        file.close()
        
        next_page = soup.find(True, {"rel": "next"})
        if next_page is not None:
            next_page_link = next_page['href']
            url = next_page_link
        else:
            break
        page += 1

def crawl_city(city_url):
    hotel_ids = get_exist_ids()
    offset = 0
    max_offset = None
    while(True):
        now_url = city_url + f";offset={str(offset)}"
        now_html =  requests.get(now_url)
        now_soup = BeautifulSoup(now_html.text, 'html.parser')
        # Get nums of hotels
        if max_offset is None:
            pattern = re.compile(r"availableHotels:\s+\'[0-9]+\'")
            all_script = now_soup.findAll("script", {"src": False})
            for script in all_script:
                match = pattern.search(str(script))
                if match:
                    max_offset = int(match.group(0).split(':')[1].strip().replace('\'', ''))
        for url in now_soup.findAll(class_ = "hotel_name_link url"):
            hotel_id = generate_id(hotel_ids)
            hotel_ids.append(hotel_id)
            hotel_url = url['href'].strip().split('/')[-1]
            now_review_page_url = f'{BOOKING_URL}/reviews/my/hotel/{hotel_url}'
            crawl_review_page(now_review_page_url, hotel_id)

        offset += 15
        if offset >= max_offset:
            break
        
        time.sleep(0.05)

def start_crawl(city_soup):
    if not os.path.exists(HTML_PATH):
        os.makedirs(HTML_PATH)

    city_urls = get_city_urls(city_soup)
    for city_url in tqdm(city_urls):
        crawl_city(city_url)
            

if __name__ == "__main__":
    city_html =  requests.get(HOT_CITY_URL)
    city_soup = BeautifulSoup(city_html.text, 'html.parser')
    start_crawl(city_soup)