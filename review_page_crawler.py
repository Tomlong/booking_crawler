import os
import re
import uuid
import time
import json
import gridfs
import pymongo
import logging
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from settings import BOOKING_URL, HOT_CITY_URL, MONGO_URI, CRAWLER_DB_NAME, PARSER_DB_NAME

logger = logging.getLogger(__name__)


def get_city_urls(city_soup):
    city_urls = []
    logger.info('Start getting city urls....')

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
            pass
        time.sleep(0.05)
    logger.info('Finish getting city urls')
    return city_urls

def generate_id(hotel_ids_db):
    while(True):
        uid = str(uuid.uuid1())
        uid = uid.lower()
        uid = uid.replace('-','')
        uid = uid[:10]

        if not hotel_ids_db.find_one({'hotel_id': uid}):
            hotel_ids_db.insert_one({'hotel_id': uid})
            return uid

def crawl_review_page(url, hotel_id, fs, parser_list_db, review_page_id):
    page = 1

    while(True):
        uid = f'{hotel_id}_{page}'
        information = {
            'uid': uid,
        }
        html = requests.get(url)
        html.encoding = 'utf-8'
        soup = BeautifulSoup(html.text, 'html.parser')
        
        if not fs.find_one({'uid': uid}):
            obj_id = fs.put(html.content, **information)
            parser_list_db.insert({
                'id': obj_id,
                'review_page_id': review_page_id
                'hotel_id': hotel_id,
                'uid': uid,
                'status': 'waiting',
            })
        
        next_page = soup.find(True, {"rel": "next"})
        if next_page is not None:
            next_page_link = next_page['href']
            url = next_page_link
        else:
            break
        page += 1


def crawl_city(city_url, crawler_db, parser_db):

    fs = gridfs.GridFS(crawler_db)
    hotel_ids_db = crawler_db.hotel_ids
    review_pages_db = crawler_db.review_pages
    parser_list_db = parser_db.parser_list

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

        # Get hotel review page
        for url in now_soup.findAll(class_ = "hotel_name_link url"):
            hotel_id = generate_id(hotel_ids_db)
            hotel_url = url['href'].strip().split('/')[-1]
            now_review_page_url = f'{BOOKING_URL}/reviews/my/hotel/{hotel_url}'
            review_page_status = review_pages_db.find_one({'city_url': city_url, 'hotel_url': now_review_page_url})
            if review_page_status:
                if (review_page_status['crawled'] == 'waiting'):
                    review_page_id = review_page_status._id
                    crawl_review_page(now_review_page_url, hotel_id, fs, parser_list_db, review_page_id)
                    review_pages_db.find_one_and_update({'city_url': city_url, 'hotel_url': now_review_page_url},
                                                        {'$set': {'crawled': 'finish'}})
                    
            else:
                insert_data = {
                    'city_url': city_url,
                    'hotel_url': now_review_page_url,
                    'crawled': 'waiting',
                }
                review_page_id = review_pages_db.insert_one(insert_data).inserted_id
                crawl_review_page(now_review_page_url, hotel_id, fs, parser_list_db, review_page_id)
                review_pages_db.find_one_and_update({'city_url': city_url, 'hotel_url': now_review_page_url},
                                                    {'$set': {'crawled': 'finish'}})

        offset += 15
        if offset >= max_offset:
            break
        time.sleep(0.05)

def start_crawl(city_soup, crawler_db):
    crawler_db = mongo_client[CRAWLER_DB_NAME]
    parser_db = mongo_client[PARSER_DB_NAME]

    city_urls = get_city_urls(city_soup)
    city_urls_db = crawler_db.city_urls
    
    # insert new city urls
    logger.info('Update city urls...')
    for city_url in tqdm(city_urls):
        if city_urls_db.find_one({'url': city_url}):
            continue
        
        city_urls_db.insert_one({
            'url': city_url,
            'crawled': 'pending'
        })

    # crawl and update db
    logger.info('Update review page...')
    for city_url in tqdm(city_urls):
        # crawling pending urls
        city_urls_status = city_urls_db.find_one({'url': city_url})
        if city_urls_status:
            if (city_urls_status['crawled'] == 'pending') or (city_urls_status['crawled'] == 'crawling'):
                city_urls_db.find_one_and_update({'url': city_url}, {'$set': {'crawled': 'crawling'}})
                crawl_city(city_url, crawler_db, parser_db)
                # update finished city url
                city_urls_db.find_one_and_update({'url': city_url}, {'$set': {'crawled': 'finish'}})


if __name__ ==  "__main__":
    logging.basicConfig(level=logging.INFO)
    mongo_client = pymongo.MongoClient(MONGO_URI)

    try:
        mongo_client.server_info()
        
    except:
        logger.warning(f'MongoDB is not connected')
        exit()

    city_html = requests.get(HOT_CITY_URL)
    city_soup = BeautifulSoup(city_html.text, 'html.parser')
    start_crawl(city_soup, mongo_client)