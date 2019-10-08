import os
import time
import json
import gridfs
import pymongo
import logging
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
from settings import MONGO_URI, CRAWLER_DB_NAME, PARSER_DB_NAME

logger = logging.getLogger(__name__)


def parse_review(html, hotel_id, parser_db):
    parser_data_db = parser_db.data
    soup = BeautifulSoup(html, 'html.parser')

    # hotel stars 
    find_stars = soup.find(True, {"class": "bk-icon-stars"})
    if find_stars is not None: 
        stars = find_stars.text.strip()
    else:
        stars = ''
    # hotel name
    try:
        hotel_name = soup.find(True, {"class": "item hotel_name"}).a.text.strip()
        logger.info(f'hotel name: {hotel_name}')
    except:
        hotel_name = ''
    for review_box in soup.findAll("li", {"class": "review_item clearfix "}):
        # date
        try:
            date = review_box.find(True, {"class": "review_item_date"}).text.strip()
        except:
            date = ''

        # name
        try:
            name = review_box.find(True, {"class": "reviewer_name"}).text.strip()
        except:
            name = ''

        # reviewer country
        try:
            reviewer_coutry = review_box.find(True, {"class": "reviewer_country"}).text.strip()
        except:
            reviewer_coutry = ''

        # reviewer reviews count 
        try:
            reviews_count = review_box.find(True, {"class": "review_item_user_review_count"}).text.strip().split('条评语')[0]
        except:
            reviews_count = ''

        # staydate
        try:
            staydate = review_box.find(True, {"class": "review_staydate"}).text.strip()
        except:
            staydate = ''

        # tags
        try:
            tags = []
            for tag_item in review_box.findAll(True, {"class": "review_info_tag"}):
                tag = tag_item.text.strip()
                tag = tag.replace('•', '').strip()
                tags.append(tag)
            review_tags = ','.join(tags)
        except:
            review_tags = ''

        # Important part
        try:
            # score
            score = review_box.find(True, {"class": "review-score-badge"}).text.strip()

            # review_pos
            review_pos_soup = review_box.find(True, {"class": "review_pos"})
            if review_pos_soup is not None:
                review_pos = review_pos_soup.text.strip()
            else:
                review_pos = ""

            # review_neg
            review_neg_soup = review_box.find(True, {"class": "review_neg"})
            if review_neg_soup is not None:
                review_neg = review_neg_soup.text.strip()
            else:
                review_neg = ""

            parser_data_db.insert_one({
                'hotel_id': hotel_id,
                'hotel_name': hotel_name,
                'stars': stars,
                'date': date,
                'name': name,
                'reviewer_coutry': reviewer_coutry,
                'reviews_count': reviews_count,
                'score': score,
                'review_pos': review_pos,
                'review_neg': review_neg,
                'staydate': staydate,
                'tags': review_tags,
            })

        except Exception as e:
            logger.info(e)

def start_parse(mongo_client):
    crawler_db = mongo_client[CRAWLER_DB_NAME]
    parser_db = mongo_client[PARSER_DB_NAME]
    parser_list_db = parser_db.parser_list

    fs = gridfs.GridFS(crawler_db)
    while(True):
        job = parser_list_db.find_one_and_update({'status': 'waiting'},{'$set': {'status': 'ready'}})
        if job:
            hotel_id = job['hotel_id']
            logger.info(f'Parse {hotel_id}')
            dataset_id = job['id']
            stream = fs.get(dataset_id)
            html = stream.read()

            parse_review(html, hotel_id, parser_db)
        else:
            logger.info('Waiting for new job...')
            time.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mongo_client = pymongo.MongoClient(MONGO_URI)

    try:
        mongo_client.server_info()
        logger.info('MongoDB connect success')
    except:
        logger.warning('MongoDB is not connected')
        exit()

    start_parse(mongo_client)
