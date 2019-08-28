import os
import json
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from settings import HTML_PATH, PARSED_PATH


def parse_review(path, hotel_id):
    data_json_list = []
    for file_name in os.listdir(path):
        if file_name.endswith('html'):
            with open(f'{path}/{file_name}', 'r') as html_file:
                soup = BeautifulSoup(html_file, 'html.parser')
        else:
            continue

        # hotel stars 
        find_stars = soup.find(True, {"class": "bk-icon-stars"})
        if find_stars is not None: 
            stars = find_stars.text.strip()
        else:
            stars = ''
        # hotel name
        try:
            hotel_name = soup.find(True, {"class": "item hotel_name"}).a.text.strip()
        except:
            break

        for review_box in soup.findAll("li", {"class": "review_item clearfix "}):
            try:
                # date
                date = review_box.find(True, {"class": "review_item_date"}).text.strip()
                # name
                name = review_box.find(True, {"class": "reviewer_name"}).text.strip()
                # reviewer country
                reviewer_coutry = review_box.find(True, {"class": "reviewer_country"}).text.strip()
                # reviewer reviews count 
                reviews_count = review_box.find(True, {"class": "review_item_user_review_count"}).text.strip().split('条评语')[0]
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
                # staydate
                staydate = review_box.find(True, {"class": "review_staydate"}).text.strip()
                # tags
                tags = []
                for tag_item in review_box.findAll(True, {"class": "review_info_tag"}):
                    tag = tag_item.text.strip()
                    tag = tag.replace('•', '').strip()
                    tags.append(tag)
                review_tags = ','.join(tags)
                data_json_list.append({
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
            except:
                pass
    return data_json_list

def start_parse():
    for file_name in tqdm(os.listdir(HTML_PATH)):
        if os.path.isdir(f'{HTML_PATH}/{file_name}'):
            hotel_id = file_name
            now_html_path = f'{HTML_PATH}/{file_name}'
        else:
            continue
        
        parse_data = parse_review(now_html_path, hotel_id)
        if len(parse_data) != 0:
            data_df = pd.DataFrame(parse_data)
            data_df.to_csv(f'{PARSED_PATH}/{hotel_id}.csv', index=False)


if __name__ == "__main__":
    if os.path.exists(HTML_PATH) is False:
        print('There is no html need to parse')
        exit()
    if os.path.exists(PARSED_PATH) is False:
        os.makedirs(PARSED_PATH)
    start_parse()
    
            