import os
BOOKING_URL = 'https://www.booking.com/'
HOT_CITY_URL = 'https://www.booking.com/city.zh-cn.html'

MONGO_URI = os.getenv('MONGODB')
CRAWLER_DB_NAME = os.getenv('CRAWLER_DB_NAME', 'crawler')
PARSER_DB_NAME = os.getenv('PARSER_DB_NAME', 'parser')