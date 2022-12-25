import argparse
import arrow
import contextlib
import os
import pymysql
import sqlite3
import ssl
import time
import urllib.parse as parse 

import requests
import simplejson as json

from math import ceil, floor

class MetartModelParser():
    # Gallery types
    GALLERY = 'gallery'
    MOVIE = 'movie'

    def __init__(self, site_name, ini):
        self.site_name = site_name
        self.config = json.load(open(os.path.join(ini)))
        ssl._create_default_https_context = ssl._create_unverified_context
    

    def get_response_json(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        while(True):
            try:
                rsp = requests.get(url, headers=headers)
                break
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                print('Issue request data...' + str(e))
                time.sleep(10)
        return rsp.json()


    def get_total_model_count(self):
        url = f'https://www.{self.site_name}.com/api/models?first=40&page=1&order=DATE&direction=ASC'
        rsp = self.get_response_json(url)
        total_gallery_count = rsp['total']
        return total_gallery_count


    def get_page_data_list(self, page):
        url = f'https://www.{self.site_name}.com/api/models?first=40&page={str(page)}&order=DATE&direction=ASC'
        rsp = self.get_response_json(url)
        page_datas = rsp['models']
        return page_datas               


    def get_model_data(self, url_model_name):
        url = f'https://www.{self.site_name}.com/api/model?name={url_model_name}&order=DATE&direction=DESC'
        rsp = self.get_response_json(url)
        return rsp


    def get_db_connection(self):
        db_meta = self.config['mysql']
        connection = pymysql.connect(
            host=db_meta['host'],
            port=3306,
            user=db_meta['username'],
            password=db_meta['password'],
            charset='utf8mb4',
            database='metart'
        )
        return connection


    def replace_data(self, table_name, data):
        connection = self.get_db_connection()
        table_name = f'{table_name}'
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        qry = f'''
            REPLACE INTO {table_name} ( {columns} )
            VALUES ( {placeholders} )
        '''
        with connection.cursor() as cursor:
            cursor.execute(qry, list(data.values()))
        connection.commit()
        connection.close()


    def update_model_data(self, model_data):
        table_name = f'{self.site_name}_model'
        chest_size, waist_size, waist_size = None, None, None
        if model_data['size']:
            chest_size, waist_size, waist_size = (model_data['size'].split('/') + [None] * 3)[:3]
        data = {
            'uuid': model_data['UUID'] or None,
            'biography': model_data['biography'] or None,
            'breasts': model_data['breasts'] or None,
            'debut_month': arrow.get(f'''{model_data['debutYear']}-{model_data['debutMonth']}''', 'YYYY-MMMM').format('YYYY-MM-DD') or None,
            'global_uuid': model_data['globalUUID'] or None,
            'hair': model_data['hair'] or None,
            'headshot_image_path': model_data['headshotImagePath'] or None,
            'height': model_data['height'] or None,
            'name': model_data['name'] or None,
            'path': model_data['path'] or None,
            'site_uuid': model_data['siteUUID'] or None,
            'chest_size': chest_size or None,
            'waist_size': waist_size or None,
            'hip_size': waist_size or None,
            'top_rank': model_data['topRank'] or None,
            'weight': model_data['weight'] or None,
            'pubic_hair': model_data['pubicHair'] or None,
            'publish_age': model_data['publishAge'] or None,
            'ethnicity': model_data['ethnicity'] or None,
            'eyes': model_data['eyes'] or None,
            'gender': model_data['gender'] or None
        }
        self.replace_data(table_name=table_name, data=data)


    def run(self):
        total_model_count = self.get_total_model_count()
        if total_model_count <=0:
            print(f'No {self.GALLERY} model in this site')
    
        total_model_page_count = 4#ceil(total_model_count/40)
        for page in range(total_model_page_count, 0, -1):
            print(page)
            page_datas = self.get_page_data_list(page=page)
            if not page_datas:
                print(f'No {self.GALLERY} gallery in this page')
                continue
            
            for page_data in page_datas:
                url_model_name = parse.quote(page_data['name'])
                print(url_model_name)
                model_data = self.get_model_data(url_model_name=url_model_name)
                self.update_model_data(model_data=model_data)


def parse_arge():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', '--site_name', type=str,
                        required=True, help='site name')
    parser.add_argument('-i', '--ini', type=str,
                        required=True, help='path of credential')
    args = parser.parse_args()
    return args

# python metart_model_parser.py -s metartx -i secrets\secret.ini
# python metart_model_parser.py -s metart -i secrets\secret.ini
if __name__=="__main__":
    args = parse_arge()

    MetartModelParser(
        site_name=args.site_name,
        ini=args.ini
    ).run()