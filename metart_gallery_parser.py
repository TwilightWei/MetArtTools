import argparse
import arrow
import contextlib
import os
import pymysql
import sqlite3
import ssl
import time

import requests
import simplejson as json

from math import ceil, floor

class MetartGalleryParser():
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


    def get_total_gallery_count(self, gallery_type):
        if gallery_type == self.GALLERY:
            url = f'https://www.{self.site_name}.com/api/galleries?galleryType=GALLERY&first=60&page=1&staffSelectionHead=false&tabId=0&order=DATE&direction=ASC&type=GALLERY'
        else:
            url = f'https://www.{self.site_name}.com/api/movies?galleryType=MOVIE&first=60&page=1&staffSelectionHead=false&tabId=0&order=DATE&direction=ASC&type=MOVIE'
        rsp = self.get_response_json(url)
        total_gallery_count = rsp['total']
        return total_gallery_count


    def get_page_data_list(self, gallery_type, page):
        if gallery_type == self.GALLERY:
            url = f'https://www.{self.site_name}.com/api/galleries?galleryType=GALLERY&first=60&page={str(page)}&staffSelectionHead=false&tabId=0&order=DATE&direction=ASC&type=GALLERY'
        else:
            url = f'https://www.{self.site_name}.com/api/movies?galleryType=MOVIE&first=60&page={str(page)}&staffSelectionHead=false&tabId=0&order=DATE&direction=ASC&type=MOVIE'
        rsp = self.get_response_json(url)
        page_datas = rsp['galleries']
        return page_datas               


    def get_gallery_data(self, gallery_type, url_gallery_name, url_date):
        if gallery_type == self.GALLERY:
            url = f'https://www.{self.site_name}.com/api/gallery?name={url_gallery_name}&date={url_date}'
        else:
            url = f'https://www.{self.site_name}.com/api/movie?name={url_gallery_name}&date={url_date}'
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


    def update_gallery_data(self, gallery_type, gallery_data):
        table_name = f'{self.site_name}_gallery'
        data = {
            'uuid': gallery_data['UUID'] or None,
            'cover_clean_image_path': gallery_data['coverCleanImagePath'] or None,
            'cover_image_path': gallery_data['coverImagePath'] or None,
            'description': gallery_data['description'] or None,
            'meta_description': gallery_data['metaDescription'] or None,
            'name': gallery_data['name'] or None,
            'path': gallery_data['path'] or None,
            'published_at': arrow.get(gallery_data['publishedAt']).format('YYYY-MM-DD') or None,
            'site_uuid': gallery_data['siteUUID'] or None,
            'thumbnail_cover_path': gallery_data['thumbnailCoverPath'] or None,
            'type': gallery_data['type'] or None
        }
        if gallery_type == self.GALLERY:
            data.update({
                'original_uuid': gallery_data['originalUUID'] or None,
            })
        elif gallery_type == self.MOVIE:
            data.update({
                'splash_image_path': gallery_data['thumbnailCoverPath'] or None,
            })
        self.replace_data(table_name=table_name, data=data)

    
    def update_model_gallery_relation(self, gallery_data):
        table_name = f'{self.site_name}_model_gallery'
        gallery_uuid = gallery_data['UUID']
        models = gallery_data['models']
        for model in models:
            data = {
                'model_uuid': model['UUID'],
                'gallery_uuid': gallery_uuid
            }
            self.replace_data(table_name=table_name, data=data)


    def run(self):
        for gallery_type in [self.MOVIE, self.GALLERY]:
            total_gallery_count = self.get_total_gallery_count(gallery_type=gallery_type)
            if total_gallery_count <=0:
                print(f'No {self.GALLERY} gallery in this site')
                continue
            total_gallery_page_count = ceil(total_gallery_count/60)
            for page in range(total_gallery_page_count, 0, -1):
                print(page)
                page_datas = self.get_page_data_list(gallery_type=gallery_type, page=page)
                if not page_datas:
                    print(f'No {self.GALLERY} gallery in this page')
                    continue
                
                for page_data in page_datas:
                    connection = self.get_db_connection()
                    uuid = page_data['UUID']
                    table_name = f'{self.site_name}_gallery'
                    qry = f'''
                        SELECT COUNT(*) AS count
                        FROM {table_name}
                        WHERE `uuid`='{uuid}'
                    '''
                    with connection.cursor() as cursor:
                        cursor.execute(qry)
                    results = cursor.fetchall()
                    if results[0][0] > 0:
                        continue

                    url_gallery_name = page_data['name'].upper().translate({ord(i): '_' for i in ' \'<>():"/\\|!?*&,+%#'})
                    print(url_gallery_name)
                    url_date = page_data['path'].split('/')[-2]
                    gallery_data = self.get_gallery_data(gallery_type=gallery_type, url_gallery_name=url_gallery_name, url_date=url_date)
                    self.update_gallery_data(gallery_type=gallery_type, gallery_data=gallery_data)
                    self.update_model_gallery_relation(gallery_data=gallery_data)
                    

def parse_arge():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', '--site_name', type=str,
                        required=True, help='site name')
    parser.add_argument('-i', '--ini', type=str,
                        required=True, help='path of credential')
    args = parser.parse_args()
    return args

# python metart_gallery_parser.py -s metartx -i secrets\secret.ini
# python metart_gallery_parser.py -s metart -i secrets\secret.ini
if __name__=="__main__":
    args = parse_arge()

    MetartGalleryParser(
        site_name=args.site_name,
        ini=args.ini
    ).run()