from typing import List

import logging
import pandas as pd
from requests import get
from lxml.etree import HTML
from csv_util import *
import csv

from time import sleep, localtime

_get = get
logger = logging.Logger("main")
handler = logging.FileHandler(f"logs/"
                              f"{localtime().tm_year}-"
                              f"{localtime().tm_mon}-"
                              f"{localtime().tm_mday}--"
                              f"{localtime().tm_hour}h-"
                              f"{localtime().tm_min}m-"
                              f"{localtime().tm_sec}s.log",
                              encoding="utf-8")
formatter = logging.Formatter("[%(levelname)-5.5s][%(funcName)-7.7s][%(lineno)3.3d行]-%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logger.addHandler(handler)
logger.addHandler(console)

TIMEOUT = 10
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.50"}
OUTFILE = f"out/{localtime().tm_year}" \
          f"-{localtime().tm_mon}" \
          f"-{localtime().tm_mday}" \
          f"--{localtime().tm_hour}h" \
          f"-{localtime().tm_min}m" \
          f"-{localtime().tm_sec}s.out.csv"


# 包装get方法
def get(*args, **kwargs):
    retry = 0
    while retry <= 3:
        try:
            if TIMEOUT:
                kwargs["timeout"] = TIMEOUT
            if "headers" not in kwargs:
                kwargs["headers"] = HEADERS
            logger.debug("[获取网页] {}".format(args[0]))
            logger.debug(f"[使用] headers [{kwargs['headers']}]  -  timeout [{kwargs['timeout']}]")
            return _get(*args, **kwargs)
        except Exception as e:
            logger.error("[获取网页失败]: ")
            logger.exception(e)
            retry += 1
    return None


class OutFile:
    def __init__(self, filename: str):
        self.f = open(filename, 'w', newline='', encoding='utf-8')
        self.writer = None

    def save(self, item: dict):
        if not self.writer:
            self.writer = csv.DictWriter(self.f, fieldnames=list(item.keys()), lineterminator='\n')
            self.writer.writeheader()
        self.writer.writerow(item)


def get_detailed_info(novel_url) -> dict:
    retry = 0
    while True:
        resp = get(novel_url, headers=HEADERS)
        retry += 1
        if resp.status_code == 200:
            break
        sleep(0.5)
        if retry >= 10:
            raise Exception
    resp.encoding = resp.apparent_encoding
    html = HTML(resp.text)

    book_info = html.xpath("/html/body/table[@id='oneboolt']/tbody/tr")[-1].xpath("./td/div/span")

    item = {}

    # 总评论数
    review_count = book_info[1].text

    # 总收藏数
    collected_count = book_info[2].text

    # 营养液数
    gift_count = book_info[3].text

    # 把数据添加到item
    item["review_count"] = review_count
    item["collected_count"] = collected_count
    item["gift_count"] = gift_count
    return item


def _spider(url: str):
    outfile = OutFile(OUTFILE)
    resp = get(url, headers=HEADERS)
    resp.encoding = resp.apparent_encoding
    base_url = url[:22]
    if not resp:
        logger.info(f"爬行到底了")
        return None
    html = HTML(resp.text)
    novels = []

    tbody = html.xpath('/html/body/table[3]/tbody')[0][1:]
    for tr in tbody:
        item = dict()
        item["index"] = tr[0].text
        item["author"] = tr[1].xpath("a")[0].text
        item["name"] = tr[2].xpath("a")[0].text
        item["tag"] = tr[3].text.strip()
        item["style"] = tr[4].text.strip()
        item["status"] = tr[5].text.strip()
        if not item["status"]:
            item["status"] = tr[5].xpath('font')[0].text
        item["word_count"] = tr[6].text.strip()

        novel_url = base_url + tr[2].xpath("a")[0].attrib['href']
        try:
            detailed_info = get_detailed_info(novel_url).items()
        except Exception as e:
            logger.error(f"[数据抓取错误] {item['name']}")
            logger.exception(e)
            continue

        for key, value in detailed_info:
            item[key] = value

        item["score"] = tr[7].text.strip().replace(',', '')
        item["pub_time"] = tr[8].text.strip()

        novels.append(item)
        outfile.save(item)

        logger.info("[导出数据] {}-{}-{}-{} ...".format(
            item["index"],
            item["name"],
            item["author"],
            item["pub_time"]
        ))

    return novels


def all_spider(url_list: List[str], page: int):
    outfile = OutFile(OUTFILE)
    novels = []
    for url in url_list:
        first_url = url
        logger.info(f"Url Now:{url}")
        sleep(1)
        for p in range(1, page + 1):
            logger.info(f"Page Now:{p}")
            sleep(0.5)
            url = first_url + str(p)
            resp = get(url, headers=HEADERS)
            resp.encoding = resp.apparent_encoding
            base_url = url[:22]
            if not resp:
                logger.info(f"爬行到底了")
                return None
            html = HTML(resp.text)
            tbody = html.xpath("/html/body/div[@class='tc']/table/tbody")[0][1:]
            for tr in tbody:
                item = dict()
                item["author"] = tr[0].xpath("a")[0].text
                item["name"] = tr[1].xpath("a")[0].text
                item["tag"] = tr[2].text.strip()
                item["style"] = tr[3].text.strip()
                item["status"] = tr[4].text.strip()
                if not item["status"]:
                    item["status"] = tr[4].xpath('font')[0].text
                item["word_count"] = tr[5].text.strip()

                novel_url = base_url + tr[1].xpath("a")[0].attrib['href']
                try:
                    detailed_info = get_detailed_info(novel_url).items()
                except Exception as e:
                    logger.error(f"[数据抓取错误] {item['name']}")
                    logger.exception(e)
                    continue

                for key, value in detailed_info:
                    item[key] = value

                item["score"] = tr[6].text.strip().replace(',', '')
                item["pub_time"] = tr[7].text.strip()

                novels.append(item)
                outfile.save(item)

                logger.info("[导出数据] {}-{}-{} ...".format(
                    item["name"],
                    item["author"],
                    item["pub_time"]
                ))
    return novels


# def remove_duplicate(path: str):
#     logger.info(f"Try to remove duplicate lines in {path}...")
#     try:
#         frame = pd.read_csv(path)
#         data = frame.drop_duplicates(subset=['name'])
#         data.to_csv(path, encoding='utf8', index=False)
#     except IOError:
#         logger.error("Cannot remove duplicate in csv, please check config")


def main():
    # _spider('https://www.jjwxc.net/topten.php?orderstr=5&t=0')
    url_list = ['https://www.jjwxc.net/bookbase_slave.php?orderstr=2&t=2&page=',
                'https://www.jjwxc.net/bookbase_slave.php?booktype=vip&t=2&page=',
                'https://www.jjwxc.net/bookbase_slave.php?booktype=package&t=2&page=',
                'https://www.jjwxc.net/bookbase_slave.php?booktype=vip&orderstr=2&t=2&page=',
                'https://www.jjwxc.net/bookbase_slave.php?booktype=sp&t=2&page=',
                'https://www.jjwxc.net/bookbase_slave.php?booktype=scriptures&t=2&page=',
                'https://www.jjwxc.net/bookbase_slave.php?booktype=free&t=2'
                ]
    all_spider(url_list, 8)
    write_csv(remove_duplicate(read_file(OUTFILE)), OUTFILE)


if __name__ == '__main__':
    main()
