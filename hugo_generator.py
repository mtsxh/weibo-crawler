#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import logging.config
import os
import re
import glob
import shutil
import warnings
import pandas as pd
import yaml
import argparse

from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep

import const
from util import csvutil
from util.dateutil import convert_to_days_ago
from util.notify import push_deer

warnings.filterwarnings("ignore")

# 如果日志文件夹不存在，则创建
logger = None
# if not os.path.isdir("log/"):
#     os.makedirs("log/")
# logging_path = os.path.split(os.path.realpath(__file__))[0] + os.sep + "logging.conf"
# logging.config.fileConfig(logging_path)
# logger = logging.getLogger("weibo")


userinfo = pd.read_excel(r".\weibo\weibolist.xlsx")

class Hugo_generator:
    def __init__(self, config):
        """Weibo类初始化"""
        self.post_dir = config.post_dir
        self.static_dir = config.static_dir
        self.data_dir = config.data_dir
        self.img_files = glob.glob(self.data_dir + os.sep + '**/*.jpg', recursive=True)
        self.video_files = glob.glob(self.data_dir + os.sep + '**/*.mp4', recursive=True)
        self.user_files = glob.glob(self.data_dir + os.sep + '**/users.csv', recursive=True)
        self.uid_files = glob.glob(self.data_dir + os.sep + '**/*.csv', recursive=True)
        self.userlist_file = config.userlist_file
        self.since_days = config.since_days

    def get_author_info(self, user):
        return {
            'original' : False,
            'author' : user['昵称'],
            'website' : user['主页'],
            'motto' : f"{user['认证信息']}<br/>{user['简介']}",
            'avatar' : user['头像'],
        }

    def get_post_content(self, user, group, post_date):
        uid = int(user['用户id'])
        uname = user['昵称']
        imgs_list = []
        video_list = []
        content = f'<h2>{uname}</h2>\n'
        for idx, row in group.iterrows():
            id = row['id'].replace('\t','')
            text = re.sub(r'\s\s+', ' ', row['正文'])
            # if text.find('_blank')>0:
            #     print(text)
            # text = re.sub(r'\<a\s+href\=\"[\w\:\/\.\d\?\=\:]+\"\s+target\=\"\_blank\"\s+rel\=\"noopener\"\>', '', text)
            content = content + f"{text}<br/>\n"
            #pattern = f'**/{post_date.replace("-","")}_{id}*.jpg'
            pattern_uid = f'{post_date.replace("-","")}_{id}'
            images = [f for f in self.img_files if f.find(pattern_uid)>=0]
            for uid_img_file in images:
            # for uid_img_file in glob.glob(pattern, recursive=True):
                filename = f'/images/wb_{uid}_{post_date}/{os.path.basename(uid_img_file)}'
                content = content + f'<img src="{filename}" width=320>'
                imgs_list.append((uid_img_file, filename))
                # shutil.copy(uid_img_file, os.path.join(hugo_images_dir, filename))
            content = content + f"  \n\n<br/><p><a href='https://m.weibo.cn/detail/{id}'>工具: {row['工具']}  点赞数: {row['点赞数']}  评论数: {row['评论数']} 转发数: {row['转发数']}</a></p><br/><hr/><br/>\n"
        # return markdownify.markdownify(content, heading_style="ATX")
        # return md(content, heading_style="ATX")
        return (content, imgs_list, video_list)
    # https://github.com/matthewwithanm/python-markdownify/blob/develop/markdownify/__init__.py


    def create_post(self, user_post_path, post_date, user, group):
        uid = int(user['用户id'])
        uname = user['昵称']
        category = userinfo[userinfo['id']==uid].iloc[0]['category']
        first = group.iloc[0]
        print(category)
        front_matter = {
            'title' : f'{uname}的weibo({len(group)}条)',
            'date' : post_date,
            # 'url' : first,
            'draft' : False,
            'isCJKLanguage' : True,
            'toc' : False,
            'tocNum' : True,
            'displayCopyright' : True,
            'share' : False,
            'dropCap' : False,
            'badge' : False,
            'categories' : [category, uname],
            'tags' : [uname],
        }

        front_matter.update(self.get_author_info(user))
        print(front_matter)
        content = '---\n' + yaml.dump(front_matter) + '\n---\n'
        post_content, imgs_list, video_list = self.get_post_content(user, group, post_date)
        content = content + post_content

        with open(user_post_path,'w',encoding='utf8') as f:
            f.write(content) 
        for (uid_img_file, filename) in imgs_list:
            file_path = os.path.join(self.static_dir+filename)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            shutil.copy(uid_img_file, file_path)
        for (uid_video_file, filename) in video_list:
            file_path = os.path.join(self.static_dir, filename)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            shutil.copy(uid_video_file, file_path)            
        

    def process_user(self, user : dict):
        # print(type(user), user)
        uid = user['用户id']
        # print(f"Id: {user['用户id']}, 昵称: {user['昵称']}")
        csv_files = [f for f in self.uid_files if uid in f]
        for uid_csv_file in csv_files:
            print(uid_csv_file)
            df = pd.read_csv(uid_csv_file, dtype=str)
            for col in df.columns:
                df[col] = df[col].astype(str)
            # print(df.info(verbose=True))
            df = df.drop_duplicates(subset=['bid'], keep='last')
            grouped = df.groupby('日期')
            for post_date_str, group in grouped:
                print(post_date_str)
                post_date = datetime.strptime(post_date_str, '%Y-%m-%d')
                if (datetime.now() - post_date).days < self.since_days:
                    # print(group)
                    user_post_path = os.path.join(self.post_dir, f'wb_{uid}_{post_date_str}.md')
                    if not os.path.exists(user_post_path):
                        self.create_post(user_post_path, post_date_str, user, group)

    def process(self):
        for users_file in self.user_files:
            print(users_file)
            df = pd.read_csv(users_file, dtype=str)
            for idx, user in df.iterrows():
                self.process_user(user.to_dict())


def main():
    global logger
    parser = argparse.ArgumentParser(description="weibo-crawler 爬取微博信息",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-p", "--post-dir", required=True, help="path for hugo post folder")
    parser.add_argument("-m", "--static-dir", required=True, help="path for hugo static folder")
    parser.add_argument("-l", "--log-dir", help="directory to save log")
    parser.add_argument("-d", "--data-dir", default=".", help="directory to get crawled data")
    parser.add_argument("-u", "--userlist-file", default="weibo/weibolist.xlsx", help="xlsx file for userinfo")
    parser.add_argument("-s", "--since-days", default=20, type=int, help="days of articles to process since now time")

    # parser.add_argument("--verbose", help="increase output verbosity", action="store_true")
    args = parser.parse_args()

    log_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], "log")
    if args.log_dir:
        log_dir = args.log_dir
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_all = Path(log_dir) / "all.log"
    log_file_err = Path(log_dir) / "error.log"
    logging_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "logging.conf")
    logging.config.fileConfig(logging_path, 
                              defaults={ 
                                "alllogfilename": log_file_all.as_posix(),
                                "errorlogfilename": log_file_err.as_posix(),
                              })
    logger = logging.getLogger("weibo")

    try:
        generator = Hugo_generator(args)
        generator.process()
    except Exception as e:
        if const.NOTIFY["NOTIFY"]:
            push_deer("Hugo generator运行出错，错误为{}".format(e))
        logger.exception(e)


if __name__ == "__main__":
    main()
