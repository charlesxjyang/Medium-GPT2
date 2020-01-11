#from https://github.com/Hsankesara/medium-scrapper/blob/master/scrap.py
import urllib3
from bs4 import BeautifulSoup
import requests
import os
import csv
import unicodedata
import pandas as pd
from datetime import timedelta, date
import datetime
import multiprocessing as mp
import numpy as np
from langdetect import detect

n_cpus = 12
clap_threshold = 5

if n_cpus == -1 or n_cpus>mp.cpu_count():
    n_cpus = mp.cpu_count()

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_links(tag, date):
    url = 'https://medium.com/tag/' + tag+'/archive/'+date
    data = requests.get(url)
    soup = BeautifulSoup(data.content, 'html.parser')
    articles = soup.findAll('div', {"class": "postArticle-readMore"})
    links = []
    for i in articles:
        links.append(i.a.get('href'))
    return links

def get_claps(soup):
    claps  = soup.findAll('button')
    for string in claps:
        if 'claps' in string.get_text():
            num_claps = string.get_text().split(' ')[0]
            if 'K' in num_claps:
                num_claps = int(float(num_claps[:-1])*1000)
            else:
                num_claps = int(num_claps)
            return num_claps
    return -1

def get_article(links):
    articles = []
    for link in links:
        try:
            article = {}
            data = requests.get(link)
            soup = BeautifulSoup(data.content, 'html.parser')
            title = soup.findAll('title')[0]
            title = title.get_text()
            author = soup.findAll('meta', {"name": "author"})[0]
            author = author.get('content')
            article['author'] = unicodedata.normalize('NFKD', author)
            article['link'] = link
            article['title'] = unicodedata.normalize('NFKD', title)
            article['claps'] = get_claps(soup)
            if article['claps']<clap_threshold:
                continue
            paras = soup.findAll('p')
            text = ''
            nxt_line = '\n'
            for para in paras:
                text += unicodedata.normalize('NFKD',para.get_text()) + nxt_line
            if detect(text)!='en':
                continue
            else:
                article['text'] = text
                articles.append(article)
        except KeyboardInterrupt:
            print('Exiting')
            os._exit(status = 0)
        except Exception as e:
            # for exceptions caused due to change of format on that page
            if 'link' in article.keys():
                print(article['link'])
            print(e)
            continue
    return articles

def get_links_articles(tag,single_date):
    single_date_str = single_date.strftime("%Y/%m/%d")
    links = get_links(tag, single_date_str)
    articles = get_article(links)
    return articles
            
def isEndOfYear(date):
    if date.month==12:
        if date.day==31:
            return True
    else:
        return False
def get_last_day_in_year(single_date):
    return date(single_date.year,12,31)
    
def main():
    tags = ['Towards Data Science','Reinforcement Learning','Coding','Keras','Optimization','Random Forest','Decision Tree','Science','Kaggle','AI','Technology','Machine Learning','Artificial Intelligence','Data Science','Deep Learning','Visualization','programming','Neural Networks','Big Data','Python','Data','Analytics','Tech','Tensorflow','Pytorch','NLP','Computer Vision','Technology']
    file_name = "data/raw_medium_articles"
    years = [2015,2016,2017,2018,2019,2020]
    for year in years:
        idx = []
        start_date = date(year,1,1)
        end_date = get_last_day_in_year(start_date)
        current_datetime = datetime.datetime.now()
        current_date = date(current_datetime.year,current_datetime.month,current_datetime.day)
        if end_date>current_date:
            end_date = current_date
        for tag in tags:
            for a in daterange(start_date,end_date):
                idx.append([tag,a])
        with mp.Pool(n_cpus) as pool:
            articles = pool.starmap(get_links_articles, [(tag,single_date) for tag,single_date in idx])
        articles = [item for sublist in articles for item in sublist]
        df = pd.DataFrame(articles)
        if len(df)==0:
            if len(articles)==0:
                print("PROBLEM: BOTH ARE ")
            else:
                print("PROBLEM")
            print(year)
            break
        #only keep rows with duplicates
        #df = df[df.duplicates(keep=False)]
        df = df.drop_duplicates()
        df.to_pickle(file_name+'_'+str(year)+'.pkl')
        print("YEAR: {0}".format(year))
if __name__=='__main__':
    main()
