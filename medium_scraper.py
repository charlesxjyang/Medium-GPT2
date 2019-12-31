#from https://github.com/Hsankesara/medium-scrapper/blob/master/scrap.py
import urllib3
from bs4 import BeautifulSoup
import requests
import os
import csv
import unicodedata
import pandas as pd
from datetime import timedelta, date
import multiprocessing as mp
import numpy as np
from langdetect import detect

n_cpus = 8

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
            break
    return num_claps

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
            print("Exception failed")
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
    is_write = True
    tags = ['AI','Technology','Machine Learning','Artificial Intelligence','Data Science','Deep Learning','Visualization','programming','Neural Networks','Big Data','Python','Data','Analytics','Tech','Tensorflow','Pytorch','NLP','Computer Vision']
    file_name = "data/raw_medium_articles"
    years = [2017,2018,2019]
    for year in years:
        idx = []
        start_date = date(year,1,1)
        end_date = get_last_day_in_year(start_date)
        for tag in tags:
            for a in daterange(start_date,end_date):
                idx.append([tag,a])
        with mp.Pool(n_cpus) as pool:
            articles = pool.starmap(get_links_articles, [(tag,single_date) for tag,single_date in idx])
        articles = [item for sublist in articles for item in sublist]
        df = pd.DataFrame(articles)
        df = df.drop_duplicates()
        df.to_pickle(file_name+'_'+str(year)+'.pkl')
        
if __name__=='__main__':
    main()
