#from https://github.com/Hsankesara/medium-scrapper/blob/master/scrap.py

import urllib3
from bs4 import BeautifulSoup
import requests
import os
import csv
import unicodedata
import pandas as pd
from datetime import timedelta, date

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
            paras = soup.findAll('p')
            text = ''
            nxt_line = '\n'
            for para in paras:
                text += unicodedata.normalize('NFKD',para.get_text()) + nxt_line
            article['text'] = text
            articles.append(article)
        except KeyboardInterrupt:
            print('Exiting')
            os._exit(status = 0)
        except Exception as e:
            # for exceptions caused due to change of format on that page
            print("Exception failed: {0}".format(e))
            continue
    return articles

def save_articles(articles, csv_file,  is_write = True):
    csv_columns = ['author', 'claps', 'reading_time', 'link', 'title', 'text']
    if is_write:
        with open(csv_file, 'w',encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='|')
            writer.writeheader()
            for data in articles:
                writer.writerow(data)
            csvfile.close()
    else:
        with open(csv_file, 'a+',encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns,  delimiter='|')
            for data in articles:
                writer.writerow(data)
            csvfile.close()

def isEndOfYear(date):
    if date.month==12:
        if date.day==31:
            return True
    else:
        return False

def main():
    is_write = True
    #tags = ['AI','Technology','Machine Learning','Artificial Intelligence','Data Science','Deep Learning','Visualization','programming','Neural Networks','Big Data','Python','Data','Analytics','Tech','Tensorflow','Pytorch','NLP','Computer Vision']
    tags = ['AI','Data']
    file_name = "data/raw_medium_articles"
    #if len(file_name.split('.')) == 1:
    #    file_name += '.csv'
    start_date = date(2017, 1, 1)
    end_date = date(2019, 12, 31)
    for single_date in daterange(start_date, date.today()):
        dfs = []
        for tag in tags:
            single_date_str = single_date.strftime("%Y/%m/%d")
            links = get_links(tag, single_date_str)
            articles = get_article(links)
            dfs.append(pd.DataFrame(articles))
            is_write = False
            #caching
            if isEndOfYear(single_date):
                df = pd.concat(dfs)
                df = df.drop_duplicates()
                df.to_pickle(file_name+'_'+str(single_date.year)+'.pkl')
                print('EOY')
    #articles = pd.read_csv(file_name, file_name, delimiter='|')
    #articles = articles.drop_duplicates()
    #articles.to_csv(file_name, sep='|', index=False)
if __name__=='__main__':
    main()