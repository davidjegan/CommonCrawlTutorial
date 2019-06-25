#Import packages
import requests
import json
from bs4 import BeautifulSoup
import codecs
import pandas as pd
import StringIO
import gzip
import csv
import re



df = pd.DataFrame()

#Access the domain
def search_domain(domain, index):
    print "[*] Accessing Domain {0} for the index {1}".format(domain,index)
    #"{0}{1:{2}}".format(delete, i, digits)
    record_list = []
    print "[*] Trying index %s" % index
    response = requests.get("http://index.commoncrawl.org/CC-MAIN-"+ index +"-index?url=" + domain + "&matchType=domain&output=json")
    if response.status_code == 200:
        records = response.content.splitlines()
        for record in records:
            if '/book/show/' in json.loads(record).get('url'):
                record_list.append(json.loads(record))
    print "[*] Found a total of %d hits." % len(record_list)
    record_list_df = pd.concat([pd.DataFrame([record_list[i]], columns=['id','urlkey','timestamp','mime','mime-detected','digest','offset','url','length','status','filename']) for i in range(len(record_list))],ignore_index=False)
    return record_list,record_list_df        


#Find external links
def extract_external_links(domain, html_content,link_list):
    parser = BeautifulSoup(html_content)
    links = parser.find_all("a")
    if links:
        for link in links:
            href = link.attrs.get("href")
            if href is not None:
                if domain not in href:
                    if href not in link_list and href.startswith("http"):
                        print "[*] Discovered external link: %s" % href
                        link_list.append(href)
    return link_list



#bs4 html parser
def parse_me(html_msg):
    subset_sentences_list = []
    sublist = []
    for i in html_msg:
        soup = BeautifulSoup(i, 'html.parser')
        soup.prettify()
        subset_sentences = ''
        for sentences in soup.find_all('p'):
            if(sentences.string not in [None, ' ', '', 'Advertisement','Welcome back. Just a moment while we sign you in to your Goodreads account.','None']):
                subset_sentences = subset_sentences + (sentences.string)  
        if(len(subset_sentences)>5):
            subset_sentences_list.append(subset_sentences)
    subset_sentences_list = map(lambda s: re.sub('\s+', ' ', s), subset_sentences_list)
    subset_sentences_list_df = pd.DataFrame({'data':subset_sentences_list})
    subset_sentences_list_df = subset_sentences_list_df.dropna()
    subset_sentences_list_df.to_csv('data/urldata_discovered.csv',sep='|', encoding='utf-8') 


#download data directly using offest and length
def download_page(record):
    offset, length = str(record['offset']), str(record['length'])
    offset_end = int(offset) + int(length) - 1
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    resp = requests.get(prefix + str(record['filename']), headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
    raw_data = StringIO.StringIO(resp.content)
    f = gzip.GzipFile(fileobj=raw_data)
    data = f.read()
    response = ""
    if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
        except:
            pass
    return response


#Search for links, download the data and extract external links
def crawler(domain, index_list):
    link_list   = []
    html_msg = []
    html_df = pd.DataFrame()
    cnt = 0
    record_list,record_list_df = search_domain(domain, index_list)
    #record_list_size = record_list[0:100] Change length from (0 to len(record_list))
    record_list_size = record_list[0:100]
    for record in record_list_size:
        html_content = download_page(record)
        print(cnt),
        cnt = cnt + 1
        if (len(html_content)) != 0:
                print "[*] Retrieved %d bytes for %s" % (len(html_content),record['url'])
                link_list = extract_external_links(domain, html_content,link_list)
        html_msg.append(html_content)
    print "[*] Total external links discovered: %d" % len(link_list)
    link_list_df = pd.DataFrame({'data':link_list})
    link_list_df.to_csv('data/otherlinks_discovered.csv',sep='|', encoding='utf-8')
    record_list_df.to_csv('data/url_discovered.csv',sep='|', encoding='utf-8')
    parse_me(html_msg)  


#crawler method with (url,  month-index) as params
crawler('goodreads.com/book',  "2019-13")