# -*- coding: utf-8 -*-
"""
Created on Wed May  1 07:57:53 2019

@author: z
"""

import pandas as pd
import sqlite3
from xml.etree import ElementTree as ET
import xml.etree.cElementTree as et
import sys 
import os
import requests, zipfile, io
import zipfile
import time

def make_dir(dir_name):
    if not os.path.exists(dir_name):
        print("{} dir has created".format(dir_name))
        os.makedirs(dir_name)

def db_open(path):
    db = sqlite3.connect(path)
    return db


def to_xml(df, filename=None, mode='wb'):
    make_dir(dir_name='out')
    def row_to_xml(row):
        xml = ['<data>','  <row>']
        for i, col_name in enumerate(row.index):
            xml.append('    <{0}>{1}</{0}>'.format(col_name,row.iloc[i]))
        xml.append('  </row>')
        xml.append('</data>')
        return '\n'.join(xml)
    res = '\n'.join(df.apply(row_to_xml, axis=1))

    if filename is None:
        return res
    try:
        tree = ET.XML(res)
        filename = 'out/{}_{}_{}.xml'.format(filename,msg[1],msg[0])
        with open(filename, "wb") as f:
            f.write(ET.tostring(tree))
            time.sleep(2)
            print("XML file {} has saved to - '{}'\n".format(filename.split('/')[-1],os.path.abspath(filename)))
    except Exception as e:
        print("There is no records to save on XML file\n",e)
    

def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None
 

def create_table(df,head):
    time.sleep(2)
    query = 'create table {} as select * from temp'.format(head)
    df.to_sql("temp",DB,if_exists="replace",index=None)
    print_query = '\n select * from \n'+ query.split()[2]
    if df.shape[0]!=0:
        try:
            DB.execute(query)
            print('\nTable - {} has created, with {} records\n'.format(print_query.split()[-1],df.shape[0]))
        except Exception  as e:
            print(e,'\n')
    else:
        print("Table - {} is Empty".format(head))
            

def save_csv(df,name):
    make_dir(dir_name='out')
    path = r'out/{}.csv'.format(name)                  
    df.to_csv(path,index=False)
    time.sleep(2)
    print("CSV file {} has saved to - '{}'\n".format(path.split('/')[-1],os.path.abspath(path)))


def save_json(df,name):
    make_dir(dir_name='out')
    path = r'out/{}.json'.format(name)                   
    df.to_json(path)
    time.sleep(2)
    print("CSV file {} has saved to - '{}'\n".format(path.split('/')[-1],os.path.abspath(path)))


def download_db(url):
    make_dir(dir_name='db')
    db_path = 'db/'+url.split('/')[-1].split('.')[0]+'.db'
    if os.path.exists(db_path):
        print("DB exist")
        return db_open(db_path) 
    print("Downloading...")
    try:
        r = requests.get(url)
        print("Download has completed")
        time.sleep(2)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print("Saving file - {}".format(z.namelist()[0]))
        z.extractall('db')
        print(os.path.join(os.path.abspath('db'),z.namelist()[0]))
        return db_open(os.path.join(os.path.abspath('db'),z.namelist()[0]))
    except Exception as e:
        print(e)


def search_db():
    try:
        f = os.listdir(r'db')
        if f[0].endswith('.db'):
            print("{} DB has found".format(str(f[0])))
            return db_open('db/'+str(f[0]))
        else:
            print("No DB file has found, Please provide link")
            exit()
    except FileNotFoundError:
        print("No DB file has found, Please provide link")
        exit()
            

def main(msg_):
    global msg
    msg = msg_.decode("utf-8").split('#')
    global DB
    if len(msg) == 2:
        DB = search_db()
    else:
        DB = download_db(msg[2])
    customers = pd.read_sql_query("select * from customers",DB)
    invoices = pd.read_sql_query("select * from invoices",DB)
    #loading the album name in advance 
    invoices_items = pd.read_sql_query('''select a.*,c.Title as AlbumName from invoice_items a
                                       left join tracks b on a.TrackId=b.TrackId
                                       left join albums c on b.AlbumId=c.AlbumId''',DB)
    
    #Q1 - a list of purchases  by Country
    purchase_by_country = (pd.merge(invoices,customers,how='inner',on='CustomerId').groupby('Country')
                                                                                   .size()
                                                                                   .sort_values(ascending=False)
                                                                                   .reset_index(name='N_Of_purchases'))
    # Save to CSV  
    save_csv(purchase_by_country,name='purchase_by_country')
    
    #Q2 - a list of product sold by country
    items_by_country = (pd.merge(invoices_items,pd.merge(invoices,customers,how='inner',on='CustomerId'),how='inner',on='InvoiceId')
                                                                                            .groupby('Country')
                                                                                            .size()
                                                                                            .sort_values(ascending=False)
                                                                                             .reset_index(name='N_Of_Items'))
    # Save to CSV
    save_csv(items_by_country,name='items_by_country')
    
    
    #Q3 -  a list of albums by country
    albums_by_country = pd.merge(invoices_items,pd.merge(invoices,customers,how='inner',on='CustomerId'),how='inner',on='InvoiceId')
    albums_by_country = albums_by_country['AlbumName'].groupby([albums_by_country.Country]).apply(set).reset_index()
    albums_by_country.set_index('Country',inplace=True)
    save_json(albums_by_country,name = 'albums_by_country')
    
    #Q4
    max_album_by_year_rock = pd.read_sql_query('''select AlbumName,Country,max(sales) as sales,year  from (select albums.Title as AlbumName, customers.Country,count( albums.Title) as sales ,
                                               strftime("%Y",invoices.InvoiceDate) as year from customers
                                               left join invoices on customers.CustomerId=invoices.CustomerId
                                               left join invoice_items on invoices.InvoiceId=invoice_items.InvoiceId
                                               left join tracks on invoice_items.TrackId=tracks.TrackId
                                               left join albums on tracks.AlbumId=albums.AlbumId
                                               where Lower(customers.Country)="{}" and strftime("%Y",invoices.InvoiceDate)>="{}" and tracks.GenreId in 
                                               (select GenreId from genres where Name="Rock")
                                               group by albums.Title)
                                               '''.format(msg[0].lower(),msg[1]),DB)
    to_xml(max_album_by_year_rock,'Max_rock_album_by_year')
    
    
    #Create Tables
    #Q-5-2.1 2.2 2.4
    create_table(purchase_by_country,head='Purchase_by_country')
    create_table(items_by_country,head='Items_by_country')
    create_table(max_album_by_year_rock,head='Max_rock_album_by_year_{}_{}'.format(msg[1],msg[0]))

