
import urllib.request
import csv
import random
import time
import socket
import http.client
from bs4 import BeautifulSoup as bs
import json
import mysql.connector

class crawl:
    def getHtml(self,url=None):
        html=urllib.request.urlopen(url)
        return html
    def connectDB(self):
        conn=mysql.connector.connect(user='cyc',password='cyc1997927',database='publications')

        return conn


    def createTable(self,createTableName):
        conn=self.connectDB()
        cursor=conn.cursor()
        cursor.execute('create table if not exists '+createTableName+ ' (title varchar(100) ,name varchar(20))')
        conn.close
        print('create table '+createTableName+' successfully')
        return createTableName
    def inserttable(self,insertTable,insertTitle,insertName):
        insertContentSql = "INSERT INTO " + insertTable + " (title,name)VALUES(%s,%s)"
        conn=self.connectDB()
        cursor=conn.cursor()
        cursor.execute(insertContentSql,(insertTitle,insertName))
        conn.commit()
        cursor.close()
        print('insert contents to '+insertTable+' successfully')


urls={'https://www.v2ex.com/go/programmer','https://www.v2ex.com/go/python','https://www.v2ex.com/go/idev'
      ,'https://www.v2ex.com/go/android','https://www.v2ex.com/go/linux'}
urls2={'programmer','python','idev','android','linux'}
crawl=crawl()
crawl.connectDB()
for url in urls2:
    url1='https://www.v2ex.com/go/'+url
    html=urllib.request.urlopen(url1)
    soup=bs(html,'html.parser')
    wrapper=soup.findAll('span',{'class':'item_title'})
    table=crawl.createTable(url+'b')
    for aherf in wrapper:
        #s = aherf.a.string
        s=''
        crawl.inserttable(table,s,'cyc')
'''url="https://www.v2ex.com/go/programmer"
#get_html(url)
html= urllib.request.urlopen(url)
soup = bs(html, 'html.parser')
wrapper=soup.findAll('span',{'class':'item_title'})
print(wrapper)
for aherf in wrapper:
    s=aherf.a.string
    print(s)'''