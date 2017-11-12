import urllib.request
from bs4 import BeautifulSoup as bs
import mysql.connector

class crawl:
    def getHtml(self,url=None):
        html=urllib.request.urlopen(url)
        return html
    def connectDB(self):
        conn=mysql.connector.connect(user='root',password='',database='publications')

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


#urls={'https://www.v2ex.com/go/programmer','https://www.v2ex.com/go/python','https://www.v2ex.com/go/idev'
 #     ,'https://www.v2ex.com/go/android','https://www.v2ex.com/go/linux'}
urls2={'programmer','python','idev','android','linux'}
#crawl=crawl()
#crawl.connectDB()
for url in urls2:
    url1='https://www.v2ex.com/go/'+url
    html=urllib.request.urlopen(url1)
    soup=bs(html,'html.parser')
    items = soup.findAll('td',{'valign':'middle','width':'auto'})
    #table=crawl.createTable(url+'b')
    for item in items:
        #title = item.find('span',{'class': 'item_title'}).select().get_text()
        #title = item.select('span[class="item_title"] > a')[0].get_text()
        title = item.find('span',{'class': 'item_title'}).a.get_text()
        name = item.find('span',{'class': 'small fade'}).a.get_text()
        print(title)
        print(name)
     #   crawl.inserttable(table, title, 'cyc')