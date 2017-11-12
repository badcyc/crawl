#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
功能：1.爬取大类结构的数据并放入mongogb
      2.将数据从mongodb放入mysql
      3.将数据从mysql放入excel文件

mysql数据库结构为事先静态创建，因为事先不知道最多有多少层类别，所以在从mongodb到mysql的过程中可能会因为mysql中的层次字段数量不足而出现问题
"""
import requests, time, pymongo, copy, pymysql, xlwt
from bs4 import BeautifulSoup

url = "http://d.qianzhan.com/xdata/list/xfyyy0yyIxPyywyy2xDxfd.html"


def getProxyList(mode):
    ProxyList = []
    url = "http://ip.chinaz.com/"

    if mode == 0:
        print "Error"
        return ProxyList

    if (mode>=1):
        '''爬取西刺代理'''
        page = requests.get("http://cache.baiducontent.com/c?m=9f65cb4a8c8507ed4fece763105392230e54f72967868b432c8fcd1384642c101a39feaf627f5052dcd20d1016db4c4beb802102311451c68cc9f85dadbd855c5c9f5636676bf0&p=90769a4786cc41ac52fecb2b614789&newp=882a9546dc881ee609be9b7c440d91231610db2151d4d11f6b82c825d7331b001c3bbfb423241100d2c3796304a44e5feef43175330025a3dda5c91d9fb4c57479c3787d21&user=baidu&fm=sc&query=%CE%F7%B4%CC%B4%FA%C0%ED&qid=d4f590d500012d17&p1=1&fast=y")
        soup = BeautifulSoup(page.text, 'lxml')
        trList = soup.find_all('tr')
        for tr in trList:
            tdList = tr.find_all('td')
            if len(tdList)==0:
                continue
            if tdList[5].string.lower()=='http' or tdList[5].string.lower()=='https':
                proxy = {tdList[5].string.lower(): tdList[1].string + ':' + tdList[2].string, }
                try:
                    requests.get(url, proxies=proxy, timeout=0.2)
                except Exception, e:
                    pass
                else:
                    ProxyList.append(proxy)
                    for _proxy in proxy:
                        print "%s://%s" % (_proxy, proxy[_proxy])
        print u'西刺代理爬取结束'

    if (mode==2):
        time.sleep(1)
        '''爬取快代理'''
        page = requests.get("http://www.kuaidaili.com/free/inha/")
        soup = BeautifulSoup(page.text, 'lxml')
        trList = soup.find_all('tr')
        for tr in trList:
            tdList = tr.find_all('td')
            if(len(tdList)==0):
                continue
            proxy = {tdList[3].string.lower(): tdList[0].string + ':' + tdList[1].string, }
            try:
                requests.get(url, proxies=proxy, timeout=0.2)
            except Exception, e:
                pass
            else:
                ProxyList.append(proxy)
                for _proxy in proxy:
                    print "%s://%s" % (_proxy, proxy[_proxy])
        time.sleep(1)
        page = requests.get("http://www.kuaidaili.com/free/intr/")
        soup = BeautifulSoup(page.text, 'lxml')
        trList = soup.find_all('tr')
        for tr in trList:
            tdList = tr.find_all('td')
            if(len(tdList)==0):
                continue
            proxy = {tdList[3].string.lower(): tdList[0].string + ':' + tdList[1].string, }
            try:
                requests.get(url, proxies=proxy, timeout=0.2)
            except Exception, e:
                pass
            else:
                ProxyList.append(proxy)
                for _proxy in proxy:
                    print "%s://%s" % (_proxy, proxy[_proxy])
        print u'快代理爬取结束'

    return ProxyList


def fetchdata(url, prelevel, proxylist, database, data2mongo, prename):
    for proxie in proxylist:
        try:
            response = requests.get(url, verify=False,proxies=proxie, timeout=20)
            break
        except:
            continue
    soup = BeautifulSoup(response.text, 'lxml')
    sel1 = soup.select(".searchfilter_sub")
    nowlevel = len(sel1) - 1
    cont = sel1[nowlevel - 1]
    allsuplink = cont.find_all("a")

    result = soup.select('div[class="search-result-tit"] > em')
    num = result[len(result)-1].text
    name = result[0].text
    nowdata2mongo = copy.deepcopy(data2mongo)
    nowdata2mongo['class' + str(nowlevel)] = name
    nowdata2mongo['number'] = num
    nowdata2mongo['href'] = url
    if nowlevel > prelevel:
        for link in allsuplink:
            if link.text != u"全部":
                nowdata2mongo['class' + str(prelevel)] = prename
            nexturl = "http://d.qianzhan.com" + link['href']
            fetchdata(nexturl, nowlevel, proxylist, database, nowdata2mongo, name)
    else:
        database.insert_one(nowdata2mongo)
    return


def connectmysql():
    database = pymysql.connect(host='localhost',
                               user='root',
                               password='',
                               db='qianzhan',
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor
                               )
    cursor = database.cursor()
    return database, cursor


def mongo2mysql(mongodb):
    database, cursor = connectmysql()
    query_in = """INSERT INTO `qianzhan`.`qianzhanweb`
        (`class1`,
        `class2`,
        `class3`,
        `class4`,
        `class5`,
        `number`,
        `link`)
        VALUES
        (%s,%s,%s,%s,%s,%s,%s);
        """
    for data in mongodb.find():
        values = []
        print data
        for i in range(1, 6):
            if data.has_key('class'+str(i)):
                values.append(data['class'+str(i)])
            else:
                values.append("--")
        values.append(int(data['number'].replace(",", "")))
        values.append(data['href'])
        datainsql = tuple(values)
        cursor.execute(query_in, datainsql)
        database.commit()
    cursor.close()
    database.close()


def write_excel(data):
    f = xlwt.Workbook()  # 创建工作簿
    '''
    创建第一个sheet:
      sheet1
    '''
    sheet1 = f.add_sheet(u'Sheet1', cell_overwrite_ok=True)  # 创建sheet
    row0 = [u'class1', u'class2', u'class3', u'class4', u'class5', u'number', u'link']

    # 生成第一行
    for i in range(0, len(row0)):
        sheet1.write(0, i, row0[i])

    for i in range(1, len(data)):
        datainsql = (data[i]['class1'], data[i]['class2'], data[i]['class3'], data[i]['class4'], data[i]['class5'], data[i]['number'], data[i]['link'])
        for j in range(0, len(data[i])):
            sheet1.write(i, j, datainsql[j])

    f.save('..\qianzhanweb.xls')  # 保存文件

def sql2excel():
    database, cursor = connectmysql()
    query_sel = """SELECT * FROM qianzhan.qianzhanweb;"""
    cursor.execute(query_sel)
    results = cursor.fetchall()
    write_excel(results)


if __name__ == "__main__":
    client = pymongo.MongoClient('localhost',27017)  # 连接mongodb
    mongodb = client['qianzhan']  # 创建一个名叫xiaozhu的库文件
    home_info = mongodb['qianzhanweb'] # 创建一个home_info的页面

    data2mongo = {}
    functionnum = raw_input("""select a function:
    1.fetch data and put in mongodb
    2.mongogb to mysql
    3.mysql to excel
    """)
    if functionnum == "1":
        proxylist = getProxyList(1)
        fetchdata(url, 0, proxylist, home_info, data2mongo, "中国宏观")
    elif functionnum == "2":
        mongo2mysql(home_info)
    elif functionnum == "3":
        sql2excel()
    else:
        print "don't have the function%s" % functionnum
