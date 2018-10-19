# -*- coding: utf-8 -*-

import urllib2
import pandas as pd

recordContract = ['rb1901','m1901','l1901','SR901','p1901'] #收取的vnpy数据格式

temp1 = [i.upper() for i in recordContract]
temp2 = [filter(str.isalpha,i)+'1'+filter(str.isdigit,i) if len(filter(str.isdigit,i)) == 3 else i for i in temp1]
temp3 = [filter(str.isalpha, x) for x in temp1]

format1 = lambda x: x.split("_")[-1]
format2 = lambda x: filter(str.isalpha,x)
format3 = lambda x : x in temp3

recordContractSina = temp2 # sina数据格式为字母全大写，且数字为四位
doninantContract = ['RB0','AG0','AU0','CU0','AL0','ZN0','PB0','RU0','FU0','A0','M0','Y0','J0','C0','L0','P0',\
                   'V0','PP0','RS0','RM0','FG0','CF0','MA0','TA0','SR0']
url = "http://hq.sinajs.cn/list="
urlRC = url + ','.join(recordContractSina)
urlDC = url + ','.join(doninantContract)
def getHtml(url):
    m = urllib2.urlopen(url)
    html = m.read()
    return html

# def save_to_file(file_name, contents):
#     fh = open(file_name, 'w')
#     fh.write(contents)
#     fh.close()

def html2Df(html):
    h = html.split(";")
    h1 = [i.split("=") for i in h]
    df = pd.DataFrame(h1)
    df['symbol'] = df[0].map(format1)
    cols = ['name', 'time', 'openPrice', 'highPrice', 'lowPrice', 'preClose', 'bid1Price', 'ask1Price', 'lastPrice', \
            'settlePrice', 'preSettlePrice', 'bid1Vol', 'ask1Vol', 'openInterest', 'Vol', 'exchange', 'product', \
            'date', 'statusId', 'highPrice', 'unknown1', 'highPrice', 'unknown2', 'unknown3', 'unknown2', \
            'unknown4', 'unknown5', 'unknown6']
    names = df[1].str.split(',', expand=True)
    names.columns = cols
    df = df.join(names)
    df['symbolAlpha'] = df['symbol'].map(format2)
    return df

htmlRC = getHtml(urlRC)
htmlDC = getHtml(urlDC)
dfRecord = html2Df(htmlRC)
dfDominant = html2Df(htmlDC)
# dfRecord.to_csv("dfRecord.csv")
# dfDominant.to_csv("dfDominant.csv")

def cmpRecDomi(df1,df2):
    df1 = df1.set_index(df1.symbolAlpha)
    df1 = df1[df1.symbolAlpha.apply(format3)]#.filter(items=['Vol','openInterest'])
    df1 = df1[['date', 'time', 'openInterest', 'Vol']]
    df11 = df1[['openInterest', 'Vol']].applymap(lambda s: s.replace(',', '')).astype(float)

    df2 = df2.set_index(df2.symbolAlpha)
    df2 = df2[df2.symbolAlpha.apply(format3)]#.filter(items=['Vol', 'openInterest'])
    df2 = df2[['date', 'time', 'openInterest', 'Vol']]
    df22 = df2[['openInterest', 'Vol']].applymap(lambda s: s.replace(',', '')).astype(float)

    df3 = df1
    df3['OIDiff'] = df11[['openInterest']] - df22[['openInterest']]
    df3['VDiff'] = df11[['Vol']] - df22[['Vol']]
    print df3

if __name__ == "__main__":
    cmpRecDomi(dfRecord, dfDominant)
# res = dfRecord.append(dfDominant)
# res.to_csv("res.csv")

# html = requests.get(urlRC)
# soup = BeautifulSoup(html.text, 'lxml')
# print soup
# print soup.find_all()