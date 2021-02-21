from datetime import datetime
import feedparser
import asyncio
import asyncpg


date_format = "%d.%m.%Y"

def get_from_rss():
    """Делает запрос к RSS на основной странице закупок"""
    NewsFeed = feedparser.parse("https://zakupki.gov.ru/epz/order/extendedsearch/rss.html")
    entries = NewsFeed.entries
    data = []
    for entry in entries:
        number = entry.title.split('№')[-1]
        link = entry.link
        try:
            posted = datetime.strptime(entry.summary.split('<br />')[-5].split('</strong>')[-1], date_format)
        except:
            posted = datetime.strptime('01.01.1970', date_format)
        try:
            updated = datetime.strptime(entry.summary.split('<br />')[-4].split('</strong>')[-1], date_format)
        except:
            posted = datetime.strptime('01.01.1970', date_format)
        status = entry.summary.split('<br />')[-3].split('</strong>')[-1]
        act = entry.summary.split('<br />')[7].split('</strong>')[-1]
        d = (number, link, posted, updated, status, act)
        data.append(d)
    return data

async def load_to_db(data):
    """Загружает данные из RSS в БД
    """
    conn = await asyncpg.connect('postgresql://postgres:1488@127.0.0.1:5432/tender_db')
    await conn.executemany('''SELECT load_to_rss($1, $2, $3, $4, $5, $6)''', data)
    await conn.close()

def start_parse():
    data = get_from_rss()
    asyncio.get_event_loop().run_until_complete(load_to_db(data))
    return True

if __name__ == '__main__':
    start_parse()