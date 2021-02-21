from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import asyncpg
import asyncio
from pymongo import MongoClient

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    driver = webdriver.Chrome('/usr/lib/chromium-browser/chromedriver', options=chrome_options)
    return driver

def get_data(driver):
    tender_info = {}
    tender_info['tender_number'] = driver.find_element_by_class_name('cardMainInfo__purchaseLink').text
    tender_info['type_tender'] = driver.find_element_by_class_name('cardMainInfo__title').text
    tender_info['tender_status'] = driver.find_element_by_class_name('cardMainInfo__state').text
    tender_info['start_price'] = driver.find_element_by_class_name('cost').text

    main_info = driver.find_elements_by_class_name('cardMainInfo__section')
    for td in main_info:
        try:
            tender_info[td.find_element_by_class_name('cardMainInfo__title').text] = td.find_element_by_class_name('cardMainInfo__content').text
        except NoSuchElementException:
            pass

    blockInfo = driver.find_elements_by_class_name("blockInfo")
    for blocks in blockInfo:
        for section in blocks.find_elements_by_class_name('blockInfo__section'):
            try:
                tender_info[section.find_element_by_class_name('section__title').text[0:25]] = section.find_element_by_class_name('section__info').text
            except NoSuchElementException:
                pass

    return tender_info


async def get_links_from_db():
    conn = await asyncpg.connect('postgresql://postgres:1488@127.0.0.1:5432/tender_db')
    row = await conn.fetch("SELECT tender_link FROM rss WHERE req_number = 1 AND tender_act LIKE '44%'")
    links = [r.get('tender_link') for r in row]
    print(links)
    await conn.close()
    return links

def start_parse():
    client = MongoClient('0.0.0.0', 27017)
    db = client.tender_db
    collection = db.test_collection
    links = asyncio.get_event_loop().run_until_complete(get_links_from_db())
    driver = get_driver()
    for l in links:
        driver.get(l)
        data = get_data(driver)
        collection.insert_one(data).inserted_id
    return True



if __name__ == '__main__':
    start_parse()







