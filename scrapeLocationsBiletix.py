import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import boto3
from decimal import Decimal

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
cookies = {'region' : 'TURKIYE',
    'BXID' : 'AAAAAAWL6Bp52Y73b5/vrbH/RcZU3NglLYybpUv1XzB+Qx7xFg=='}

MAPS_API_KEY = ""

with open('rootkey.key') as f: 
    read = f.readlines()
    MAPS_API_KEY = read[2].strip()

async def fetchAll(urls, jso):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[fetch(session, url, jso) for url in urls])

async def fetch(session: aiohttp.ClientSession, url: str, jso: json):

    timeout = aiohttp.ClientTimeout(total=20)
    id = url.split('/')[4]

    async with session.get(url, headers=headers, cookies=cookies, timeout=timeout) as result:
        txt = await result.text()
        soup = BeautifulSoup(txt, 'html.parser')
        await asyncio.sleep(1)
        header = soup.find("h1", {"class": "venueTitleWrapperDesktop"})
        subtext = soup.find("ul", {"itemprop": "address"})
        addr = header.text.strip().replace('\n',' ') + " " + subtext.text.strip().replace('\n',' ')
        print(header.text.strip().replace('\n',' ') + " " + subtext.text.strip().replace('\n',' '))
        jso[id] = {"addr" : addr, "lat": 0, "long": 0}


async def fetchAllGeocode(urls, jso):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[fetchGeocode(session, urls[key], key, jso) for key in urls])

async def fetchGeocode(session: aiohttp.ClientSession, url: str, key: str, jso: json):

    timeout = aiohttp.ClientTimeout(total=20)
    async with session.get(url, headers=headers, cookies=cookies, timeout=timeout) as result:
        txt = await result.text()
        myJson = json.loads(txt)
        await asyncio.sleep(1)
        jso[key]['lat'] = myJson["results"][0]['geometry']['location']['lat']
        jso[key]['long'] = myJson["results"][0]['geometry']['location']['lng']


def scrapeLocation(urls):
    jsn: json = {}

    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetchAll(urls, jsn))

    geocodeUrls = {}
    for key in jsn:
        obj = jsn[key]
        addr: str = obj['addr'].split(" ")
        yo = '+'.join(addr)
        geocodeUrls[key] = (f"https://maps.googleapis.com/maps/api/geocode/json?address={yo}&key={MAPS_API_KEY}")
        print(geocodeUrls[key])

    #TODO: if not in database
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetchAllGeocode(geocodeUrls, jsn))

    with open('data222.json', 'w', encoding='utf-8') as f:
        json.dump(jsn, f, ensure_ascii=False, indent=4)

    access_key_id = secret_access_key = ""
    with open('rootkey.key') as f: 
        read = f.readlines()
        access_key_id = read[0].strip()
        secret_access_key = read[1].strip()
    dynamodb = boto3.resource('dynamodb',aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name='eu-central-1')
    table = dynamodb.Table('BiletixLocations')

    for key in jsn:
        obj = jsn[key]
        print(key)
        table.put_item(Item = {'venuecode' : key, 'address' : obj['addr'], 'lat' : Decimal(f"{obj['lat']}"), 'lng' : Decimal(f"{obj['long']}")})


if __name__ == '__main__':
    nan = ['1Q', 'ZA', 'YH', '6R']
    scrapeLocation([f"https://www.biletix.com/mekan/{x}/TURKIYE/tr" for x in nan])
