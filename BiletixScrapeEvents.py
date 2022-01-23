import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
import datetime
import boto3

# headers and cookies are for convincing biletix that this script is an actual human
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
cookies = {'region' : 'TURKIYE',
    'BXID' : 'AAAAAAWL6Bp52Y73b5/vrbH/RcZU3NglLYybpUv1XzB+Qx7xFg=='} # my personal biletix id that I extracted from cookies. TODO: spoof it.


def scrapeBiletix() -> json:
    today = datetime.datetime.now()
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)

    isEventLambda = lambda x: len(x["id"]) < 6

    # biletix's api url which returns events
    API = f'https://www.biletix.com/solr/tr/select/?start=0&rows=1300&q=*:*&fq=start%3A%5B{today.year}-{today.month}-{today.day}T00%3A00%3A00Z%20TO%20{tomorrow.year}-{tomorrow.month}-{tomorrow.day}T07%3A00%3A00Z%2B1DAY%5D&sort=start%20asc,%20vote%20desc&&fq=city:%22%C4%B0stanbul%22&wt=json&indent=true&facet=true&facet.field=category&facet.field=venuecode&facet.field=region&facet.field=subcategory&facet.mincount=1'
    #API2 = 'https://www.biletix.com/solr/tr/select/?start=0&rows=1300&q=*:*&fq=start%3A%5B2021-10-27T00%3A00%3A00Z%20TO%202023-10-28T00%3A00%3A00Z%2B1DAY%5D&sort=start%20asc,%20vote%20desc&&fq=city:%22%C4%B0stanbul%22&wt=json&indent=true&facet=true&facet.field=category&facet.field=venuecode&facet.field=region&facet.field=subcategory&facet.mincount=1'
    r = requests.get(API, headers=headers, cookies=cookies) # calls a get request
    html = r.content.decode('utf8')
    jso2 = json.loads(html)
    jso = list(filter(isEventLambda,jso2["response"]["docs"]))
    
    for obj in jso:
        obj["description"] = BeautifulSoup(obj["description"], 'html.parser').text[:50] + "..."
        id = obj['id']
        obj["url"] = f"https://www.biletix.com/etkinlik/{id}/TURKIYE/tr"
        obj["avlbtyUrl"] = f"https://www.biletix.com/availability/{id}?region=TURKIYE&id={id}&lang=en"
        obj["eventDates"] = []
        
        required = ["id", "url", "avlbtyUrl", "eventDates", "venuecode", "category", "start", "end", "description", "name", "artist"]
        for child in list(obj):
            if child not in required:
                obj.pop(child)

        if len(obj["artist"]) > 80:
            obj["artist"] = obj["artist"][:80]
            obj["artist"].append("and more ...")

        obj['geo'] = {}

    return jso


def timeStampConverter(stamp: str):
    if len(stamp) > 10:
        try:
            date_time_obj = datetime.datetime.strptime(stamp, '%B %d, %Y %H:%M')
            return datetime.datetime.strftime(date_time_obj, '%Y-%m-%d %H:%M:%S')
        except:
            pass


async def fetchAll(jsn: json):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[fetch(session, obj) for obj in jsn])

async def fetch(session: aiohttp.ClientSession, obj: json):

    url = obj["avlbtyUrl"]
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with session.get(url, headers=headers, cookies=cookies, timeout=timeout) as result:
            
            txt = await result.text()
            await asyncio.sleep(1)
            try:
                json1 = json.loads(txt.split("datax = ")[1][:-2])
            except IndexError:
                json1 = ""
            if "profiles" in json1 and "performances" in json1["profiles"][0]:
                for date in json1["profiles"][0]["performances"]:
                    obj["eventDates"].append(timeStampConverter(date["date"]))

    except asyncio.exceptions.TimeoutError:
        pass


def parseBiletix(putToDb: bool = False):
    jso = scrapeBiletix()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetchAll(jso))

    if putToDb: writeToDB(jso)

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(jso, f, ensure_ascii=False, indent=4)
        

def writeToDB(jso: json):
    access_key_id = secret_access_key = ""
    with open('rootkey.key') as f: 
        read = f.readlines()
        access_key_id = read[0].strip()
        secret_access_key = read[1].strip()
    dynamodb = boto3.resource('dynamodb',aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name='eu-central-1')
    table = dynamodb.Table('BiletixEvents')

    for obj in jso:
        if len(obj['eventDates']) == 0: obj['eventDates'].append("")
        if len(obj['artist']) == 0: obj['artist'].append("")
        Itm={"id": obj['id'] , "artist": obj['artist'], "url": obj['url'],"avlbtyUrl": obj['avlbtyUrl'], "eventDates":obj['eventDates'] ,"venuecode": obj['venuecode'] ,"category": obj['category'], "start": obj['start'], "end": obj['end'],"description": obj['description'] ,"name": obj['name']}

        with table.batch_writer() as writer:
            writer.put_item(Item=Itm)
            

if __name__ == "__main__":
    parseBiletix()
