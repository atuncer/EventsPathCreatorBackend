import asyncio
import aiohttp
import json
import datetime
import time
import boto3
from math import ceil, sqrt

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
cookies = {'region' : 'TURKIYE',
    'BXID' : 'AAAAAAWL6Bp52Y73b5/vrbH/RcZU3NglLYybpUv1XzB+Qx7xFg=='}

MAPS_API_KEY = ''
with open('rootkey.key') as f: 
        read = f.readlines()
        MAPS_API_KEY = read[0].strip()

myJson = {}

def stringToDatetime(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")

def locationPullFromDB(events: json):
    access_key_id = secret_access_key = ""
    with open('rootkey.key') as f: 
        read = f.readlines()
        access_key_id = read[0].strip()
        secret_access_key = read[1].strip()
    dynamodb = boto3.resource('dynamodb',aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name='eu-central-1')
    TABLE_NAME = 'BiletixLocations'
    table = dynamodb.Table(TABLE_NAME)

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    for obj in events:
        if obj['category'] != 'startorend':
            try:
                obj['geo']['lat'] = [float(x['lat']) for x in data if obj['venuecode'] == x['venuecode']][0]
                obj['geo']['lng'] = [float(x['lng']) for x in data if obj['venuecode'] == x['venuecode']][0]
            except IndexError: # TODO: scrape from biletix and add the coresponding lat&lng to database
                obj['geo']['lat'] = 0.0
                obj['geo']['lng'] = 0.0
            
    return events


def matrixAPI(jso: json): 
    
    batchReqs = []
    # This api can only take maximum 10*10 parameters, so in this loop, parameters are divided into 10*10.
    for i in range(ceil(len(jso)/10)):
        orign = [f"{jso[a]['geo']['lat']}%2C{jso[a]['geo']['lng']}%7C" for a in range(i*10,min((i+1)*10,len(jso)))]
        for j in range(ceil(len(jso)/10)):
            dest = [f"{jso[a]['geo']['lat']}%2C{jso[a]['geo']['lng']}%7C" for a in range(j*10,min((j+1)*10,len(jso)))]
            batchReqs.append({f'{i}-{j}' : f'https://maps.googleapis.com/maps/api/distancematrix/json?departure_time=now&origins={"".join(orign)[:-3]}&destinations={"".join(dest)[:-3]}&key={MAPS_API_KEY}'})
    return batchReqs


async def fetchAll(reqs: list):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[fetch(session, obj) for obj in reqs])

async def fetch(session: aiohttp.ClientSession, dct: str):
    url,id = [(dct[x],x) for x in dct][0]
    
    async with session.get(url) as result:
        txt = await result.text()
        myJson[id] = json.loads(txt)


def mergeJson(matrix: json):
    for i in range(int(sqrt(len(matrix)))):
        if i != 0:
            matrix['0-0']['origin_addresses'].extend(matrix[f'{i}-{0}']['origin_addresses'])
            matrix['0-0']['rows'].extend(matrix[f'{i}-{0}']['rows'])
        for j in range(int(sqrt(len(matrix)))):
            if j != 0:
                if i == 0:
                    matrix['0-0']['destination_addresses'].extend(matrix[f'{i}-{j}']['destination_addresses'])
                for objIdx in range(len(matrix[f'{i}-{j}']['rows'])):
                    matrix['0-0']['rows'][i*10+objIdx]['elements'].extend(matrix[f'{i}-{j}']['rows'][objIdx]['elements'])
        
    matrix = matrix['0-0']

    return matrix

    
def feasibleEvents(events: json, matrix: json, whichAlgo: int, isToEnd: bool, maxWait: int):  #jsoFinal.json, matrixMerged.json

    currentTime = datetime.datetime.now()
    eventTime = datetime.timedelta(hours=2)
    maxWaitTime = datetime.timedelta(hours=maxWait)
    
    
    isFeasbileLambda2 = lambda x: datetime.datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S') > (currentTime + eventTime + datetime.timedelta(seconds=x[2])) and datetime.datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S') < (currentTime + maxWaitTime + eventTime)
    
    treeDict = {}
    
    for i, obj in enumerate(events):
        
        treeDict[obj['id']] = {}
        treeDict[obj['id']]['Dates'] = obj['eventDates']
        treeDict[obj['id']]['Duration'] = {}

        if len(treeDict[obj['id']]) == 0:
            
            start = datetime.datetime.strptime(obj["start"], "%Y-%m-%dT%H:%M:%S%z")
            treeDict[obj['id']]['Dates'].append(str(start.strftime('%Y-%m-%d %H:%M:%S')))

        for j, obj2 in enumerate(events):
            if matrix['rows'][i]['elements'][j]['status'] == 'OK':
                treeDict[obj['id']]['Duration'][obj2['id']] = matrix['rows'][i]['elements'][j]['duration']['value']
            else:
                treeDict[obj['id']]['Duration'][obj2['id']] = -1  # TODO: fix this. If no geolocation data in database, duration is -1. It will run eventScrape lambda in further updates.



    feasibleTreeNodes = {}
    for key in treeDict:
        for item in treeDict[key]['Dates']:
            currentTime = datetime.datetime.strptime(item, '%Y-%m-%d %H:%M:%S')
            if key != "start" and key != "end":
                feasibleTreeNodes[f"{key}-*-{item}"] = list(filter(isFeasbileLambda2,[[key2, time, treeDict[key2]['Duration'][key]] for key2 in treeDict for time in treeDict[key2]['Dates']]))
            else:
                feasibleTreeNodes[key] = list(filter(isFeasbileLambda2,[[key2, time, treeDict[key2]['Duration'][key]] for key2 in treeDict for time in treeDict[key2]['Dates']]))
    
    return algos(events, feasibleTreeNodes, whichAlgo, isToEnd)


def algos(allEvents, feasibleTree, algo, toEnd):
    visited=[]
    isVisited = lambda x,visitedPlaces: x in visitedPlaces
    addToTree = lambda x,tree: tree[x[0]] if x[0] == 'start' or x[0] == 'end' else tree[f"{x[0]}-*-{x[1]}"]

    myEvents = ["start"]
    itms = feasibleTree["start"]
    while len(itms) > 0:
        bestTime = 99999999
        closestEventTime = datetime.datetime.strptime("2099-12-31 23:59:59", '%Y-%m-%d %H:%M:%S')
        bestItm: list
        mostNodes = 0

        if algo == 0:  # en kısa araba yolu
            for i,it in enumerate(itms):
                if it[2] < bestTime and not isVisited(it[0],visited):
                    bestTime = it[2]
                    bestItm = it
                elif it[2] == bestTime and not isVisited(it[0],visited):
                    if datetime.datetime.strptime(it[1], '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(bestItm[1], '%Y-%m-%d %H:%M:%S'):
                        bestItm = it

        elif algo == 1: # en yakın event
           for i,it in enumerate(itms):
                if stringToDatetime(it[1]) < closestEventTime and not isVisited(it[0],visited):
                    closestEventTime = stringToDatetime(it[1])
                    bestItm = it
                elif stringToDatetime(it[1]) == closestEventTime and it[2] < bestItm[2] and not isVisited(it[0],visited):
                    bestItm = it
        
        elif algo == 2: # en çok childrenı olan event
            for i,it in enumerate(itms):
                if len(addToTree(it,feasibleTree)) > mostNodes and not isVisited(it[0],visited):
                    mostNodes = len(feasibleTree[f"{it[0]}-*-{it[1]}"])
                    bestItm = it
                elif len(addToTree(it,feasibleTree)) == mostNodes and not isVisited(it[0],visited):
                    if datetime.datetime.strptime(it[1], '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(bestItm[1], '%Y-%m-%d %H:%M:%S'):
                        bestItm = it
                if len(addToTree(it,feasibleTree)) == 0 and not isVisited(it[0],visited):
                    bestItm = it
            
        
        if not toEnd:
            if bestItm[0] != "end":
                myEvents.append(f"{bestItm[0]}-*-{bestItm[1]}")
                itms = feasibleTree[f"{bestItm[0]}-*-{bestItm[1]}"]
            else:
                myEvents.append(f"{bestItm[0]}")
                break
            visited.append(bestItm[0])

        else:
            if len(addToTree(bestItm,feasibleTree)) > 0 or bestItm[0] == 'end':
                myEvents.append(f"{bestItm[0]}-*-{bestItm[1]}") if bestItm[0] != "end" else myEvents.append(f"{bestItm[0]}")
                itms = addToTree(bestItm,feasibleTree)
                visited.append(bestItm[0])
                print(f"added: {bestItm}")
            else:
                myEvents = myEvents[:-1]
                print(f"gone back to: {myEvents[-1]}")
                itms = feasibleTree[myEvents[-1]]
                popped = feasibleTree.popitem()[0]
                # deleting all the references to the popped node
                for key in feasibleTree:
                    for elem in feasibleTree[key]:
                        if elem[0] == popped.split("-*-")[0] and elem[1] == popped.split("-*-")[1]:
                            feasibleTree[key].remove(elem)
                
                            

    print(myEvents)
    geos = []
    tempEvent: dict
    for myEvent in myEvents:
        for event in allEvents:
            if myEvent.split("-*-")[0] == event['id']:
                tempEvent = event['geo']
                tempEvent['title'] = event['name']
                tempEvent['comment'] = event['description']
                geos.append(tempEvent)

    for i,geo in enumerate(geos):
        geo['id'] = i
    return geos


def runnerJson():
    jso: json
    start = {
        "id": "start",
        "category": "startorend",
        "venuecode": "start",
        "end": "",
        "eventDates": [
	        str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ],
        "geo": {
            "lat": 40.986744300000012,
            "lng": 29.0286141,
        }
    }
    end = {
        "id": "end",
        "category": "startorend",
        "venuecode": "end",
        "end": "",
        "eventDates": [
            "2021-11-18 22:00:00"
        ],
        "geo": {
            "lat": 40.986744300000012,
            "lng": 29.0286141,
        }
    }
    with open('data.json') as json_file: 
        jso = json.load(json_file)[:38]
    jso.insert(0,start)
    jso.insert(1,end)
    jso = locationPullFromDB(jso)
    
    batchs = matrixAPI(jso)

    for i in range(len(batchs)//10 + 1):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(fetchAll(batchs[i*10:min((i+1)*10,len(batchs))]))
        time.sleep(1)  # max 10 reqs per second
    
    matrix = mergeJson(myJson)
    with open('matrixMerged.json', 'w', encoding='utf-8') as f:
        json.dump(matrix, f, ensure_ascii=False, indent=4)
    with open('jsoFinal.json',  'w', encoding='utf-8') as f:
        json.dump(jso, f, ensure_ascii=False, indent=4)

    return jso, matrix

def runnerLocalJson():
    with open('jsoFinal.json') as f1, open('matrixMerged.json') as f2: 
        return json.load(f1), json.load(f2)


if __name__ == '__main__':
    # jso, matrix = runnerJson()

    jso, matrix = runnerLocalJson()
    feasibleEvents(jso, matrix)
    