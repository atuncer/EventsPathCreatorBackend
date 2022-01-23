import json
import datetime

def stringToDatetime(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")


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


def runnerLocalJson():
    with open('jsoFinal.json',"r", encoding="utf-8") as f1, open('matrixMerged.json',"r", encoding="utf-8") as f2: 
        return json.load(f1), json.load(f2)

if __name__ == '__main__':
    feasibleEvents(runnerLocalJson()[0], runnerLocalJson()[1],0,True)