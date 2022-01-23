# Events Path Creator Backend
Computer Science degree Final Project with [Onurcan Yüksel](https://github.com/onurcyksl) and [Ege Çapar](https://github.com/megecapar).

### [Check the Frontend]()

## Summary
This project scrapes events from the Turkey's biggest event ticket marketplace and plans a daily schedule of events by Google Maps' APIs and tailor made path finding algorithms.

## Serverless Architecture
This project is built with Amazon Web Services Serverless Application Model Framework ([AWS SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html)).
### AWS API Gateway
Triggers AWS Lambda and responds to the API call.
### AWS Lambda
Gets triggered by AWS API Gateway and runs the code.
### AWS DynamoDB
A NoSQL database that holds the events and geolocations of event venues.

## Code Walkthrough
### BiletixScrapeEvents.py ([here](https://github.com/atuncer/EventsPathCreatorBackend/blob/main/BiletixScrapeEvents.py))
Scrapes the events from the ticket marketplace, cleans the data, creates a json, and saves to DynamoDB. Also for each event, it asyncly scrapes the availability status and event sessions.
### PathFinder.py ([here](https://github.com/atuncer/EventsPathCreatorBackend/blob/main/PathFinder.py))
### ScrapeLocationsBiletix.py ([here](https://github.com/atuncer/EventsPathCreatorBackend/blob/main/ScrapeLocationsBiletix.py))
