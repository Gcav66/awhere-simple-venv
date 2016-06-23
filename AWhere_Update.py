# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from calendar import monthrange
import csv


key = ''
secret = ''

class AwhereUpdate(object):

    field_url = 'https://api.awhere.com/v2/fields'
    """
    "GET /v2/weather/locations/" +  row['latnum']+ "," + row['longnum'] + \
                                    "/observations/" + startdate + "," + enddate + "/?blockSize=1"
    """
    

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self.location_url = 'https://api.awhere.com/v2/weather/locations'


    def fetch_token(self):
        client = BackendApplicationClient(client_id = self.key)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url='https://api.awhere.com/oauth/token', client_id=self.key, client_secret=self.secret)
        client = OAuth2Session(key, token=token)
        return client

    def single_call(self, mylat, mylong, startdate, enddate):

        url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/observations/" + \
                  str(startdate) + "," + str(enddate) + "/?blockSize=1"
        print url        
        client = self.fetch_token()
        result = client.get(url)
        return result.json()


    def flatten_single(self, results):
      obsvData = []
      for index, result in enumerate(results['observations']):
          myRow = {}
          myRow = {'date': result['date'],
                   'precipitation': result['precipitation']['amount'],
                   'solar': result['solar']['amount'],
                   'humid_max': result['relativeHumidity']['max'],
                   'humid_min': result['relativeHumidity']['min'],
                   'wind_avg': result['wind']['average'],
                   'temp_max': float(result['temperatures']['max']),
                   'temp_min': float(result['temperatures']['min']),
                   'id': str(index)
                  }
          obsvData.append(myRow)
      return obsvData

    def single_forecast(self, mylat, mylong, startdate='', enddate=''):

        #url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + \
                  #str(startdate) + "," + str(enddate) + "/?blockSize=1"
        if startdate and enddate:
            url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + \
                  str(startdate) + "," + str(enddate) + "/?blockSize=1"
        elif startdate:
            url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + \
                  str(startdate) + "/?blockSize=1"
        else:
            url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + "?blockSize=1"
        print url        
        client = self.fetch_token()
        result = client.get(url)
        return result.json()

    def flatten_forecast(self, results):
        try:
            obsvData = []
            for result in results['forecast']:
                myRow = {}
                myRow = {'startTime': result['startTime'],
                         'endTime': result['endTime'],
                         'precipitation_units': result['precipitation']['units'],
                         'precipitation_chance': result['precipitation']['chance'],
                         'precipitation_amount': result['precipitation']['amount'],
                         'conditionsText': result['conditionsText'],
                         'wind_units': result['wind']['units'],
                         'wind_max': result['wind']['max'],
                         'wind_min': result['wind']['min'],
                         'wind_average': result['wind']['average'],
                         'relativeHumidity_max': result['relativeHumidity']['max'],
                         'relativeHumidity_average': result['relativeHumidity']['average'],
                         'relativeHumidity_min': result['relativeHumidity']['min'],
                         'solar_units': result['solar']['units'],
                         'solar_amount': result['solar']['amount'],
                         'dewPoint_units': result['dewPoint']['units'],
                         'dewPoint_units': result['dewPoint']['amount'],
                         'conditionsCode': result['conditionsCode'],
                         'sky_sunshine': result['sky']['sunshine'],
                         'sky_cloudCover': result['sky']['cloudCover'],
                         'temperature_unit': result['temperatures']['units'],
                         'temperature_max': result['temperatures']['max'],
                         'temperature_min': result['temperatures']['min']
                        }
                obsvData.append(myRow)
            return obsvData
        except KeyError:
            obsvData = []
            for day in results['forecasts']:
                for result in day['forecast']:
                    myRow = {}
                    myRow = {'startTime': result['startTime'],
                             'endTime': result['endTime'],
                             'precipitation_units': result['precipitation']['units'],
                             'precipitation_chance': result['precipitation']['chance'],
                             'precipitation_amount': result['precipitation']['amount'],
                             'conditionsText': result['conditionsText'],
                             'wind_units': result['wind']['units'],
                             'wind_max': result['wind']['max'],
                             'wind_min': result['wind']['min'],
                             'wind_average': result['wind']['average'],
                             'relativeHumidity_max': result['relativeHumidity']['max'],
                             'relativeHumidity_average': result['relativeHumidity']['average'],
                             'relativeHumidity_min': result['relativeHumidity']['min'],
                             'solar_units': result['solar']['units'],
                             'solar_amount': result['solar']['amount'],
                             'dewPoint_units': result['dewPoint']['units'],
                             'dewPoint_units': result['dewPoint']['amount'],
                             'conditionsCode': result['conditionsCode'],
                             'sky_sunshine': result['sky']['sunshine'],
                             'sky_cloudCover': result['sky']['cloudCover'],
                             'temperature_unit': result['temperatures']['units'],
                             'temperature_max': result['temperatures']['max'],
                             'temperature_min': result['temperatures']['min']
                            }
                    obsvData.append(myRow)
                return obsvData
    
    def flatten_batch(self, results):
        obsvData = []
        for result in results['results']:
            for obsv in result['payload']['observations']:
                myRow = {}
                try:
                    myRow = {'date': obsv['date'],
                             'precipitation': obsv['precipitation']['amount'],
                             'solar': obsv['solar']['amount'],
                             'humid_max': obsv['relativeHumidity']['max'],
                             'humid_min': obsv['relativeHumidity']['min'],
                             'wind_avg': obsv['wind']['average'],
                             'temp_max': float(obsv['temperatures']['max']),
                             'temp_min': float(obsv['temperatures']['min']),
                             'id': result['title'].split("_")[1]
                            }
                except TypeError:
                    myRow = {'date': obsv['date'],
                             'precipitation': obsv['precipitation']['amount'],
                             'solar': obsv['solar']['amount'],
                             'humid_max': obsv['relativeHumidity']['max'],
                             'humid_min': obsv['relativeHumidity']['min'],
                             'wind_avg': obsv['wind']['average'],
                             'temp_max': obsv['temperatures']['max'],
                             'temp_min': obsv['temperatures']['min'],
                             'id': result['title'].split("_")[1]
                            }
                obsvData.append(myRow)
        return obsvData
"""        
def create_batch(myfile, start_year, end_year):
    myData = {}
    with open(myfile) as f:
        latlong = csv.DictReader(f)
        for row in latlong:
            myName = "id_" + row['anonymous_id'] + "_weather_data"
            myId = []
            for z in range(start_year, end_year):
                for i in range(1, 13):
                #for i in range(1, 5):
                    myCall = {}
                    day = monthrange(z, i)[1]
                    if len(str(i)) < 2:
                        startdate = str(z) + '-0' + str(i) + '-01'
                        enddate = str(z) + '-0' + str(i) + '-' + str(day)
                    else:
                        startdate = str(z) + '-' + str(i) + '-01'
                        enddate = str(z) + '-' + str(i) + '-' + str(day)
                    
                    myCall["title"] = myName + "_" + str(z) + "_" + str(i)
                    myCall["api"] = "GET /v2/weather/locations/" +  row['latnum']+ "," + row['longnum'] + \
                                    "/observations/" + startdate + "," + enddate + "/?blockSize=1"
                    myId.append(myCall)
                    #counter += 1
            myData[row['anonymous_id']] = myId
            
    return myData
    

def make_call(get_requests, output_file):
    calls_made = []    
    myIDs = []
    with open(output_file, "wb") as w:
        for k, v in myData.iteritems():
            mypayload = {
                        "title":output_file.strip("txt"),
                         "type":"batch",
                         "requests": v
                        }
            client = fetch_token(key, secret)
            stuff = client.post(r'https://api.awhere.com/v2/jobs',json=mypayload)
    
            myIDs.append(stuff.json()['jobId'])
    
            w.write(str(stuff.json()['jobId']) + "\n")
            print "completed " + str(stuff.json()['jobId'])
            calls_made.append(k)
    return calls_made

myData = create_batch("C:\\Users\\gus\\workspace\\awhere\\Sofia_Data\\NG_1990_anonymized.csv", 1980, 1991)

print myData['2'][0]

myCalls = make_call(myData, "C:\\Users\\gus\\workspace\\awhere\\app_v2\\ng_1990_ids.txt")

print len(myCalls)
print myCalls[0]
"""