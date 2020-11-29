from datetime import datetime
import csv
import requests
import logging

country_data_file = open('/home/fintan/covid_data/country_data_file.csv', 'w')
# country_data_file = open('/home/duncanndiithi/PycharmProjects/covid/country_data_file.csv', 'w')
# country_req = requests.get('https://services7.arcgis.com/1Cyg6S9yGgIqdFPO/arcgis/rest/services/Covid_Kenya__Cases_Updated/FeatureServer/0/query?f=json&where=(cumulative_recovered_cases%3E0%20OR%20cumulative_death_cases%3E0)&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=date%20asc&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true')
country_req = requests.get('https://services8.arcgis.com/I6vAw1FtOBh556rT/arcgis/rest/services/Corona_Cases_Kenya/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=Date+asc&outSR=102100&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true')

county_data_file = open('/home/fintan/covid_data/county_data_file.csv', 'w')
# county_data_file = open('/home/duncanndiithi/PycharmProjects/covid/county_data_file.csv', 'w')
county_req = requests.get('https://services8.arcgis.com/I6vAw1FtOBh556rT/arcgis/rest/services/COVID_Cases/FeatureServer/0/query?f=json&where=Confirmed+%3E+0&returnGeometry=false&spatialRel=esriSpatialRelIntersects&objectIds=&outFields=*&outSR=102100&cacheHint=true')

log = logging.getLogger("covid data reader")
logging.basicConfig(filename='covid_fetcher.log',format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',level=logging.DEBUG)


def extract_country_covid_data(data):
    # country_data = {}
    _data_ref ={}
    for _meta in data['features']:
        timestamp = _meta['attributes']['Date']
        country_data = {
            "date": None,
            "county": None,
            "long": None,
            "lat": None,
            "nationality": 'KENYAN',
            "day": 0,
            "confirmed_cases": None,
            "death_cases": None,
            "recovered_cases": None,
            "cumulative_confirmed_cases": None,
            "cumulative_death_cases": None,
            "cumulative_recovered_cases": None,
            "active_cases": None,
            "ObjectId": None
        }
        try:

            dt_object = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
            _meta['attributes']['Date'] = dt_object
            cum_key = "'%s:%s'" % (_meta['attributes']['Date'],"KENYA")

            country_data['date'] = dt_object
            country_data['county'] = "KENYA"
            country_data['long'] = 33.3918952
            country_data['lat'] = 0.1540805
            # country_data['nationality'] = _meta['attributes']['Confirmed']
            # country_data['day'] = _meta['attributes']['County_Name']
            country_data['confirmed_cases'] = _meta['attributes']['Confirmed_New']
            country_data['death_cases'] = _meta['attributes']['Deaths_New']
            country_data['recovered_cases'] = _meta['attributes']['Recovered_New']
            country_data['cumulative_confirmed_cases'] = _meta['attributes']['Confirmed_Total']
            country_data['cumulative_death_cases'] = _meta['attributes']['Deaths_Total']
            country_data['cumulative_recovered_cases'] = _meta['attributes']['Recovered_Total']
            country_data['active_cases'] = _meta['attributes']['Active_Total']
            country_data['ObjectId'] = 5

            _data_ref[cum_key] = country_data
            # country_data[cum_key] = _meta['attributes']
        except Exception as e:
            log.info(e)
    return _data_ref


def extract_county_covid_data(data):
    _data_ref ={}
    for _meta in data['features']:
        timestamp = _meta['attributes']['Last_Update']
        dt_object = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
        try:
            county_data= {
                "date": None,
                "county": None,
                "long": None,
                "lat": None,
                "nationality": None,
                "day": None,
                "confirmed_cases": None,
                "death_cases": None,
                "recovered_cases": None,
                "cumulative_confirmed_cases": None,
                "cumulative_death_cases": None,
                "cumulative_recovered_cases": None,
                "active_cases": None,
                "ObjectId": None
            }

            _meta['attributes']['Last_Update'] = dt_object
            cum_key = "'%s:%s'" % (_meta['attributes']['Last_Update'], _meta['attributes']['County_Name'])

            county_data['cumulative_confirmed_cases'] = _meta['attributes']['Confirmed']
            county_data['county'] = _meta['attributes']['County_Name']
            county_data['date'] = _meta['attributes']['Last_Update']

            _data_ref[cum_key] = county_data

        except Exception as e:
            log.info(e)
    return _data_ref


def write_data_to_csv(data,file):
    log.info("writting data to file")
    writer = csv.writer(file)
    heads = ['date', 'county', 'long', 'lat', 'nationality', 'day', 'confirmed_cases', 'death_cases',
              'recovered_cases', 'cumulative_confirmed_cases', 'cumulative_death_cases', 'cumulative_recovered_cases',
              'active_cases', 'ObjectId']
    writer = csv.DictWriter(file, fieldnames=heads)
    writer.writeheader()

    for key in data:
        log.info(data[key])
        writer.writerow(data[key])


#country level data pull and persist
with country_data_file:

    if country_req.status_code == 200:

        data = country_req.json()
        log.info("getting max cummulative country numbers")
        date_country=extract_country_covid_data(data)
        write_data_to_csv(date_country, country_data_file)

    else:
        log.error("failed to fetched covid data")


#country level data pull and persist
with county_data_file:
    if county_req.status_code == 200:

        data = county_req.json()
        log.info("getting max cummulative country numbers")
        date_county = extract_county_covid_data(data)
        write_data_to_csv(date_county, county_data_file)

    else:
        log.error("failed to fetched covid data")
