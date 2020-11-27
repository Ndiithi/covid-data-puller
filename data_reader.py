from datetime import datetime
import csv
import requests
import logging

f = open('/home/fintan/covid_data/covid_formated2.csv', 'w')
r = requests.get('https://services7.arcgis.com/1Cyg6S9yGgIqdFPO/arcgis/rest/services/Covid_Kenya__Cases_Updated/FeatureServer/0/query?f=json&where=(cumulative_recovered_cases%3E0%20OR%20cumulative_death_cases%3E0)&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=date%20asc&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true')

log = logging.getLogger("covid data reader")
logging.basicConfig(filename='covid_fetcher.log',format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',level=logging.DEBUG)

with f:
    writer = csv.writer(f)

    if r.status_code == 200:
        data = r.json()
        log.info("fetched data")
        date_county={}
        date_cummulative = {}
        fnames = ['date', 'county','long','lat','nationality','day','confirmed_cases','death_cases','recovered_cases','cumulative_confirmed_cases','cumulative_death_cases','cumulative_recovered_cases','active_cases','ObjectId']
        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()

        log.info("getting max cummulative numbers")

        for _meta in data['features']:
            timestamp = _meta['attributes']['date']
            try:
                dt_object = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                cum_key = "'%s'" % (dt_object)
                if cum_key in date_cummulative:
                    if date_cummulative[cum_key]['cumulative_confirmed_cases'] < _meta['attributes']['cumulative_confirmed_cases']:
                        date_cummulative[cum_key]['cumulative_confirmed_cases']= _meta['attributes']['cumulative_confirmed_cases']

                    if date_cummulative[cum_key]['cumulative_death_cases'] < _meta['attributes']['cumulative_death_cases']:
                        date_cummulative[cum_key]['cumulative_death_cases'] = _meta['attributes']['cumulative_death_cases']

                    if date_cummulative[cum_key]['cumulative_recovered_cases'] < _meta['attributes']['cumulative_recovered_cases']:
                        date_cummulative[cum_key]['cumulative_recovered_cases'] = _meta['attributes']['cumulative_recovered_cases']
                else:
                    date_cummulative[cum_key]={}
                    date_cummulative[cum_key]['cumulative_confirmed_cases'] = _meta['attributes']['cumulative_confirmed_cases']
                    date_cummulative[cum_key]['cumulative_death_cases'] = _meta['attributes']['cumulative_death_cases']
                    date_cummulative[cum_key]['cumulative_recovered_cases'] = _meta['attributes']['cumulative_recovered_cases']

            except Exception as e:
                log.info(e)



        log.info("aggregating to daily cases")
        for _meta in data['features']:
            timestamp = _meta['attributes']['date']
            try:
                dt_object = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                _meta['attributes']['date']=dt_object
                key = "'%s:%s'" % (_meta['attributes']['date'],_meta['attributes']['county'])
                if key in date_county:
                    if 'confirmed_cases' in date_county[key]:
                        date_county[key]['confirmed_cases']+=_meta['attributes']['confirmed_cases']
                    else:
                        date_county[key]['confirmed_cases'] = _meta['attributes']['confirmed_cases']

                    if 'death_cases' in date_county[key]:
                        date_county[key]['death_cases']+=_meta['attributes']['death_cases']
                    else:
                        date_county[key]['death_cases'] = _meta['attributes']['death_cases']

                    if 'recovered_cases' in date_county[key]:
                        date_county[key]['recovered_cases']+=_meta['attributes']['recovered_cases']
                    else:
                        date_county[key]['recovered_cases'] = _meta['attributes']['recovered_cases']

                else:
                    date_county[key]=_meta['attributes']
                print date_cummulative
                cum_key = "'%s'" % (dt_object)
                date_county[key]['cumulative_confirmed_cases'] = date_cummulative[cum_key]['cumulative_confirmed_cases']
                date_county[key]['cumulative_death_cases'] = date_cummulative[cum_key]['cumulative_death_cases']
                date_county[key]['cumulative_recovered_cases'] = date_cummulative[cum_key]['cumulative_recovered_cases']

                date_county[key]['active_cases']=date_county[key]['cumulative_confirmed_cases']-(date_county[key]['cumulative_death_cases']-date_county[key]['cumulative_recovered_cases'])

            except Exception as e:
                log.info(e)

        log.info("writting data to file")

        for key in date_county:
            log.info(date_county[key])
            writer.writerow(date_county[key])

    else:
        log.error("failed to fetched covid data")






