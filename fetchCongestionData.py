#Alex Rose
#CS510 Explorations in Data Science
#This scripts fetches the needed data from PORTAL
#and saves it locally as a csv
from datetime import date, timedelta
import sys, psycopg2

select = """SELECT distinct 
                   starttime, 
                   aggregated_data.detector_id, 
                   speed
              FROM freeway.aggregated_data, 
                   detectors, 
                   stations, 
                   highways
             WHERE aggregated_data.detector_id = detectors.detectorid
               AND detectors.stationid = stations.stationid
               AND stations.highwayid = highways.highwayid
               AND starttime >= '{}' AND starttime < '{}'
               AND highwayname = '{}'
               AND direction = '{}'
               AND resolution = '00:05:00'"""

def allsundays(year):
    d = date(year, 1, 1)
    d+= timedelta(days = 6 - d.weekday())
    while d.year == year:
        yield d
        d += timedelta(days = 7)

def allsaturdays(year):
    d = date(year, 1, 1)
    d += timedelta(days = 5 - d.weekday())
    while d.year == year:
        yield d
         += timedelta(days = 7)

try:
    conn = psycopg2.connect(host="speedbump.its.pdx.edu", dbname="portals", user="class_arose", password="<redacted>")
    cur = conn.cursor()
except:
    print('Couldn\'t connect to database!')
    exit(1)

for year in range(2012, 2019):
    for (highway, direction) in [('I-205', 'NORTH'), ('I-84', 'WEST'), ('US26', 'EAST'):
        for d in allsaturdays(year):
            query = select.format(d, d+timedelta(days=1), highway, direction)
            sql = "COPY ({}) TO STDOUT WITH CSV DELIMITER ','".format(query)
            with open("data/{}/saturdays/{}/{}.csv".format(year,d), "w") as file:
                cur.copy_expert(sql, file)

        for d in allsundays(year):
            query = select.format(d, d+timedelta(days=1), highway, direction)
            sql = "COPY ({}) TO STDOUT WITH CSV DELIMITER ','".format(query)
            with open("data/{}/sundays/{}/{}.csv".format(year,d), "w") as file:
                cur.copy_expert(sql, file)
    
cur.close()
conn.close()
