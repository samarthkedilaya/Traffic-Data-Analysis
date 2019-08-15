#Alex Rose
#CS510 Explorations in Data Science
from datetime import date, timedelta
import sys, psycopg2

#This query averages the speed metric for a given highway,
#across all mileposts, for a given year 
select = """SELECT cast(starttime as time), 
                   detectors.milepost, 
                   ROUND(AVG(speed), 2)
              FROM traffic.traffic, 
                   traffic.detectors, 
                   traffic.stations, 
                   traffic.highways
             WHERE traffic.detector_id = detectors.detectorid 
               AND detectors.stationid = stations.stationid 
               AND stations.highwayid = highways.highwayid 
               AND highwayname = '{}'
               AND starttime >= '{}-01-01' 
               AND starttime < '{}-01-01' 
               AND extract(dow from starttime) = {} 
               AND speed <> 0
          GROUP BY cast(starttime as time), detectors.milepost
          ORDER BY starttime"""

try:
    conn = psycopg2.connect(host="db.cecs.pdx.edu", dbname="alexrose", user="alexrose", password="<redacted>")
    cur = conn.cursor()
except:
    print('Couldn\'t connect to database!')
    exit(1)

for year in range(2012, 2019):
    for highway in ('I-84', 'I-205', 'US26'):
        #Saturdays
        query = select.format(highway, year, year+1, 6)
        sql = "COPY ({}) TO STDOUT WITH CSV DELIMITER ','".format(query)
        print("Averaging Saturdays from {} on {}".format(year, highway))
        with open("data/heatmap/Saturdays{}_{}.csv".format(highway,year), "w") as file:
            cur.copy_expert(sql, file)
        print("saved data/heatmap/Saturdays{}_{}.csv".format(highway, year))

        #Sundays
        query = select.format(highway, year, year+1, 0)
        sql = "COPY ({}) TO STDOUT WITH CSV DELIMITER ','".format(query)
        print("Averaging Sundays from {} on {}".format(year, highway))
        with open("data/heatmap/Sundays{}_{}.csv".format(highway,year), "w") as file:
            cur.copy_expert(sql, file)
        print("saved data/heatmap/Sundays{}_{}.csv".format(highway, year))

    
cur.close()
conn.close()
