SELECT distinct starttime, detector_id, speed
  FROM freeway.aggregated_data,
       detectors,
       stations,
       highways
 WHERE aggregated_data.detector_id = detectors.detectorid
   AND detectors.stationid = stations.stationid
   AND stations.highwayid = highways.highwayid
   AND starttime >= '2018-05-05'
   AND starttime < '2018-05-06'
   AND highwayname = 'I-84'
   AND direction = 'WEST'
   AND resolution = '00:05:00';
