select latitude_degrees, latitude_minutes, longitude_degrees, longitude_minutes, TD_hr, count(temp) as observations_temp, to_char(avg(temp), '90.99') as mean_temp,
min(temp) as min_temp, max(temp) as max_temp, to_char(stddev(temp), '990.9999') as std_dev_temp,
count(gear_depth) as observations_depth, ROUND(avg(gear_depth),2) as mean_depth,
min(gear_depth) as min_depth, max(gear_depth) as max_depth, to_char(stddev(gear_depth), '990.9999') as std_dev_depth
from
(
select TRIP_ID, EFFORT_NUM, TD_DATETIME, TEMP, trunc(TD_DATETIME, 'hh24') TD_hr, GEAR_DEPTH, GPS_DATETIME, trunc (GPS_DATETIME, 'hh24') GPS_hr,  
floor(latitude) latitude_degrees,
trunc(mod(latitude,1)*60) latitude_minutes,
ceil(LONGITUDE) longitude_degrees,
trunc(mod(LONGITUDE*-1,1)*60) longitude_minutes,
TOW_EVENT_CODE
from 
(select TRIP_ID, PROBE_LOG_ID, MANUFACTURER, EFFORT_NUM, GEAR_CODE, TD_DATETIME, TEMP, GEAR_DEPTH, GPS_DATETIME, LATITUDE, LONGITUDE, TOW_EVENT_CODE
from
(select a.TRIP_ID, a.PROBE_LOG_ID, b.MANUFACTURER, a.EFFORT_NUM, a.GEAR_CODE, a.TD_DATETIME,TEMP, a.GEAR_DEPTH, a.GPS_DATETIME, a.LATITUDE, a.LONGITUDE, a.TOW_EVENT_CODE
from
(select x.TRIP_ID, x.PROBE_LOG_ID, x.EFFORT_NUM, y.GEAR_CODE, x.TD_DATETIME, x.TEMP, x.GEAR_DEPTH, x.GPS_DATETIME, x.LATITUDE, x.LONGITUDE, x.TOW_EVENT_CODE
from
(select TRIP_ID, PROBE_LOG_ID, EFFORT_NUM, TD_DATETIME,TEMP, GEAR_DEPTH, GPS_DATETIME, LATITUDE, LONGITUDE,TOW_EVENT_CODE
from gte_join)x
inner join
(select TRIP_ID,EFFORT_NUM,GEAR_CODE
from gte_efforts)y
on x.TRIP_ID = y.TRIP_ID
and
x.EFFORT_NUM = y.EFFORT_NUM
and
y.GEAR_CODE not in ('097OTM','112PTM')) a
inner join
(select LOG_ID, MANUFACTURER from NERS.TD_PROBE_INFO)b
on a.PROBE_LOG_ID = b.LOG_ID)
where MANUFACTURER not in ('LOWELL INSTRUMENTS', 'MOANA')
and TOW_EVENT_CODE is null)
)
group by trip_id, latitude_degrees, latitude_minutes, longitude_degrees, longitude_minutes, TD_hr;
