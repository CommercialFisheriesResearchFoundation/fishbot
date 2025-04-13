## Fishing Industry Shared Bottom Oceanographic Timeseries

## About
FIShBOT is a python program deployed as a docker container on AWS lambda. The program is configured to run daily and synchronize multiple data sources and add them to the fishbot_realtime dataset dynamically. 

Before pushing the latest aggregated dataset to S3, the current publicly available FISHBOT dataset is archived. The url to access the complete dataset is available as a url in the fishbot_archive dataset. Therefore all previous versions of the dataset are accessible in a static manner for future analysis and reference. 

## Background
What is FIShBOT? Fishing Industry Shared Bottom Oceanographic Timeseries (FIShBOT) is a standardized in-situ gridded bottom temperature product derived from cooperative research data streams.
Why do we need FIShBOT? The oceanography of the northwest Atlantic is extremely dynamic. Remotely sensed products (satellite images) can only provide surface measurements of temperature and modeled bottom temperature products are often at coarse spatial scales which are not always representative of the habitats commercial species are exposed to. Researchers and the fishing community alike share the need to better understand and monitor ocean conditions in our region. 
How did we make FIShBOT? We combined in-situ bottom temperature data from three cooperative research programs that partner with commercial fishing vessels. All the data was filtered to include only records from bottom-fishing gears and standardized to a common spatial and temporal resolution. The resulting product is a daily grid where each grid cell (7km2/4.35mi2) contains the average of all the bottom temperature sensor measurements in a given day.

Disclaimer: This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ‘as is’ basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.


### Refresh Interval

Fishbot is updated dynamically based on the calendar. If a dataset is not publicly accessible at the time of program start (midnight EST) then those data will not be available in FishBot_realtime.

Daily, the last 5 days of data are synchronized across all available datasets.
Bi-weekly, the last 30 days of data are synchronized across all available datasets.
Quarterly, the last 365 days of data are synchronized across all available datasets.
Annually, all data are completely refreshed from  all available datasets.

The dynamic refresh interval balances making real-time data accessible quickly and optimizing storage and performance. The contributing groups to fishbot rely on a rolling QA/QC schedule and often receive data at intermittent intervals from the fleet. To capture the dynamic nature of data contributing to FISHBOT, the program itself must be dynamic to reflect perpetual QA/QC efforts. 

## See this live

FIShBOT is deployed on AWS Lambda and aggregates the data to an S3 bucket. The files are synced to the erddap server daily at midnight EST.
Access the data aggregated to [FIShBOT Realtime](https://erddap.ondeckdata.com/erddap/tabledap/fishbot_realtime.html?time%2Ctemperature%2Cstat_area&time%3E=now-14days&time%3C=now&.draw=markers&.marker=5%7C5&.color=0x000000&.colorBar=%7CC%7C%7C%7C%7C&.bgColor=0xffccccff)

For static archive access see the companion archive dataset [FIshBOT Archive](https://erddap.ondeckdata.com/erddap/tabledap/fishbot_archive.html)

### Recent Data Served from ERDDAP
![Live bottom temperature - last 2 weeks](https://erddap.ondeckdata.com/erddap/tabledap/fishbot_realtime.largePng?latitude,longitude,temperature&time>=now-14days&time<now&.draw=markers&.marker=5%7C5&.color=0x000000&.colorBar=%7CC%7C%7C%7C%7C&.bgColor=0xffccccff)