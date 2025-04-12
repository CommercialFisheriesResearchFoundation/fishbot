# FIShBOT

## About
FIShBOT is a python program deployed as a docker container on AWS lambda. The program is configured to run daily and synchronize multiple data sources and add them to the fishbot_realtime dataset dynamically. 

Before pushing the latest aggregated dataset to S3, the current publicly available FISHBOT dataset is archived. The url to access the complete dataset is available as a url in the fishbot_archive dataset. Therefore all previous versions of the dataset are accessible in a static manner for future analysis and reference. 

### Refresh Interval

Fishbot is updated dynamically based on the calendar. If a dataset is not publicly accessible at the time of program start (midnight EST) then those data will not be available in FishBot_realtime.

Daily, the last 5 days of data are synchronized across all available datasets.
Bi-weekly, the last 30 days of data are synchronized across all available datasets.
Quarterly, the last 365 days of data are synchronized across all available datasets.
Annually, all data are completely refreshed from  all available datasets.

The dynamic refresh interval balances making real-time data accessible quickly and optimizing storage and performance. The contributing groups to fishbot rely on a rolling QA/QC schedule and often receive data at intermittent intervals from the fleet. To capture the dynamic nature of data contributing to FISHBOT, the program itself must be dynamic to reflect perpetual QA/QC efforts. 

## See this live
![Bottom temperature from the past 2 weeks](https://erddap.ondeckdata.com/erddap/tabledap/fishbot_realtime.graph?latitude%2Clongitude%2Ctemperature&time%3E=now-14days&time%3C=now&.draw=markers&.marker=5%7C5&.color=0x000000&.colorBar=%7CC%7C%7C%7C%7C&.bgColor=0xffccccff)



