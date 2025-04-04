
from erddapy import ERDDAP
import logging
import asyncio
import pandas as pd
import xarray as xr
logger = logging.getLogger(__name__)


def truncate_at_first_space(col_name):
    return col_name.split(' ')[0]

class ERDDAPClient:
    def __init__(self, datasets=None, start_time='2025-03-25T00:00:00Z'):
        self.datasets = datasets
        self.start_time = start_time

    async def fetch_data(self, server, dataset_id, protocol, response, start_time, constraints=None)-> pd.DataFrame:
        logger.info(f"Retrieving data from %s for dataset %s",server,dataset_id)
        e = ERDDAP(server=server, protocol=protocol)
        e.dataset_id = dataset_id
        e.constraints = {
            'time>=': start_time,
            'time<=': pd.Timestamp.now().isoformat()
        }
        if constraints:
            e.constraints.update(constraints)
        e.response = response

        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, e.to_pandas)
            df.columns = [truncate_at_first_space(col) for col in df.columns]
            df['time'] = pd.to_datetime(df['time'])
            try:
                df['time'] = df['time'].dt.tz_localize(None)
            except Exception as e:
                logger.warning("Error localizing time: %s", e)
                pass
            logger.info("Data retrieved successfully for %s",dataset_id)
            return df
        
        except Exception as e:
            # for some reason, unable to parse error from erddapy libary, convert to string then parse for missing data
            e = str(e)
            if "outside of the variable's actual_range" in e:
                logger.warning("No new data for %s", dataset_id)
                return pd.DataFrame() # return an empty dataframe if no new data is available
            elif "The requested URL was not found on this server." in e:
                logger.warning("Dataset %s not found on server %s", dataset_id, server)
                return None
            elif "Name or service not known" in e:
                logger.warning("Server %s not found", server)
                return None
            else: 
                logger.error("Error retrieving data for %s: %s",dataset_id, e)
                raise

    async def fetch_all_data(self)-> list:
        tasks = []
        for key, value in self.datasets.items():
            server = value["server"]
            for dataset_id, protocol, response in zip(value["dataset_id"], value["protocol"], value["response"]):
                constraints = value.get("constraints", {}).get(dataset_id, {})
                tasks.append(self.fetch_data(server, dataset_id, protocol, response, self.start_time, constraints))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    
    def archive_fishbot(self,current_time, version)-> xr.Dataset:
        server = 'https://erddap.ondeckdata.com/erddap/'
        try:
            e = ERDDAP(
                server=server,
                protocol="tabledap",
                response="nc",
            )
            e.dataset_id = 'fishbot_realtime'
  
            ds = e.to_xarray()
            ds.attrs['version'] = version
            ds.attrs['archive_time'] = current_time
            file_name = f"fishbot_archive_{str(current_time).split('T')[0]}.nc"
            ds.to_netcdf(file_name)
        except Exception as e:
            logger.error("Error could not fetch fishbot data for archive. Exiting program: %s", e)
            raise
        return file_name

