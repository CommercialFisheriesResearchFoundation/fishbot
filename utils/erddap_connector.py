
from erddapy import ERDDAP
import logging
import asyncio
import pandas as pd
logger = logging.getLogger()


def truncate_at_first_space(col_name):
    return col_name.split(' ')[0]

class ERDDAPClient:
    def __init__(self, datasets=None, start_time='2025-03-25T00:00:00Z'):
        self.datasets = datasets
        self.start_time = start_time

    async def fetch_data(self, server, dataset_id, protocol, response, start_time, constraints=None, variables=None):
        time_var = 'time'
        if dataset_id == 'ocdbs_v_erddap1':
            time_var = 'UTC_DATETIME'
        logger.info(f"Retrieving data from %s for dataset %s",server,dataset_id)
        e = ERDDAP(server=server, protocol=protocol)
        e.dataset_id = dataset_id
        if dataset_id != 'fishbot_realtime':
            e.constraints = {
                f'{time_var}>=': start_time,
                f'{time_var}<=': pd.Timestamp.now().isoformat()
            }
        # Add constraints if provided 
        if constraints:
            e.constraints.update(constraints)

        # Trim requests for only needed variables
        if variables:
            e.variables = variables
        e.response = response

        try:
            loop = asyncio.get_event_loop()
            if dataset_id == 'fishbot_realtime':
                df = await loop.run_in_executor(None, e.to_xarray)
            else:
                df = await loop.run_in_executor(None, e.to_pandas)
                df.columns = [truncate_at_first_space(col) for col in df.columns]
                df.rename(columns={time_var: 'time'}, inplace=True)
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
                logger.warning("Server %s not found when trying to fetch %s", server, dataset_id)
                return None
            elif "The proxy server received an invalid response from an upstream server." in e:
                logger.warning("Server %s not responding when trying to access %s", server, dataset_id)
                return None
            elif "Not Found: Currently unknown datasetID" in e:
                logger.warning("Dataset %s not found on server %s", dataset_id, server)
                return None
            elif "Your query produced no matching results" in e:
                logger.warning("No matching results for dataset %s", dataset_id)
                return None
            else: 
                logger.error("Error retrieving data for %s: %s",dataset_id, e)
                raise

    async def fetch_all_data(self) -> list:
        tasks = []
        try:
            for key, value in self.datasets.items():
                server = value["server"]
                variables_list = value.get("variables", [])
                
                for i, (dataset_id, protocol, response) in enumerate(
                    zip(value["dataset_id"], value["protocol"], value["response"])
                ):
                    constraints = value.get("constraints", {}).get(dataset_id, {})
                    variables = variables_list[i] if i < len(variables_list) else None

                    tasks.append(
                        self.fetch_data(
                            server,
                            dataset_id,
                            protocol,
                            response,
                            self.start_time,
                            constraints,
                            variables
                        )
                    )

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results

        except Exception as e:
            logger.error("Error fetching all data: %s", e)
            raise