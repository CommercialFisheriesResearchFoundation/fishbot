__author__ = 'Linus Stoltz | Data Manager, CFRF'
__doc__ = 'program to assimilate regional data collected by NOAA and CFRF'
import logging
from logging.handlers import TimedRotatingFileHandler
from utils.erddap_connector import get_latest_data
from utils.spatial_tools import gridify_data, get_shell


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler
handler = TimedRotatingFileHandler('application.log', when='midnight', interval=1, backupCount=7)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# Add the handler to the logger
# logger.addHandler(handler)
logging.basicConfig(level=logging.INFO, handlers=[handler])


def main():
    df = get_latest_data('2023-01-01T00:00:00Z')
    logger.info('Data retrieved successfully!')
    gridify_data(df)
    # ds = get_shell()
    
    # ds.to_netcdf('test_grid.nc')



if __name__ == '__main__':
    main()