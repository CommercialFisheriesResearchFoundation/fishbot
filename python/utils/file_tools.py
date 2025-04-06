import os
import shutil
import logging
import subprocess
import requests
logger = logging.getLogger(__name__)

def move_files(file_list, destination_dir) -> None:
    """
    Moves files from a list to a destination directory while preserving their subdirectory structure.

    :param file_list: List of file paths to move.
    :param destination_dir: Destination directory where files will be moved.
    """
    destination_dir = destination_dir + '/datasets/fishbot/'
    if isinstance(file_list, str):
        file_list = [file_list]

    logger.info('Moving nc files to %s', destination_dir)
    for file_path in file_list:
        if not os.path.isfile(file_path):
            logger.warning("Skipping %s: Not a valid file.", file_path)
            continue

        # Get the relative path of the file
        relative_path = os.path.relpath(file_path, start=os.path.commonpath(file_list))
        destination_path = os.path.join(destination_dir, relative_path)

        # Create the destination subdirectories if they don't exist
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # Move the file, overwriting if it already exists
        if os.path.exists(destination_path):
            os.remove(destination_path)
        shutil.move(file_path, destination_path)
        logger.debug('Moved %s to %s', file_path, destination_path)
    logger.info('Finished moving files.')

def reload_erddap(erddap_path, dataset_id) -> None:
    try:
        subprocess.run(['touch', f'{erddap_path}/erddap_data/flag/{dataset_id}'], check=True)
        logger.info('ERDDAP reloaded successfully!')
    except Exception as e:
        logger.error('Could not reload ERDDAP: %s', e, exc_info=True)
        raise e
    
def test_erddap_archive() -> bool:
    server = 'https://erddap.ondeckdata.com/erddap/'
    dataset_id = 'fishbot_realtime'
    url = f"{server}tabledap/{dataset_id}.html"
    try:
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            logger.info("ERDDAP dataset is reachable: %s", url)
            return True
        else:
            logger.warning("ERDDAP dataset is not reachable. Status code: %d", response.status_code)
            return False
    except requests.RequestException as e:
        logger.error("Error connecting to ERDDAP: %s", e)
        return False