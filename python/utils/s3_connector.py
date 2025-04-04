import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
logger = logging.getLogger(__name__)


class S3Connector:
    """ Class to handle S3 connections and uploads"""

    def __init__(self):
        logger.info('Initializing S3 client with IAM role')
        #! DO NOT PUSH THIS PROFILE NAME TO GITHUB
        session = boto3.Session(profile_name='linus_dev_cfrf')
        # increase the max pool connections to 50, default is 10, this speeds up the upload process
        config = Config(max_pool_connections=50)
        self.s3_client = session.client(
            's3', region_name='us-east-1', config=config)

        self.archive_key = None

    def push_to_s3(self, files, bucket_name, prefix="") -> bool:
        """ Method to push files to S3. Uses ThreadPoolExecutor to upload files in parallel"""
        try:
            # Ensure files is a list
            if not isinstance(files, list):
                files = [files]

            def upload_file(file_path):
                try:
                    s3_key = f"{prefix}/{file_path}".lstrip("/")
                    self.s3_client.upload_file(file_path, bucket_name, s3_key)
                    logger.debug("File %s uploaded to %s/%s",
                                file_path, bucket_name, s3_key)
                    return s3_key
                except Exception as e:
                    logger.error("Failed to upload %s: %s", file_path, e)
                    return None

            with ThreadPoolExecutor() as executor:
                results = list(executor.map(upload_file, files))

            # get the archive key for logging later
            if prefix == 'archive' and any(results):
                self.archive_key = f"s3://{bucket_name}/{results[-1]}"
            logger.info("All files uploaded successfully")
            return all(results)
        except NoCredentialsError:
            logger.error("Error: AWS credentials not available.")
        except PartialCredentialsError:
            logger.error("Error: Incomplete AWS credentials provided.")
        except Exception as e:
            logger.error("An error occurred: %s", e)
        return False

    def get_archive_key(self) -> str:
        """ Simple method to fetch the S3 archvie key for DB logging"""
        return self.archive_key
