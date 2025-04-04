import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
logger = logging.getLogger(__name__)


class S3Connector:
    """ Class to handle S3 connections and uploads"""

    def __init__(self, bucket_name, aws_region, aws_profile):
        logger.info('Initializing S3 client with IAM role')
        self.aws_region = aws_region
        self.aws_profile = aws_profile
        self.bucket_name = bucket_name
        session = boto3.Session(profile_name=self.aws_profile)
        # increase the max pool connections to 50, default is 10, this speeds up the upload process
        config = Config(max_pool_connections=50)
        self.s3_client = session.client(
            's3', region_name=self.aws_region, config=config)

        self.archive_key = None
        self.archive_url = None

    def push_to_s3(self, files, prefix="") -> bool:
        """ Method to push files to S3. Uses ThreadPoolExecutor to upload files in parallel"""
        try:
            # Ensure files is a list
            if not isinstance(files, list):
                files = [files]

            def upload_file(file_path):
                try:
                    s3_key = f"{prefix}/{file_path}".lstrip("/")
                    extra_args = {}
                    if prefix == 'archive':
                        extra_args['StorageClass'] = 'STANDARD_IA'
                    self.s3_client.upload_file(
                        file_path, self.bucket_name, s3_key, ExtraArgs=extra_args)
                    logger.debug("File %s uploaded to %s/%s",
                                 file_path, self.bucket_name, s3_key)
                    return s3_key
                except Exception as e:
                    logger.error("Failed to upload %s: %s", file_path, e)
                    return None

            with ThreadPoolExecutor() as executor:
                results = list(executor.map(upload_file, files))

            # get the archive key for logging later
            if prefix == 'archive' and any(results):
                self.archive_s3_key = f"s3://{self.bucket_name}/{results[-1]}"
                self.archive_url = f"https://{self.bucket_name}.s3.{self.aws_region}amazonaws.com/{results[-1]}"
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
    def get_archive_url(self) -> str:
        """ Simple method to fetch the public URL for DB logging"""
        return self.archive_url
