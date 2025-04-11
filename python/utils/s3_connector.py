import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging
import os
import xarray as xr
import tempfile
from botocore.config import Config
logger = logging.getLogger()


class S3Connector:
    """ Class to handle S3 connections and uploads"""

    def __init__(self, bucket_name, aws_region):
        logger.info('Initializing S3 client with IAM role')
        self.aws_region = aws_region
        self.bucket_name = bucket_name

        session = boto3.Session()
        # increase the max pool connections to 50, default is 10, this speeds up the upload process
        config = Config(max_pool_connections=50)
        self.s3_client = session.client(
            's3', region_name=self.aws_region, config=config)

        self.archive_key = None
        self.archive_url = None
        self.archive_s3_key = None

    def push_file_to_s3(self, file, s3_key, content_type="application/octet-stream") -> str:
        """ Method to push a file object to S3 """
        try:
            extra_args = {'ContentType': content_type}
            logger.info("Uploading file to s3://%s/%s", self.bucket_name, s3_key)
            self.s3_client.upload_fileobj(file, self.bucket_name, s3_key, ExtraArgs=extra_args)
            logger.debug("file uploaded to s3://%s/%s", self.bucket_name, s3_key)
            return f"s3://{self.bucket_name}/{s3_key}"
        except (NoCredentialsError, PartialCredentialsError) as cred_error:
            logger.error("AWS credentials error: %s", cred_error)
            raise
        except Exception as e:
            logger.error("Failed to upload file: %s", e)
            raise

    def archive_fishbot(self,ds, current_time, version, prefix='archive')-> float:
        # server = 'https://erddap.ondeckdata.com/erddap/'
        if not isinstance(ds, xr.Dataset):
            raise TypeError(f"Expected ds to be xarray Dataset, got {type(ds).__name__}")
        if not isinstance(prefix, str):
            raise TypeError(f"Expected 'prefix' to be str, got {type(prefix).__name__}")
        if not isinstance(version, str):
            raise TypeError(f"Expected 'version' to be str, got {type(version).__name__}")
        
        ds.attrs['version'] = version
        ds.attrs['archive_time'] = current_time

        try:
            with tempfile.NamedTemporaryFile(suffix=".nc", dir="/tmp", delete=False) as tmp:
                ds.to_netcdf(tmp.name, mode="w", engine="scipy")
                archvie_file_size = round(os.path.getsize(tmp.name) / (1024 * 1024), 1)  # Convert bytes to MB

            with open(tmp.name, 'rb') as f:
                s3_key = f"{prefix}/{str(current_time).split('T')[0]}.nc"
                self.push_file_to_s3(f, s3_key, content_type="application/netcdf")
               
        except Exception as e:
            logger.error("Failed to upload to S3: %s", e)
            raise

        self.archive_s3_key = f"s3://{self.bucket_name}/{s3_key}"
        self.archive_url = f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/{s3_key}"
        return archvie_file_size


    def get_archive_key(self) -> str:
        """ Simple method to fetch the S3 archvie key for DB logging"""
        return self.archive_s3_key
    def get_archive_url(self) -> str:
        """ Simple method to fetch the public URL for DB logging"""
        return self.archive_url
