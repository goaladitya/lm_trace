import boto3
import logging as logger


class S3Connection:
    def __init__(self):
        self.S3_CLIENT = None

    def get_s3(self, access_key=None, secret_key=None):
        """
        Get the boto3 s3 connection
        """
        if not self.S3_CLIENT:
            try:
                logger.info("Getting the connection to AWS S3")
                self.S3_CLIENT = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            except Exception as e:
                logger.error("Error occured while connecting to S3 : %s", e)
                raise e
        return self.S3_CLIENT


def _download_from_s3(s3_client, bucket, key, local_path):
    """
    Downloads data from s3 bucket
    """
    try:
        logger.info("Downloading from bucket %s using key %s", bucket, key)
        s3_client.download_file(bucket, key, local_path)
    except Exception as err:
        logger.error("Error occured while downloading from S3 bucket: %s key: %s as:: %s", bucket, key, err)


def _get_from_s3(s3_client, bucket, key):
    """
    Reads data from s3 bucket
    """
    data = s3_client.get_object(Bucket=bucket, Key=key)
    if 'Body' in data:
        logger.info("Data found in bucket %s using key %s", bucket, key)
        return data['Body'].read()
    else:
        logger.error("No data found for bucket %s using key %s", bucket, key)
        return None


def _put_to_s3(s3_client, body, bucket, key):
    """
    puts data to s3 bucket
    """
    # write to s3
    try:
        res = s3_client.put_object(Body=body, Bucket=bucket, Key=key)
        logger.info("Put file to bucket: %s key: %s response: %s", bucket, key, res)
    except Exception as err:
        logger.error("Error occured while writing to S3 bucket: %s key: %s as %s", bucket, key, err)
