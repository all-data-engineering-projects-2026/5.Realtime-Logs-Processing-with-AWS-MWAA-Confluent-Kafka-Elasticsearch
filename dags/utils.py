# import json
# import logging
#
# import boto3
# from botocore.exceptions import ClientError
#
# logger = logging.getLogger(__name__)
#
#
# def get_secret(secret_name, region_name='us-east-1'):
#     """Retrieve secrets from AWS secret Manager"""
#     session = boto3.session.Session()
#     client = session.client(service_name='secretsmanager', region_name=region_name)
#     try:
#         response = client.get_secret_value(SecretId=secret_name)
#         return json.loads(response['SecretString'])
#     except ClientError as e:
#         logger.error(f"Secret retrieval error : {e}")
#         raise
