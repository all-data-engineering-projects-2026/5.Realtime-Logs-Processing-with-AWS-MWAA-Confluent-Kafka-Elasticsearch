import json
import random
from datetime import timedelta, datetime

import boto3
from airflow import DAG
from airflow.configuration import get_custom_secret_backend
from airflow.providers.standard.operators.python import PythonOperator
from botocore.exceptions import ClientError
from confluent_kafka import Producer
from faker import Faker
import logging

# from dags.utils import get_secret

fake = Faker()
logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name='us-east-1'):
    """Retrieve secrets from AWS secret Manager"""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        logger.error(f"Secret retrieval error : {e}")
        raise

def create_kafka_producer(config):
    return Producer(config)


def generate_log():
    """Generate synthetic log"""
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    endpoints = ['/api/users', '/home', '/about', '/contact', '/services']
    statuses = [200, 301, 302, 400, 404, 500]

    user_agents = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    ]

    referrers = ['https://www.google.com', 'https://example.com', '-', 'https://bing.com', 'https://yahoo.com']

    ip = fake.ipv4()
    timestamp = datetime.now().strftime('%b %d %Y, %H:%M:%S')
    method = random.choice(methods)
    endpoint = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(1000, 15000)
    referrer = random.choice(referrers)
    user_agent = random.choice(user_agents)

    log_entry = (
        f'{ip} - - [{timestamp}] {method} {endpoint} HTTP/1/1" {status} {size} {referrer} {user_agent}'
    )

    return log_entry


def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# TRY OUT FOR LOCAL
# def get_secret(secret_name: str, region_name: str = 'us-east-1'):
#     """Retrieve secrets from AWS Secrets Manager - works locally & in MWAA"""
#     try:
#         # Option 1: Use specific profile for local development
#         session = boto3.session.Session(profile_name='mwaa-dev')
#         logger.info("Running from actual credential - using mock secrets. Set real credentials for production.")
#
#         # Option 2: Fallback to default session (env vars or default profile)
#         # session = boto3.session.Session()
#
#         client = session.client('secretsmanager', region_name=region_name)
#
#         response = client.get_secret_value(SecretId=secret_name)
#         return json.loads(response['SecretString'])
#
#     except Exception as e:
#         logger.warning(f"AWS credential/secret error: {e}")
#
#         # === LOCAL FALLBACK (for development) ===
#         if "NoCredentialsError" in str(e) or "Credentials" in str(e):
#             logger.info("Running locally - using mock secrets. Set real credentials for production.")
#             # Mock secrets for local testing
#             return {
#                 "KAFKA_BOOTSTRAP_SERVER": "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092",
#                 "KAFKA_SASL_USERNAME": "E6JIQDVYGPSPAR3T",
#                 "KAFKA_SASL_PASSWORD": "cfltzMue3d97y6k4HkO702/aYmNLdESuf+xqrEW5WvX4Q6YGBZ0OdYyd2Q9yMORg"
#             }
#
#         logger.error(f"Failed to retrieve secret {secret_name}")
#         raise

def produce_logs(**context):
    """ Produce log entries into Kafka"""
    secrets = get_secret("MWAA_Secrets_V2")
    kafka_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 50000
    }

    producer = create_kafka_producer(kafka_config)
    topic = 'billion_website_logs'

    for _ in range(200):
        log = generate_log()
        try:
            producer.produce(topic, log.encode('utf-8'), on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f"Error producing log:{e}")
            raise

    logger.info(f"Produced 200 logs to Kafka topic {topic}")


default_args = {
    'owner': 'himanshu_airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'log_generation_pipeline',
    default_args=default_args,
    description="Generate and produce synthetic logs",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 6, 22),
    catchup=False,
    tags={'logs', 'kafka', 'production'}
)

produce_logs_task = PythonOperator(
    task_id='generate_and_produce_logs',
    python_callable=produce_logs,
    dag=dag,
)

