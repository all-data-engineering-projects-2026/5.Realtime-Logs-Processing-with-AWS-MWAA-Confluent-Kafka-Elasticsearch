import re
from datetime import datetime, timedelta
import json
import logging

import boto3
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError, KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name='us-east-1'):
    """Retrieve secrets from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        logger.error(f"Secret retrieval error : {e}")
        raise

def parse_log_entry(log_entry):
    log_pattern = r'(?P<ip>[\d.]+) - - \[(?P<timestamp>.*)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)'
    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f"Invalid log format: {log_entry}")
        return None

    data = match.groupdict()
    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], '%b %d %Y, %H:%M:%S')
        data['@timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None

    return data

def consume_and_index_logs(**context):
    secrets = get_secret("MWAA_Secrets_V2")

    consumer_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }

    es = Elasticsearch(
        hosts=[secrets['ELASTICSEARCH_URL']],
        api_key=secrets['ELASTICSEARCH_API_KEY']
    )

    consumer = Consumer(consumer_config)
    topic = 'billion_website_logs'
    consumer.subscribe([topic])

    try:
        index_name = 'billion_website_logs'
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f'Created index: {index_name}')

        logs = []
        max_messages = 100  # Safety limit

        for _ in range(max_messages):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                break

            log_entry = msg.value().decode('utf-8')
            # parsed_log = parse_log_entry(log_entry)
            #
            # if parsed_log:
            #     logs.append(parsed_log)

            # Index raw log directly (no parsing temporary)
            doc = {
                "raw_log": log_entry,
                "@timestamp": datetime.utcnow().isoformat()
            }
            logs.append(doc)

            if len(logs) >= 50:
                actions = [
                    {'_op_type': 'create', '_index': index_name, '_source': log}
                    for log in logs
                ]
                success, failed = bulk(es, actions, refresh=True)
                logger.info(f'Indexed {success} logs, {len(failed)} failed')
                logs = []

        # Remaining logs
        if logs:
            actions = [
                {'_op_type': 'create', '_index': index_name, '_source': log}
                for log in logs
            ]
            bulk(es, actions, refresh=True)

    except Exception as e:
        logger.error(f"Error in consume_and_index_logs: {e}")
    finally:
        consumer.close()
        es.close()

# DAG Definition
default_args = {
    'owner': 'himanshu_airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='log_consumer_pipeline',
    default_args=default_args,
    description="Consume and Index synthetic logs",
    schedule='*/5 * * * *',
    start_date=datetime(2026, 6, 22),
    catchup=False,
    tags={'logs', 'kafka', 'elasticsearch'},
) as dag:

    consume_logs_task = PythonOperator(
        task_id='generate_and_consume_logs',
        python_callable=consume_and_index_logs,
    )
