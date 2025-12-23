from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC = 'airflow_topic'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def producer_task():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {
            'timestamp': current_time,
            'message': 'Data from Producer',
            'value': 100
        }
        
        producer.send(TOPIC, value=data)
        producer.flush()
        print(f"Sent data: {data}")
        producer.close()
    except Exception as e:
        print(f"Error in producer: {e}")
        raise e   
    
def hello_world():
    print("-------------------------------------------------")
    print("HELLO WORLD - Dữ liệu đã được gửi đi thành công!")
    print("-------------------------------------------------")
    
def consumer_task():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    
    messages = []
    print(f"Wating message from topic: {TOPIC}")
    
    for message in consumer:
        messages.append(message.value)
        print(f"Received: {message.value}")
        
        if len(messages) >= 5:
            break
    
    consumer.close()
    if not messages:
        print("Dont have any messages")
        
def goodbye():
    print("-------------------------------------------------")
    print("GOODBYE - Đã hoàn thành việc đọc dữ liệu.")
    print("-------------------------------------------------")
    
with DAG(
    'kafka_producer',
    default_args=default_args,
    description='Publish message',
    schedule=timedelta(seconds=15),
    catchup=False,
    tags=['kafka', 'demo']
) as dag1:
    t1_publish = PythonOperator(
        task_id='publish_data_to_kafka',
        python_callable=producer_task
    )
    
    t2_hello = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world
    )
    
    t1_publish >> t2_hello

with DAG(
    'kafka_consumer',
    default_args=default_args,
    description='Read data from kafka broker',
    schedule=timedelta(seconds=30),
    catchup=False,
    tags=['kafka', 'demo']
) as dag2:
    t3_consume = PythonOperator(
        task_id='consumer_task',
        python_callable=consumer_task
    )
    
    t4_goodbye = PythonOperator(
        task_id='goodbye',
        python_callable=goodbye
    )
    
    t3_consume >> t4_goodbye