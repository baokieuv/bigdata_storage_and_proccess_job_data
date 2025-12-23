from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
from confluent_kafka import Producer, Consumer, KafkaException

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
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
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
        })
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {
            'timestamp': current_time,
            'message': 'Data from Producer',
            'value': 100
        }
        
        producer.produce(TOPIC, json.dumps(data).encode("utf-8"))
        producer.flush()
        
        print(f"Sent data: {data}")
        
    except Exception as e:
        print(f"Error in producer: {e}")
        raise e   
    
def hello_world():
    print("-------------------------------------------------")
    print("HELLO WORLD - Dữ liệu đã được gửi đi thành công!")
    print("-------------------------------------------------")
    
def consumer_task():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "auto.offset.reset": 'earliest',
        "group.id": 'consumer_group',
        "enable.auto.commit": True
    })
    
    consumer.subscribe([TOPIC])
    print(f"Wating message from topic: {TOPIC}")
    
    msg = consumer.poll(5.0)
    
    if msg is None:
        print("No new message")
    elif msg.error():
        print("Consumer error: ", msg.error())
    else:
        value = json.loads(msg.value().decode("utf-8"))
        print("Received message:", value)
        
        consumer.commit(msg)
    
    consumer.close()
        
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