from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

# Hàm để lấy và update ngày xử lý tiếp theo
def get_next_process_date(**context):
    """
    Lấy ngày cần xử lý tiếp theo từ Airflow Variable.
    Bắt đầu từ 2026-01-01, mỗi lần tăng 1 ngày.
    """
    try:
        current_date_str = Variable.get("job_processing_current_date", default_var="2026-01-01")
        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
        
        # Push date vào XCom để task tiếp theo sử dụng
        context['ti'].xcom_push(key='process_date', value=current_date_str)
        
        return current_date_str
    except Exception as e:
        print(f"Error getting process date: {e}")
        raise

def update_process_date(**context):
    """
    Update ngày đã xử lý, tăng lên 1 ngày cho lần chạy tiếp theo.
    Dừng lại ở 2026-01-20.
    """
    try:
        current_date_str = context['ti'].xcom_pull(key='process_date')
        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
        
        # Kiểm tra xem đã xử lý đến ngày cuối chưa
        if current_date_str == "2026-01-20":
            print("Reached end date (2026-01-20). No more dates to process.")
            return
        
        # Tăng lên 1 ngày
        next_date = current_date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        
        # Lưu lại vào Variable
        Variable.set("job_processing_current_date", next_date_str)
        print(f"Updated next process date to: {next_date_str}")
        
    except Exception as e:
        print(f"Error updating process date: {e}")
        raise

# Default arguments cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Retry 2 lần nếu fail
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

# Định nghĩa DAG
with DAG(
    dag_id='job_processing_pipeline',
    default_args=default_args,
    description='Process daily job data from MinIO to Cassandra using Spark',
    schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Không chạy backfill các lần missed
    tags=['spark', 'minio', 'cassandra', 'batch-processing'],
) as dag:
    
    # Task 1: Lấy ngày cần xử lý
    get_date_task = PythonOperator(
        task_id='get_process_date',
        python_callable=get_next_process_date,
        provide_context=True,
    )
    
    # Task 2: Chạy Spark job trên Kubernetes
    spark_job_task = KubernetesPodOperator(
        task_id='run_spark_job',
        name='spark-job-{{ ti.xcom_pull(key="process_date") }}',
        namespace='default',
        image='phantaii/spark-job:latest',  # Thay bằng image registry của bạn
        cmds=['/opt/spark/bin/spark-submit'],
        arguments=[
            '--master', 'spark://spark-master.default.svc.cluster.local:7077',
            '--deploy-mode', 'client',
            '--conf', 'spark.driver.memory=1g',
            '--conf', 'spark.executor.memory=2g',
            '--conf', 'spark.executor.cores=1',
            '--conf', 'spark.executor.instances=2',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio.default.svc.cluster.local:9000',
            '--conf', 'spark.hadoop.fs.s3a.access.key=minioadmin',
            '--conf', 'spark.hadoop.fs.s3a.secret.key=minioadmin',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--conf', 'spark.cassandra.connection.host=cassandra.default.svc.cluster.local',
            '--conf', 'spark.cassandra.connection.port=9042',
            '--jars', '/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.367.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/spark-cassandra-connector-assembly_2.12-3.4.1.jar',
            '/opt/spark/jobs/spark_job.py',
            '--date', '{{ ti.xcom_pull(key="process_date") }}'
        ],
        labels={'app': 'spark-job', 'component': 'batch-processing'},
        get_logs=True,
        is_delete_operator_pod=True,  # Xóa pod sau khi hoàn thành
        in_cluster=True,  # Chạy trong cluster (Airflow cũng trên K8s)
        startup_timeout_seconds=300,
        container_resources=k8s.V1ResourceRequirements(
            requests={
                'memory': '2Gi',
                'cpu': '1000m'
            },
            limits={
                'memory': '3Gi',
                'cpu': '2000m'
            }
        ),
        retries=2,
    )
    
    # Task 3: Update ngày đã xử lý
    update_date_task = PythonOperator(
        task_id='update_process_date',
        python_callable=update_process_date,
        provide_context=True,
    )
    
    # Định nghĩa flow: get_date → spark_job → update_date
    get_date_task >> spark_job_task >> update_date_task
