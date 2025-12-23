from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ----- Các hàm thực thi -----
def task1_hello():
    print("Hello World!")

def task2_print_1_to_10():
    for i in range(1, 11):
        print(i)

def task3_sum_1_to_100():
    total = sum(range(1, 101))
    print(f"Sum from 1 to 100 is {total}")

def task4_goodbye():
    print("Goodbye!")

# ----- DAG -----
with DAG(
    dag_id='demo_dag',
    start_date=datetime(2025, 11, 9),  # Ngày bắt đầu
    schedule='*/2 * * * *',             # Chạy mỗi 2 phút
    catchup=False                        # Không chạy các DAG quá khứ
) as dag:

    t1 = PythonOperator(
        task_id='task1_hello',
        python_callable=task1_hello
    )

    t2 = PythonOperator(
        task_id='task2_print_1_to_10',
        python_callable=task2_print_1_to_10
    )

    t3 = PythonOperator(
        task_id='task3_sum_1_to_100',
        python_callable=task3_sum_1_to_100
    )

    t4 = PythonOperator(
        task_id='task4_goodbye',
        python_callable=task4_goodbye
    )

    # ----- Thiết lập luồng -----
    t1 >> [t2, t3]
    [t2, t3] >> t4
