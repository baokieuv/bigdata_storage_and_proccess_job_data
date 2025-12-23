import time
import random
from pyspark.sql import SparkSession

def heavy_computation(x):
    # Giả lập một tác vụ nặng tốn khoảng 1 giây xử lý mỗi item
    # In ra dòng này để biết task đang chạy ở đâu (xem log worker sẽ thấy)
    time.sleep(1) 
    return x * x

if __name__ == "__main__":
    # 1. Khởi tạo Spark Session
    print(">>> Đang khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("Demo-Phan-Phoi-Job") \
        .getOrCreate()

    # 2. Tạo dữ liệu giả: 20 con số
    data = list(range(1, 31))
    
    # 3. Parallelize: Đưa dữ liệu vào RDD
    # Quan trọng: numSlices=10 nghĩa là chia dữ liệu thành 10 phần (partitions).
    # Vì ta có 2 Worker (mỗi worker 1 core), Spark sẽ chia 10 phần này cho 2 worker xử lý dần.
    rdd = spark.sparkContext.parallelize(data, numSlices=10)

    print(">>> Bắt đầu xử lý phân tán...")
    print(f"Số lượng Partition: {rdd.getNumPartitions()}")

    # 4. Map: Gửi hàm heavy_computation xuống các Worker
    # Worker sẽ nhận code và chạy song song
    result_rdd = rdd.map(heavy_computation)

    # 5. Collect: Gom kết quả từ các Worker về lại Master/Driver
    # Action này mới thực sự kích hoạt việc chạy (Lazy evaluation)
    total = result_rdd.reduce(lambda a, b: a + b)

    print(f">>> KẾT QUẢ CUỐI CÙNG: Tổng bình phương là {total}")

    # Giữ app chạy thêm 30s để bạn kịp xem giao diện Web UI nếu muốn
    print(">>> Job xong. Đang chờ 30s trước khi tắt hẳn...")
    time.sleep(30)
    
    spark.stop()