cd D:\bigdata\bigdata_storage_and_proccess_job_data\bigdata-project\k8s
# ktr cluster 
kubectl cluster-info

# xac dinh contex
kubectl config get-contexts

<!-- kubectl create namespace bigdata -->

kubectl apply -f cassandra.yaml -n 
kubectl apply -f minio.yaml -n 
kubectl apply -f spark.yaml -n 

kubectl port-forward svc/minio 9001:9001
kubectl port-forward svc/cassandra 9042:9042

#debug
netstat -ano | findstr :8080

# Tăng số worker
kubectl scale deployment spark-worker --replicas=3

# Giảm số worker
kubectl scale deployment spark-worker --replicas=1

# Cài MinIO client (mc) nếu chưa có
# Tải từ: https://min.io/docs/minio/linux/reference/minio-mc.html

# Cấu hình alias
mc alias set myminio http://localhost:9001  minioadmin minioadmin 
#power shell
.\mc alias set myminio http://localhost:9000 minioadmin minioadmin 

# Tạo bucket
mc mb myminio/job-data

# Upload file JSON vào bucket
mc cp path/to/your/dataset.json myminio/job-data/

# Hoặc upload tất cả file JSON trong thư mục
mc cp path/to/dataset/*.json myminio/job-data/

# Kiểm tra
mc ls myminio/job-data

# tao tất cả file 1 lần 
.\mc mb myminio/raw-json myminio/processed-parquet myminio/spark-checkpoints myminio/cassandra-backup myminio/models

# Gỡ cài đặt version hiện tại
py -3.11 -m pip uninstall pyspark -y

# Cài version mới nhất
py -3.11 -m pip install pyspark==3.5.1


# 1. Tìm tên Spark Master pod
kubectl get pods -l app=spark-master

# 2. Copy file spark_job.py vào pod
kubectl cp spark_job.py spark-master-68f5fdd9b4-d9rjq:/tmp/spark_job.py

# 1. Set alias
.\mc.exe alias set myminio http://localhost:9000 minioadmin minioadmin

# 2. Tạo bucket
$bucket = "job-data-processed"
.\mc.exe mb myminio/$bucket 2>$null

# 3. Upload files
$files = Get-ChildItem "D:\bigdata\data\postings*.json"
Write-Host "Uploading $($files.Count) files to bucket '$bucket'..."

foreach ($file in $files) {
    $sizeMB = [math]::Round($file.Length / 1MB, 2)
    Write-Host "  $($file.Name) ($sizeMB MB)..." -NoNewline
    
    .\mc.exe cp $file.FullName myminio/$bucket/ 2>$null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host " ✅" -ForegroundColor Green
    } else {
        Write-Host " ❌" -ForegroundColor Red
        # Show error
        .\mc.exe cp $file.FullName myminio/$bucket/
    }
}

# đẩy tay 10 file dữ liệu lên minio bằng mc (Chưa có thì phải tải về)

PS D:\> .\mc.exe alias set myminio http://localhost:9000 minioadmin minioadmin
Added `myminio` successfully.
PS D:\> .\mc.exe rm --recursive --force myminio/postings-data
Removed `myminio/postings-data/raw/postings1.json`.
Removed `myminio/postings-data/raw/postings10.json`.
Removed `myminio/postings-data/raw/postings2.json`.
Removed `myminio/postings-data/raw/postings3.json`.
Removed `myminio/postings-data/raw/postings4.json`.
Removed `myminio/postings-data/raw/postings5.json`.
Removed `myminio/postings-data/raw/postings6.json`.
Removed `myminio/postings-data/raw/postings7.json`.
Removed `myminio/postings-data/raw/postings8.json`.
Removed `myminio/postings-data/raw/postings9.json`.