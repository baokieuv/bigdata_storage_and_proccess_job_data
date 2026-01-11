# BigData Pipeline - Quick Start Guide

## 🎯 Mô Tả

Pipeline xử lý dữ liệu job postings theo batch:

- **MinIO**: Lưu trữ file JSON theo ngày
- **Spark**: Xử lý aggregations (lương TB, số job theo type)
- **Cassandra**: Lưu kết quả đã xử lý
- **Airflow**: Tự động hóa (schedule mỗi 5 phút)

## ⚡ Quick Start - CHỈ 3 BƯỚC!

### Bước 1: Cài Docker Desktop

Download và cài: https://www.docker.com/products/docker-desktop/

- Restart máy sau khi cài
- Đợi Docker Desktop khởi động (icon xanh)

### Bước 2: Chạy Script Setup

Mở **PowerShell as Administrator** (chuột phải → Run as Administrator):

```powershell
# Navigate đến thư mục setup
cd C:\BK_WORKSPACE\bigdata\bigdata_storage_and_proccess_job_data\bigdata-project\scripts\setup

# Chạy script tự động
.\run-all.ps1
```

Làm theo hướng dẫn trên màn hình. Script sẽ tự động:

1. Cài kubectl, Helm, Minikube, Python (nếu chưa có)
2. Start Kubernetes cluster
3. Tạo 20 file JSON dữ liệu mẫu
4. Deploy tất cả services
5. Test Spark job
6. (Optional) Deploy Airflow

**Tổng thời gian:** ~15-20 phút

### Bước 3: Xem Kết Quả

Các service sẽ tự động port-forward:

- **MinIO Console**: http://localhost:9001 (`minioadmin` / `minioadmin`)
- **Spark UI**: http://localhost:8080
- **Airflow UI**: http://localhost:8081 (`admin` / `admin`)

Query dữ liệu trong Cassandra:

```powershell
kubectl exec -it cassandra-0 -- cqlsh

# Trong cqlsh:
USE metrics;
SELECT * FROM avg_salary_by_experience LIMIT 10;
SELECT * FROM jobs_by_work_type LIMIT 10;
```

## 📁 Cấu Trúc Project

```
bigdata-project/
├── scripts/
│   ├── setup/
│   │   ├── run-all.ps1                 # ⭐ CHẠY FILE NÀY
│   │   ├── 1-install-prerequisites.ps1 # Cài tools
│   │   ├── 2-start-minikube.ps1        # Start K8s
│   │   ├── 3-generate-data.ps1         # Tạo data
│   │   ├── 4-deploy-infrastructure.ps1 # Deploy services
│   │   ├── 5-setup-minio.ps1           # Setup MinIO
│   │   ├── 6-init-cassandra.ps1        # Init Cassandra
│   │   ├── 7-build-spark-image.ps1     # Build Spark image
│   │   ├── 8-test-spark-job.ps1        # Test job
│   │   └── 9-deploy-airflow.ps1        # Deploy Airflow
│   └── generate_sample_data.py         # Script sinh data
├── src/
│   └── spark_job.py                    # Spark job code
├── k8s/
│   ├── minio-config.yaml               # MinIO deployment
│   ├── cassandra-config.yaml           # Cassandra deployment
│   ├── spark-config.yaml               # Spark deployment
│   ├── init-cassandra.cql              # Cassandra schema
│   └── spark-job-template.yaml         # Spark job template
├── dags/
│   └── job_processing_dag.py           # Airflow DAG
├── Dockerfile.spark                    # Spark Docker image
├── SETUP_FROM_SCRATCH.md               # Hướng dẫn chi tiết
└── DEPLOYMENT_GUIDE.md                 # Deployment guide

```

## 🔧 Chạy Từng Bước (Nếu Muốn Kiểm Soát)

Thay vì `run-all.ps1`, chạy từng script theo thứ tự:

```powershell
cd bigdata-project\scripts\setup

# 1. Cài prerequisites
.\1-install-prerequisites.ps1

# 2. Start Minikube
.\2-start-minikube.ps1

# 3. Sinh dữ liệu
.\3-generate-data.ps1

# 4. Deploy infrastructure
.\4-deploy-infrastructure.ps1

# 5. Setup MinIO
.\5-setup-minio.ps1

# 6. Init Cassandra
.\6-init-cassandra.ps1

# 7. Build Spark image
.\7-build-spark-image.ps1

# 8. Test Spark job
.\8-test-spark-job.ps1

# 9. Deploy Airflow (optional)
.\9-deploy-airflow.ps1
```

## 🐛 Troubleshooting

### Script báo lỗi "execution policy"

```powershell
Set-ExecutionPolicy -Scope Process -Force Bypass
```

### Docker không start được

- Mở Docker Desktop
- Đợi icon màu xanh ở system tray
- Test: `docker ps`

### Minikube lỗi

```powershell
minikube delete
minikube start --driver=docker --cpus=4 --memory=8192
```

### Pod bị pending/crash

```powershell
# Xem lỗi
kubectl describe pod <pod-name>
kubectl logs <pod-name>

# Restart
kubectl delete pod <pod-name>
```

### Port-forward bị disconnect

```powershell
# Restart port-forward
kubectl port-forward svc/minio 9001:9001
kubectl port-forward svc/spark-master 8080:8080
kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow
```

## 📊 Kiểm Tra Kết Quả

### Check pods đang chạy

```powershell
kubectl get pods -A
```

### Xem Spark job logs

```powershell
kubectl get jobs
kubectl logs job/spark-job-2026-01-01
```

### Query Cassandra

```powershell
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT * FROM metrics.avg_salary_by_experience LIMIT 5;"
```

### Xem MinIO files

Vào http://localhost:9001 → Buckets → sensor-data

## 🎓 Học Thêm

- **SETUP_FROM_SCRATCH.md**: Hướng dẫn chi tiết từng bước
- **DEPLOYMENT_GUIDE.md**: Deployment guide cho production
- Chi tiết code: Xem comments trong các file Python/YAML

## 🆘 Cần Giúp Đỡ?

1. Check logs: `kubectl logs <pod-name>`
2. Describe pod: `kubectl describe pod <pod-name>`
3. Xem file SETUP_FROM_SCRATCH.md phần Troubleshooting

---

**Chúc bạn setup thành công! 🚀**
