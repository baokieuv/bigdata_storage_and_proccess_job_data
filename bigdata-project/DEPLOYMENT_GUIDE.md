# Hướng Dẫn Triển Khai Pipeline Job Processing

## Tổng Quan

Pipeline này xử lý dữ liệu job postings theo batch từ MinIO sang Cassandra bằng Spark, được điều phối bởi Airflow trên Kubernetes.

## Kiến Trúc

```
MinIO (bucket: sensor-data)
    ↓ (đọc jobs_YYYY-MM-DD.json)
Spark Job (xử lý aggregations)
    ↓ (ghi kết quả)
Cassandra (keyspace: metrics)
    ↓ (audit logs)
Airflow (schedule mỗi 5 phút)
```

## Bước 1: Tạo Dữ Liệu Mẫu

```bash
cd bigdata-project/scripts
python generate_sample_data.py
```

Kết quả: 20 file JSON trong thư mục `sample_data/` (jobs_2026-01-01.json đến jobs_2026-01-20.json)

## Bước 2: Deploy Infrastructure trên Kubernetes

### 2.1 Deploy MinIO

```bash
kubectl apply -f bigdata-project/k8s/minio-config.yaml

# Đợi MinIO ready
kubectl wait --for=condition=ready pod -l app=minio --timeout=300s

# Port-forward để truy cập MinIO console
kubectl port-forward svc/minio 9001:9001
```

Truy cập http://localhost:9001

- Username: `minioadmin`
- Password: `minioadmin`

**Tạo bucket và upload dữ liệu:**

1. Vào MinIO console → Create Bucket → Tên: `sensor-data`
2. Upload 20 file JSON từ thư mục `sample_data/`

### 2.2 Deploy Cassandra

```bash
kubectl apply -f bigdata-project/k8s/cassandra-config.yaml

# Đợi Cassandra ready (có thể mất 2-3 phút)
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=600s

# Kiểm tra cluster status
kubectl exec -it cassandra-0 -- cqlsh -e "DESCRIBE CLUSTER"
```

**Khởi tạo schema:**

```bash
# Copy file CQL vào pod
kubectl cp bigdata-project/k8s/init-cassandra.cql cassandra-0:/tmp/

# Execute CQL script
kubectl exec -it cassandra-0 -- cqlsh -f /tmp/init-cassandra.cql

# Verify tables đã được tạo
kubectl exec -it cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE metrics;"
```

### 2.3 Deploy Spark

```bash
kubectl apply -f bigdata-project/k8s/spark-config.yaml

# Đợi Spark Master và Workers ready
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=300s
kubectl wait --for=condition=ready pod -l app=spark-worker --timeout=300s

# Port-forward để xem Spark UI
kubectl port-forward svc/spark-master 8080:8080
```

Truy cập http://localhost:8080 để xem Spark Dashboard

## Bước 3: Build và Push Spark Job Image

```bash
cd bigdata-project

# Build Docker image
docker build -f Dockerfile.spark -t your-docker-registry/spark-job:latest .

# Push to registry (thay bằng registry của bạn: Docker Hub, GCR, ECR, ACR...)
docker push your-docker-registry/spark-job:latest
```

**Lưu ý:** Thay `your-docker-registry` trong các file sau:

- `k8s/spark-job-template.yaml`
- `dags/job_processing_dag.py`

## Bước 4: Test Spark Job Thủ Công

```bash
# Sửa spark-job-template.yaml, thay DATEPLACEHOLDER thành 2026-01-01
sed 's/DATEPLACEHOLDER/2026-01-01/g' k8s/spark-job-template.yaml > k8s/spark-job-test.yaml

# Apply job
kubectl apply -f k8s/spark-job-test.yaml

# Theo dõi logs
kubectl logs -f job/spark-job-2026-01-01

# Kiểm tra kết quả trong Cassandra
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT * FROM metrics.avg_salary_by_experience WHERE process_date='2026-01-01';"
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT * FROM metrics.jobs_by_work_type WHERE process_date='2026-01-01';"
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT * FROM metrics.job_processing_audit ORDER BY started_at DESC LIMIT 5;"
```

## Bước 5: Deploy Airflow trên Kubernetes

### 5.1 Cài Airflow bằng Helm

```bash
# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Tạo namespace cho Airflow
kubectl create namespace airflow

# Tạo values.yaml cho Helm chart
cat > airflow-values.yaml <<EOF
executor: KubernetesExecutor

dags:
  gitSync:
    enabled: true
    repo: https://github.com/your-username/your-repo.git  # Thay bằng Git repo của bạn
    branch: main
    subPath: bigdata-project/dags
    wait: 60

logs:
  persistence:
    enabled: true
    size: 10Gi

webserver:
  service:
    type: LoadBalancer

# Variables
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: 'False'
  - name: AIRFLOW__KUBERNETES__IN_CLUSTER
    value: 'True'
  - name: AIRFLOW__KUBERNETES__NAMESPACE
    value: 'default'

# Init container để set variable ban đầu
extraInitContainers:
  - name: init-variables
    image: apache/airflow:2.7.0
    command:
      - bash
      - -c
      - |
        airflow variables set job_processing_current_date "2026-01-01"
EOF

# Install Airflow
helm install airflow apache-airflow/airflow -n airflow -f airflow-values.yaml

# Đợi Airflow ready
kubectl wait --for=condition=ready pod -l component=webserver -n airflow --timeout=600s
```

### 5.2 Truy Cập Airflow UI

```bash
# Get Airflow web service URL (nếu dùng LoadBalancer)
kubectl get svc -n airflow airflow-webserver

# Hoặc port-forward
kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow
```

Truy cập http://localhost:8081

- Username: `admin`
- Password: Lấy bằng lệnh: `kubectl get secret airflow-webserver-secret -n airflow -o jsonpath="{.data.webserver-secret-key}" | base64 -d`

### 5.3 Enable DAG

1. Vào Airflow UI
2. Tìm DAG `job_processing_pipeline`
3. Toggle ON để enable
4. DAG sẽ chạy mỗi 5 phút

## Bước 6: Monitoring

### Xem Logs Airflow

```bash
kubectl logs -f -n airflow -l component=scheduler
```

### Xem Spark Job Logs

```bash
# List các jobs đang chạy
kubectl get jobs -l app=spark-job

# Xem logs của job cụ thể
kubectl logs -f job/spark-job-2026-01-XX
```

### Query Cassandra Audit Logs

```bash
kubectl exec -it cassandra-0 -- cqlsh -e "
  SELECT process_date, status, records_read, records_written_exp, records_written_work, started_at
  FROM metrics.job_processing_audit
  ORDER BY started_at DESC
  LIMIT 20;
"
```

### Xem Kết Quả Xử Lý

```bash
# Lương trung bình theo Experience Level
kubectl exec -it cassandra-0 -- cqlsh -e "
  SELECT experience_level, avg_salary, job_count, process_date
  FROM metrics.avg_salary_by_experience
  WHERE process_date='2026-01-15';
"

# Số job theo Work Type
kubectl exec -it cassandra-0 -- cqlsh -e "
  SELECT work_type, job_count, process_date
  FROM metrics.jobs_by_work_type
  WHERE process_date='2026-01-15';
"
```

## Cấu Trúc Files Đã Tạo

```
bigdata-project/
├── Dockerfile.spark                    # Image chứa Spark job + JARs
├── src/
│   └── spark_job.py                   # Spark job xử lý dữ liệu
├── scripts/
│   └── generate_sample_data.py        # Script sinh dữ liệu mẫu
├── k8s/
│   ├── minio-config.yaml              # Deploy MinIO
│   ├── cassandra-config.yaml          # Deploy Cassandra
│   ├── spark-config.yaml              # Deploy Spark cluster
│   ├── init-cassandra.cql             # Schema Cassandra
│   └── spark-job-template.yaml        # Template cho Spark job
└── dags/
    └── job_processing_dag.py          # Airflow DAG
```

## Troubleshooting

### Spark Job Không Đọc Được MinIO

- Kiểm tra MinIO đang chạy: `kubectl get pods -l app=minio`
- Kiểm tra bucket `sensor-data` đã tạo chưa
- Verify file tồn tại: Port-forward MinIO console và kiểm tra

### Spark Job Không Ghi Được Cassandra

- Kiểm tra Cassandra ready: `kubectl exec -it cassandra-0 -- nodetool status`
- Verify schema: `kubectl exec -it cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE metrics;"`
- Kiểm tra network connectivity: `kubectl exec -it <spark-pod> -- nc -zv cassandra.default.svc.cluster.local 9042`

### Airflow DAG Không Chạy

- Kiểm tra DAG có lỗi syntax không: Vào Airflow UI → DAGs
- Xem logs scheduler: `kubectl logs -f -n airflow -l component=scheduler`
- Verify Variable đã set: Airflow UI → Admin → Variables

### Job Processing Audit Log Trống

- Kiểm tra Spark job có chạy thành công không
- Xem logs Spark job để tìm lỗi
- Verify connection Cassandra trong Spark job

## Next Steps

1. **Setup Git Repository cho DAGs:** Push `dags/` folder lên Git để Airflow Git-Sync tự động sync
2. **Configure Remote Logging:** Setup S3/GCS/Azure Blob cho Airflow logs
3. **Setup Alerting:** Configure Airflow email alerts khi job fail
4. **Optimize Spark:** Tune Spark configs (memory, cores) dựa trên workload thực tế
5. **Add Monitoring:** Deploy Prometheus + Grafana để monitor metrics

## Tham Khảo

- Spark Documentation: https://spark.apache.org/docs/3.5.1/
- Cassandra CQL: https://cassandra.apache.org/doc/latest/cql/
- Airflow KubernetesPodOperator: https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/
- MinIO S3 API: https://min.io/docs/minio/linux/developers/python/API.html
