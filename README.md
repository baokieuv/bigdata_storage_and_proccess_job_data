# 🚀 Deploy K8s Full Big Data Project

## 📌 Tổng quan

Project này triển khai hệ thống xử lý dữ liệu Big Data end-to-end trên Kubernetes, bao gồm:

- **Kafka (Strimzi)**: Ingest dữ liệu job
- **MinIO**: Lưu trữ dữ liệu thô (raw data)
- **Spark**:
  - Streaming: Kafka → Elasticsearch
  - Batch: MinIO → Cassandra + Elasticsearch
- **Cassandra**: Lưu trữ dữ liệu phân tích
- **Elasticsearch + Kibana**: Realtime & analytics visualization
- **Python services**: Producer, ingestor
- **Docker + Kubernetes**: GKE / kind

### Luồng xử lý dữ liệu

```
Job API → Producer → Kafka
Kafka → Spark Streaming → Elasticsearch (Realtime)
Kafka → MinIO → Spark Batch → Cassandra + Elasticsearch
```

## 📁 Cấu trúc thư mục

```
.
├── k8s/
│   ├── kafka-config.yaml
│   ├── minio-config.yaml
│   ├── cassandra-config.yaml
│   ├── spark-config.yaml
│   ├── elastic-config.yaml
│   ├── kibana-config.yaml
│   ├── init-job.yaml
│   ├── app-deployment.yaml
│   └── kind-config.yaml
├── src/
│   ├── producer.py
│   ├── kafka_to_minio.py
│   ├── spark_job.py
│   └── spark_streaming.py
├── Dockerfile
└── README.md
```

## I. Chuẩn bị môi trường

### 1️⃣ Chạy trên Google Cloud (GKE)

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable container.googleapis.com artifactregistry.googleapis.com
```

**Tạo cluster 3 nodes:**

> **Khuyến nghị**: machine type `e2-standard-2` (2 vCPU, 8GB RAM) do dùng Kafka + Cassandra + Spark

**Kiểm tra kết nối:**

```bash
kubectl get nodes
```

### 2️⃣ Chạy local bằng kind

Tạo file `k8s/kind-config.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
```

Khởi tạo cluster:

```bash
kind create cluster --name bigdata-cluster --config k8s/kind-config.yaml
kubectl get nodes
```

## II. Deploy hạ tầng Big Data

### 1️⃣ Cài Strimzi (Kafka Operator)

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=default'
```

> ⚠️ **Chờ Strimzi running xong trước khi deploy Kafka**

### 2️⃣ Deploy Kafka

```bash
kubectl apply -f k8s/kafka-config.yaml
```

- Kafka chạy chế độ KRaft
- 3 replicas (có thể giảm khi test)

### 3️⃣ Deploy MinIO (Distributed)

```bash
kubectl apply -f k8s/minio-config.yaml
```

- MinIO chạy StatefulSet
- Tối thiểu 4 replicas (Erasure Coding)
- Bucket dùng trong project: `job-raw-data`

### 4️⃣ Deploy Cassandra

```bash
kubectl apply -f k8s/cassandra-config.yaml
```

- 3 replicas
- Có readiness probe
- Dùng CQL port 9042

### 5️⃣ Deploy Spark Cluster

```bash
kubectl apply -f k8s/spark-config.yaml
```

- 1 Spark Master
- 3 Spark Workers (có thể giảm)

### 6️⃣ Deploy Elasticsearch & Kibana

```bash
kubectl apply -f k8s/elastic-config.yaml
kubectl apply -f k8s/kibana-config.yaml
```

### 7️⃣ Init resource (bucket + Cassandra table)

> ⚠️ **Chỉ chạy sau khi MinIO & Cassandra đã RUNNING**

```bash
kubectl apply -f k8s/init-job.yaml
```

Sau khi hoàn tất:

```bash
kubectl delete -f k8s/init-job.yaml
```

## III. Build & Push Docker Image

### 1️⃣ Build image

```bash
docker build -t <dockerhub-username>/<repo>:v1 .
```

### 2️⃣ Push image

```bash
docker push <dockerhub-username>/<repo>:v1
```

## IV. Deploy các ứng dụng xử lý dữ liệu

**File**: `k8s/app-deployment.yaml`

Bao gồm:

- **Producer**: Job API → Kafka
- **Ingestor**: Kafka → MinIO
- **Spark Streaming**: Kafka → Elasticsearch
- **Spark Batch (CronJob)**: MinIO → Cassandra + Elasticsearch
- **Job API**

> ⚠️ **Sửa image name trong file:**

```yaml
image: baokieu/my-repo:v1   # THAY BẰNG IMAGE CỦA BẠN
```

**Deploy:**

```bash
kubectl apply -f k8s/app-deployment.yaml
```

## V. Mô tả các chương trình

### 🔹 producer.py

- Gọi Job API
- Gửi dữ liệu vào Kafka topic `jobs-topic`
- Retry nếu Kafka chưa sẵn sàng

### 🔹 kafka_to_minio.py

- Consume Kafka
- Gom batch:
  - 10 records hoặc
  - 60s
- Ghi file JSON vào MinIO

### 🔹 spark_streaming.py

- Spark Structured Streaming
- Kafka → Elasticsearch
- Index: `jobs-realtime`

### 🔹 spark_job.py

- Spark Batch
- Đọc dữ liệu từ MinIO theo batch
- Tính:
  - job_count
  - avg / max / min salary
- Ghi vào:
  - Cassandra (`job_metrics.company_analytics`)
  - Elasticsearch (`jobs-analytics`)
- Có state file để tránh xử lý trùng

## VI. Kiểm tra hệ thống

### Xem log

```bash
kubectl logs <pod-name>
```

Ví dụ:

```bash
kubectl logs spark-master-xxx
kubectl logs spark-processor-xxx
```

### Kiểm tra Cassandra

```bash
kubectl exec -it cassandra-0 -- cqlsh
```

```sql
SELECT * FROM job_metrics.company_analytics;
```

### Truy cập Kibana

```bash
kubectl port-forward svc/kibana 5601:5601
```

Mở trình duyệt:

```
http://localhost:5601
```

## 📌 Ghi chú quan trọng

- Có thể giảm replicas khi chạy local
- MinIO distributed bắt buộc ≥ 4 pod
- Spark Streaming dùng deploy-mode `client` để dễ debug
- CronJob batch chạy mỗi 2 phút

## ✅ Công nghệ sử dụng

- Kubernetes
- Kafka (Strimzi)
- Apache Spark
- MinIO
- Cassandra
- Elasticsearch + Kibana
- Docker
- Python

---

**Happy Big Data Processing! 🎉**
