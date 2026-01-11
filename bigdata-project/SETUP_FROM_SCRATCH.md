# Setup Môi Trường Từ Đầu - Windows

## 🚀 Quick Start - Chạy 1 Lệnh (Khuyến Nghị)

**Nếu bạn muốn setup tự động, chỉ cần:**

1. Mở **PowerShell as Administrator**
2. Navigate đến thư mục project:
   ```powershell
   cd C:\BK_WORKSPACE\bigdata\bigdata_storage_and_proccess_job_data\bigdata-project\scripts\setup
   ```
3. Chạy script master:
   ```powershell
   .\run-all.ps1
   ```

Script sẽ tự động:

- ✅ Cài đặt tất cả prerequisites (Chocolatey, kubectl, Helm, Python...)
- ✅ Start Minikube cluster
- ✅ Sinh dữ liệu mẫu 20 file JSON
- ✅ Deploy MinIO, Cassandra, Spark
- ✅ Upload data vào MinIO
- ✅ Init Cassandra schema
- ✅ Build Spark image
- ✅ Test Spark job
- ✅ Deploy Airflow (optional)

**Tổng thời gian:** ~15-20 phút (tùy tốc độ mạng và máy tính)

---

## 📋 Manual Setup (Nếu muốn kiểm soát từng bước)

### Yêu Cầu Hệ Thống

- Windows 10/11
- RAM tối thiểu: 16GB (khuyến nghị 32GB)
- Disk: 50GB trống
- CPU: 4 cores trở lên

## Bước 1: Cài Đặt Các Tool Cần Thiết

### Tự Động (Khuyến Nghị)

```powershell
cd bigdata-project\scripts\setup
.\1-install-prerequisites.ps1
```

### Thủ Công

### 1.1 Cài Docker Desktop

1. Download Docker Desktop: https://www.docker.com/products/docker-desktop/
2. Chạy installer và làm theo hướng dẫn
3. Khởi động lại máy tính
4. Mở Docker Desktop → Settings → Resources:
   - CPUs: 4
   - Memory: 8GB
   - Swap: 2GB
   - Apply & Restart

**Kiểm tra:**

```bash
docker --version
docker run hello-world
```

### 1.2 Cài Kubernetes (Minikube)

**Cài Minikube:**

```bash
# Download Minikube installer
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-installer.exe

# Chạy installer
minikube-installer.exe
```

**Hoặc dùng Chocolatey:**

```bash
# Cài Chocolatey nếu chưa có (chạy PowerShell as Admin)
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Cài Minikube
choco install minikube
```

**Khởi động Minikube:**

```bash
# Start với driver Docker
minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=50g

# Kiểm tra
minikube status
```

### 1.3 Cài kubectl

```bash
# Dùng Chocolatey
choco install kubernetes-cli

# Hoặc download trực tiếp
curl.exe -LO "https://dl.k8s.io/release/v1.28.0/bin/windows/amd64/kubectl.exe"
# Copy kubectl.exe vào C:\Windows\System32\
```

**Kiểm tra:**

```bash
kubectl version --client
kubectl get nodes
```

### 1.4 Cài Helm

```bash
# Dùng Chocolatey
choco install kubernetes-helm

# Hoặc download từ: https://github.com/helm/helm/releases
```

**Kiểm tra:**

```bash
helm version
```

### 1.5 Cài Python (cho script sinh data)

1. Download Python 3.11: https://www.python.org/downloads/
2. Chạy installer, **QUAN TRỌNG:** Tick ☑️ "Add Python to PATH"
3. Verify:

```bash
python --version
pip --version
```

### 1.6 Cài Git (nếu chưa có)

```bash
choco install git

# Hoặc download: https://git-scm.com/download/win
```

## Bước 2: Clone Project

```bash
cd C:\BK_WORKSPACE\bigdata
git clone <your-repo-url>
cd bigdata_storage_and_proccess_job_data\bigdata-project
```

## Bước 3: Tạo Dữ Liệu Mẫu

```bash
cd scripts
python generate_sample_data.py

# Kết quả: Tạo folder sample_data/ với 20 file JSON
dir sample_data
```

## Bước 4: Deploy Infrastructure trên Kubernetes

### 4.1 Deploy MinIO

```bash
cd ..\k8s
kubectl apply -f minio-config.yaml

# Đợi pods ready
kubectl wait --for=condition=ready pod -l app=minio --timeout=300s

# Kiểm tra status
kubectl get pods -l app=minio

# Port-forward để truy cập (mở terminal mới)
kubectl port-forward svc/minio 9000:9000 9001:9001
```

**Mở tab browser mới:** http://localhost:9001

- Login: `minioadmin` / `minioadmin`
- Click "Buckets" → "Create Bucket" → Tên: `sensor-data` → Create
- Click vào bucket `sensor-data` → "Upload" → Browse → Chọn 20 file JSON từ `scripts/sample_data/` → Upload

### 4.2 Deploy Cassandra

```bash
kubectl apply -f cassandra-config.yaml

# Đợi lâu hơn (2-3 phút)
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=600s

# Kiểm tra cluster
kubectl exec -it cassandra-0 -- nodetool status
```

**Khởi tạo schema:**

```bash
# Copy file CQL vào pod
kubectl cp init-cassandra.cql cassandra-0:/tmp/init-cassandra.cql

# Execute
kubectl exec -it cassandra-0 -- cqlsh -f /tmp/init-cassandra.cql

# Verify
kubectl exec -it cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE metrics;"
```

**Bạn sẽ thấy output:**

```
CREATE KEYSPACE metrics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};
CREATE TABLE metrics.avg_salary_by_experience ...
CREATE TABLE metrics.jobs_by_work_type ...
CREATE TABLE metrics.job_processing_audit ...
```

### 4.3 Deploy Spark

```bash
kubectl apply -f spark-config.yaml

# Đợi Spark ready
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=300s
kubectl wait --for=condition=ready pod -l app=spark-worker --timeout=300s

# Kiểm tra status
kubectl get pods -l app=spark-master
kubectl get pods -l app=spark-worker

# Xem Spark UI (mở terminal mới)
kubectl port-forward svc/spark-master 8080:8080
```

Mở browser: http://localhost:8080 (sẽ thấy Spark Dashboard với 2 workers)

## Bước 5: Build Spark Job Image

### 5.1 Cấu hình Docker Registry

**Option A: Dùng Docker Hub (khuyến nghị cho beginner)**

1. Tạo tài khoản: https://hub.docker.com/
2. Login:

```bash
docker login
# Nhập username và password
```

3. **QUAN TRỌNG:** Thay `your-docker-registry` trong các file sau:

   **File 1: `k8s/spark-job-template.yaml`** (dòng 18)

   ```yaml
   image: <your-docker-username>/spark-job:latest
   ```

   **File 2: `dags/job_processing_dag.py`** (dòng 68)

   ```python
   image='<your-docker-username>/spark-job:latest',
   ```

**Option B: Dùng Minikube local registry (nhanh hơn cho test)**

```bash
# Enable registry addon
minikube addons enable registry

# Thiết lập environment để build trực tiếp trong Minikube
# PowerShell:
& minikube -p minikube docker-env --shell powershell | Invoke-Expression

# Hoặc CMD:
@FOR /f "tokens=*" %i IN ('minikube -p minikube docker-env --shell cmd') DO @%i

# Dùng image local
# Thay image thành: localhost:5000/spark-job:latest
```

### 5.2 Build và Push Image

```bash
cd ..  # về thư mục bigdata-project

# Build (thay <your-docker-username> bằng username thật của bạn)
docker build -f Dockerfile.spark -t <your-docker-username>/spark-job:latest .

# Push (nếu dùng Docker Hub)
docker push <your-docker-username>/spark-job:latest
```

**Nếu gặp lỗi "cannot connect to Docker daemon":**

- Mở Docker Desktop
- Đợi Docker Desktop running (icon màu xanh ở system tray)
- Chạy lại lệnh build

**Build thành công khi thấy:**

```
Successfully built xxxxx
Successfully tagged <your-username>/spark-job:latest
```

## Bước 6: Test Spark Job Thủ Công

### 6.1 Tạo và Chạy Test Job

```bash
cd k8s

# PowerShell: Tạo file test với date cụ thể
(Get-Content spark-job-template.yaml) -replace 'DATEPLACEHOLDER', '2026-01-01' -replace 'your-docker-registry', '<your-docker-username>' | Set-Content spark-job-test.yaml

# CMD: Hoặc dùng text editor sửa thủ công
# Mở spark-job-template.yaml, thay DATEPLACEHOLDER → 2026-01-01
# Thay your-docker-registry → <your-docker-username>
# Save as spark-job-test.yaml

# Apply job
kubectl apply -f spark-job-test.yaml

# Kiểm tra job status
kubectl get jobs
```

### 6.2 Xem Logs và Debug

```bash
# Xem logs real-time
kubectl logs -f job/spark-job-2026-01-01

# Nếu job chưa start, xem pod events
kubectl describe job spark-job-2026-01-01

# Nếu pod pending, kiểm tra
kubectl get pods -l job-name=spark-job-2026-01-01
kubectl describe pod <pod-name>
```

**Logs thành công sẽ có:**

```
Reading data from: s3a://sensor-data/jobs_2026-01-01.json
Read XX records from ...
Written X records to avg_salary_by_experience
Written Y records to jobs_by_work_type
✓ Job completed successfully for 2026-01-01
```

### 6.3 Kiểm Tra Kết Quả trong Cassandra

```bash
# Query kết quả lương TB theo experience
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT * FROM metrics.avg_salary_by_experience WHERE process_date='2026-01-01' ALLOW FILTERING;"

# Query số job theo work type
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT * FROM metrics.jobs_by_work_type WHERE process_date='2026-01-01' ALLOW FILTERING;"

# Xem audit log
kubectl exec -it cassandra-0 -- cqlsh -e "SELECT job_name, status, records_read, records_written_exp, records_written_work, started_at FROM metrics.job_processing_audit ORDER BY started_at DESC LIMIT 5 ALLOW FILTERING;"
```

**Nếu thấy data → Thành công!** ✅

## Bước 7: Deploy Airflow (Tự Động Hóa)

### 7.1 Cài Airflow bằng Helm

```bash
# Add repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Tạo namespace
kubectl create namespace airflow
```

### 7.2 Tạo File Cấu Hình Airflow

Tạo file `airflow-values.yaml` trong thư mục `bigdata-project/`:

```yaml
executor: KubernetesExecutor

# Tắt git-sync để mount DAGs từ local
dags:
  gitSync:
    enabled: false

logs:
  persistence:
    enabled: true
    size: 5Gi

webserver:
  service:
    type: NodePort

env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "False"
  - name: AIRFLOW__KUBERNETES__IN_CLUSTER
    value: "True"
  - name: AIRFLOW__KUBERNETES__NAMESPACE
    value: "default"
  - name: AIRFLOW__CORE__DAGS_FOLDER
    value: "/opt/airflow/dags"

# Resources
resources:
  limits:
    cpu: 1000m
    memory: 2Gi
  requests:
    cpu: 500m
    memory: 1Gi
```

### 7.3 Mount DAGs vào Airflow (Cách Đơn Giản)

**Cách 1: Copy DAG vào ConfigMap (khuyến nghị cho test)**

```bash
# Tạo ConfigMap từ DAG file
kubectl create configmap airflow-dags --from-file=dags/job_processing_dag.py -n airflow

# Update values.yaml thêm:
# extraVolumes:
#   - name: dags
#     configMap:
#       name: airflow-dags
# extraVolumeMounts:
#   - name: dags
#     mountPath: /opt/airflow/dags
```

**Cách 2: Rebuild Airflow image có sẵn DAG**

Tạo `Dockerfile.airflow`:

```dockerfile
FROM apache/airflow:2.7.0
COPY dags/ /opt/airflow/dags/
```

Build và push:

```bash
docker build -f Dockerfile.airflow -t <your-username>/airflow-custom:latest .
docker push <your-username>/airflow-custom:latest

# Update values.yaml:
# images:
#   airflow:
#     repository: <your-username>/airflow-custom
#     tag: latest
```

### 7.4 Install Airflow

```bash
helm install airflow apache-airflow/airflow -n airflow -f airflow-values.yaml --timeout 10m

# Đợi ready (có thể mất 5-10 phút)
kubectl get pods -n airflow -w
# Ctrl+C khi tất cả pods đã Running

# Kiểm tra status
kubectl get pods -n airflow
```

### 7.5 Truy cập Airflow UI

```bash
# Get NodePort
kubectl get svc -n airflow airflow-webserver

# Hoặc port-forward (dễ hơn)
kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow
```

Mở browser: http://localhost:8081

**Login:**

- Username: `admin`
- Password: Lấy bằng lệnh:

```bash
kubectl get secret airflow-webserver-secret -n airflow -o jsonpath="{.data.webserver-secret-key}" | base64 -d
```

### 7.6 Setup Airflow Variable

1. Vào Airflow UI → Admin → Variables
2. Click "+ Add a new record"
3. Nhập:
   - **Key:** `job_processing_current_date`
   - **Val:** `2026-01-01`
4. Save

### 7.7 Enable DAG

1. Vào Airflow UI → DAGs
2. Tìm `job_processing_pipeline`
3. Toggle switch từ OFF → ON
4. DAG sẽ tự chạy mỗi 5 phút

## Bước 8: Monitoring & Debugging

### 8.1 Check Tất Cả Pods

```bash
# Xem tất cả pods trong cluster
kubectl get pods -A

# Xem pods theo namespace
kubectl get pods -n default
kubectl get pods -n airflow

# Xem chi tiết pod
kubectl describe pod <pod-name>
```

### 8.2 Access Services

**MinIO Console:**

```bash
kubectl port-forward svc/minio 9001:9001
# http://localhost:9001
```

**Spark UI:**

```bash
kubectl port-forward svc/spark-master 8080:8080
# http://localhost:8080
```

**Airflow UI:**

```bash
kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow
# http://localhost:8081
```

### 8.3 Query Cassandra

```bash
# Exec vào pod để mở cqlsh
kubectl exec -it cassandra-0 -- cqlsh

# Trong cqlsh, chạy queries:
USE metrics;

-- Xem lương TB theo experience
SELECT experience_level, avg_salary, job_count, process_date
FROM avg_salary_by_experience
LIMIT 20;

-- Xem số job theo work type
SELECT work_type, job_count, process_date
FROM jobs_by_work_type
LIMIT 20;

-- Xem audit logs
SELECT process_date, status, records_read, started_at, completed_at
FROM job_processing_audit
ORDER BY started_at DESC
LIMIT 10
ALLOW FILTERING;

-- Exit
exit
```

### 8.4 Xem Logs Airflow DAG Run

```bash
# Xem logs scheduler
kubectl logs -f -n airflow -l component=scheduler

# Xem logs của một task cụ thể (trong Airflow UI)
# DAGs → job_processing_pipeline → Click vào run → Click vào task → View Log
```

## Troubleshooting Phổ Biến

### 1. Minikube không start được

```bash
# Xem lỗi chi tiết
minikube start --driver=docker --cpus=4 --memory=8192 -v=7

# Nếu vẫn lỗi, reset hoàn toàn
minikube delete --all --purge
minikube start --driver=docker --cpus=4 --memory=8192

# Check Docker Desktop đang chạy
docker ps
```

### 2. Pods bị Pending

```bash
# Xem tại sao pending
kubectl describe pod <pod-name>

# Thường do:
# - Insufficient memory/cpu → Giảm resources trong config
# - Image pull failed → Check image name, registry credentials
# - Volume mount failed → Check volume exists
```

### 3. Pods bị CrashLoopBackOff

```bash
# Xem logs lỗi
kubectl logs <pod-name>
kubectl logs <pod-name> --previous  # Logs của lần restart trước

# Xem events
kubectl get events --sort-by=.metadata.creationTimestamp | grep <pod-name>
```

### 4. Docker build lỗi "cannot connect"

```bash
# Check Docker Desktop running
docker ps

# Restart Docker Desktop
# Right-click icon → Restart

# Verify
docker run hello-world
```

### 5. Spark job lỗi "FileNotFoundException: s3a://sensor-data/..."

**Nguyên nhân:** MinIO chưa có data hoặc credentials sai

```bash
# Check MinIO pods running
kubectl get pods -l app=minio

# Check bucket exists
kubectl port-forward svc/minio 9001:9001
# Vào http://localhost:9001 kiểm tra bucket sensor-data và files

# Test connection từ Spark pod
kubectl run -it --rm debug --image=alpine --restart=Never -- sh
# Trong pod:
apk add curl
curl http://minio.default.svc.cluster.local:9000
# Nếu kết nối được sẽ thấy XML response
```

### 6. Spark job lỗi "Connection refused to Cassandra"

```bash
# Check Cassandra pods running
kubectl get pods -l app=cassandra

# Check Cassandra ready
kubectl exec -it cassandra-0 -- nodetool status
# Output: UN (Up Normal) cho cả 3 nodes

# Test connection
kubectl run -it --rm debug --image=alpine --restart=Never -- sh
# Trong pod:
apk add curl
nc -zv cassandra.default.svc.cluster.local 9042
# Connection successful nếu kết nối được
```

### 7. Cassandra không ready sau 10 phút

```bash
# Xem logs
kubectl logs cassandra-0 | tail -100

# Thường do: Insufficient resources
# Giảm replicas trong cassandra-config.yaml:
# replicas: 1  # thay vì 3

# Redeploy
kubectl delete -f k8s/cassandra-config.yaml
kubectl apply -f k8s/cassandra-config.yaml
```

### 8. Airflow DAG không hiện

```bash
# Check DAG file syntax
python dags/job_processing_dag.py
# Không có lỗi = OK

# Xem logs scheduler
kubectl logs -n airflow -l component=scheduler | grep ERROR

# Refresh DAGs (trong Airflow UI)
# DAGs page → Click refresh icon

# Hoặc restart scheduler
kubectl rollout restart deployment airflow-scheduler -n airflow
```

### 9. Image pull error: "unauthorized" hoặc "not found"

```bash
# Verify image exists in registry
docker images | grep spark-job

# Check image name đúng trong:
# - k8s/spark-job-template.yaml
# - dags/job_processing_dag.py

# Nếu dùng Docker Hub, đảm bảo image là public
# Hoặc tạo ImagePullSecret:
kubectl create secret docker-registry regcred \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=<your-username> \
  --docker-password=<your-password>

# Thêm vào pod spec:
# imagePullSecrets:
#   - name: regcred
```

### 10. Port-forward bị disconnect

```bash
# Thêm flag --address để bind tất cả interfaces
kubectl port-forward --address 0.0.0.0 svc/minio 9001:9001

# Hoặc run trong background (PowerShell)
Start-Process kubectl -ArgumentList "port-forward svc/minio 9001:9001" -WindowStyle Hidden
```

## Checklist Hoàn Thành Setup

Đánh dấu khi hoàn thành từng bước:

### Prerequisites

- [ ] Docker Desktop đã cài và chạy (`docker ps` thành công)
- [ ] Minikube đã start (`minikube status` = Running)
- [ ] kubectl connect được cluster (`kubectl get nodes` = Ready)
- [ ] Helm đã cài (`helm version`)
- [ ] Python đã cài (`python --version`)

### Data Preparation

- [ ] Đã tạo 20 file JSON trong `sample_data/`
- [ ] File JSON có đúng format (mở file kiểm tra)

### Infrastructure

- [ ] MinIO pods đang Running (`kubectl get pods -l app=minio`)
- [ ] Cassandra pods đang Running (`kubectl get pods -l app=cassandra`)
- [ ] Spark Master + Workers đang Running (`kubectl get pods | grep spark`)

### MinIO Setup

- [ ] Truy cập được MinIO console (http://localhost:9001)
- [ ] Đã tạo bucket `sensor-data`
- [ ] Đã upload 20 file JSON vào bucket
- [ ] Verify: Vào bucket thấy 20 files

### Cassandra Setup

- [ ] Đã init schema (`kubectl exec cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE metrics;"`)
- [ ] Thấy 3 tables: avg_salary_by_experience, jobs_by_work_type, job_processing_audit

### Spark Job

- [ ] Build image thành công
- [ ] Push image thành công (hoặc load vào Minikube)
- [ ] Test job chạy thành công (`kubectl logs job/spark-job-2026-01-01`)
- [ ] Query Cassandra thấy data cho date 2026-01-01

### Airflow (Optional)

- [ ] Airflow pods đang Running (`kubectl get pods -n airflow`)
- [ ] Truy cập được Airflow UI (http://localhost:8081)
- [ ] Đã set Variable `job_processing_current_date`
- [ ] DAG `job_processing_pipeline` đã enable
- [ ] DAG chạy thành công ít nhất 1 lần

## Lệnh Hữu Ích

```bash
# Restart tất cả pods của một service
kubectl rollout restart deployment <deployment-name>
kubectl rollout restart statefulset <statefulset-name>

# Delete và redeploy
kubectl delete -f <file>.yaml
kubectl apply -f <file>.yaml

# Xem resource usage
kubectl top nodes
kubectl top pods -A

# Port forward nhiều services cùng lúc (PowerShell)
Start-Process kubectl -ArgumentList "port-forward svc/minio 9001:9001"
Start-Process kubectl -ArgumentList "port-forward svc/spark-master 8080:8080"
Start-Process kubectl -ArgumentList "port-forward svc/airflow-webserver 8081:8080 -n airflow"

# Clean up resource để free memory
kubectl delete job --all  # Xóa completed jobs
kubectl delete pod --field-selector=status.phase==Succeeded  # Xóa succeeded pods

# Reset hoàn toàn để bắt đầu lại
kubectl delete namespace airflow
kubectl delete all --all
minikube delete
minikube start --driver=docker --cpus=4 --memory=8192
```

## Performance Tips

### Nếu máy yếu (8GB RAM)

```bash
# Start Minikube với ít resources hơn
minikube start --driver=docker --cpus=2 --memory=6144

# Giảm replicas trong configs:
# cassandra-config.yaml: replicas: 1
# spark-config.yaml: workers replicas: 1

# Giảm Spark executor memory trong spark_job.py:
# .config("spark.executor.memory", "1g")
```

### Nếu build/pull image chậm

```bash
# Dùng Minikube cache
minikube cache add apache/spark:3.5.1
minikube cache add cassandra:4.1
minikube cache add minio/minio:RELEASE.2023-09-30T07-02-29Z

# Build trực tiếp trong Minikube (không cần push)
eval $(minikube docker-env)  # Linux/Mac
minikube docker-env | Invoke-Expression  # PowerShell
docker build -f Dockerfile.spark -t spark-job:latest .
# Sửa imagePullPolicy: Never trong yaml files
```

## Next Steps: Production Deployment

Sau khi test thành công trên local, để deploy production:

1. **Setup Git Repository**

   - Push code lên GitHub/GitLab
   - Setup Git-Sync trong Airflow để tự động sync DAGs

2. **Use Cloud Kubernetes**

   - Azure AKS
   - AWS EKS
   - Google GKE

3. **Persistent Storage**

   - Replace emptyDir với PersistentVolumeClaim
   - Use cloud storage (Azure Blob, S3) cho MinIO data

4. **Monitoring**

   - Deploy Prometheus + Grafana
   - Setup alerts cho job failures

5. **Security**

   - Use secrets management (Azure Key Vault, AWS Secrets Manager)
   - Setup RBAC
   - Network policies

6. **CI/CD**
   - GitHub Actions để auto build/push images
   - ArgoCD hoặc Flux cho GitOps

---

**Chúc bạn setup thành công! 🚀**

Nếu gặp lỗi không có trong troubleshooting, hãy:

1. Copy full error message
2. Check logs: `kubectl logs <pod-name>`
3. Check events: `kubectl describe pod <pod-name>`
4. Google error message
5. Hỏi trên Stack Overflow hoặc Kubernetes Slack
