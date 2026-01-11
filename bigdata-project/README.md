# 📊 BigData Pipeline - Job Processing System

Pipeline xử lý dữ liệu job postings theo batch từ MinIO → Spark → Cassandra với Airflow automation.

## 🚀 Quick Start

**Chỉ cần 1 lệnh:**

```powershell
# PowerShell as Administrator
cd bigdata-project\scripts\setup
.\run-all.ps1
```

✅ **Hoàn thành trong 15-20 phút!**

👉 **Hướng dẫn chi tiết:** [QUICKSTART.md](QUICKSTART.md)

## 📋 Kiến Trúc

```
┌─────────┐     ┌───────┐     ┌───────────┐     ┌─────────┐
│  MinIO  │────▶│ Spark │────▶│ Cassandra │◀────│ Airflow │
│ (JSON)  │     │ (ETL) │     │ (Results) │     │ (Cron)  │
└─────────┘     └───────┘     └───────────┘     └─────────┘
   20 files        Agg           3 tables        Every 5min
```

### Quy Trình Xử Lý

1. **MinIO**: Lưu 20 file JSON (jobs_2026-01-01.json → jobs_2026-01-20.json)
2. **Spark**: Đọc file theo ngày, tính:
   - Lương trung bình theo Experience Level
   - Số lượng job theo Work Type
3. **Cassandra**: Lưu kết quả + audit logs
4. **Airflow**: Tự động trigger Spark job mỗi 5 phút cho mỗi ngày

## 🛠️ Tech Stack

- **Kubernetes**: Minikube (local cluster)
- **Storage**: MinIO (S3-compatible)
- **Processing**: Apache Spark 3.5.1
- **Database**: Apache Cassandra 4.1
- **Orchestration**: Apache Airflow 2.7
- **Language**: Python 3.11, PySpark

## 📁 File Quan Trọng

| File                                                     | Mô Tả                          |
| -------------------------------------------------------- | ------------------------------ |
| [QUICKSTART.md](QUICKSTART.md)                           | 🌟 Bắt đầu đây - Setup tự động |
| [SETUP_FROM_SCRATCH.md](SETUP_FROM_SCRATCH.md)           | Hướng dẫn chi tiết từng bước   |
| [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)               | Deployment guide production    |
| [scripts/setup/run-all.ps1](scripts/setup/run-all.ps1)   | Script master - chạy tất cả    |
| [src/spark_job.py](src/spark_job.py)                     | Spark job xử lý data           |
| [dags/job_processing_dag.py](dags/job_processing_dag.py) | Airflow DAG                    |

## 🎯 Tính Năng

- ✅ **Tự động hóa hoàn toàn**: Script setup 1-click
- ✅ **Batch Processing**: Xử lý dữ liệu theo ngày
- ✅ **Fault Tolerance**: Retry mechanism, audit logging
- ✅ **Scalable**: Spark distributed processing
- ✅ **Production-ready**: Kubernetes deployment
- ✅ **Monitoring**: Cassandra audit logs, Spark UI, Airflow UI

## 🖥️ Yêu Cầu Hệ Thống

- **OS**: Windows 10/11
- **RAM**: 16GB minimum (32GB recommended)
- **Disk**: 50GB free space
- **CPU**: 4 cores+
- **Prerequisites**: Docker Desktop (script sẽ tự cài các tool khác)

## 📊 Kết Quả

Sau khi setup xong, bạn sẽ có:

### 1. MinIO (http://localhost:9001)

- Bucket `sensor-data` với 20 file JSON
- Mỗi file: 50-100 job postings

### 2. Cassandra Tables

```sql
-- Lương TB theo Experience Level
metrics.avg_salary_by_experience
  - experience_level, avg_salary, job_count, process_date

-- Số job theo Work Type
metrics.jobs_by_work_type
  - work_type, job_count, process_date

-- Audit logs
metrics.job_processing_audit
  - process_date, status, records_read, records_written
```

### 3. Spark UI (http://localhost:8080)

- Monitor job execution
- View workers, executors

### 4. Airflow UI (http://localhost:8081)

- DAG `job_processing_pipeline`
- Runs every 5 minutes
- Processes one day at a time

## 🔍 Example Query

```sql
-- Exec into Cassandra
kubectl exec -it cassandra-0 -- cqlsh

USE metrics;

-- Lương TB theo experience
SELECT experience_level, avg_salary, job_count
FROM avg_salary_by_experience
WHERE process_date='2026-01-01'
ALLOW FILTERING;

-- Output:
-- Entry level     | 50000.0  | 15
-- Mid-Senior level| 95000.0  | 28
-- Director        | 145000.0 | 7
-- Unknown         | 65000.0  | 10
```

## 🎓 Learning Path

1. **Beginner**: Chạy `run-all.ps1` → Xem kết quả trong UI
2. **Intermediate**: Đọc [spark_job.py](src/spark_job.py) → Hiểu aggregation logic
3. **Advanced**: Modify code → Add new metrics → Redeploy

## 🐛 Troubleshooting

### Quick Fixes

```powershell
# Restart tất cả
minikube delete
cd scripts\setup
.\run-all.ps1

# Check pods
kubectl get pods -A

# View logs
kubectl logs <pod-name>

# Query Cassandra
kubectl exec -it cassandra-0 -- cqlsh
```

### Common Issues

| Lỗi                     | Fix                                                |
| ----------------------- | -------------------------------------------------- |
| Docker not running      | Mở Docker Desktop                                  |
| Pods pending            | Giảm resources trong config                        |
| Image pull error        | Check image name trong YAML                        |
| Port-forward disconnect | Re-run: `kubectl port-forward svc/minio 9001:9001` |

👉 **Chi tiết:** [SETUP_FROM_SCRATCH.md](SETUP_FROM_SCRATCH.md) - Section Troubleshooting

## 📈 Next Steps

- [ ] Run complete pipeline (20 days)
- [ ] Add custom metrics to Spark job
- [ ] Deploy to cloud (AKS/EKS/GKE)
- [ ] Setup Grafana dashboards
- [ ] Add data quality checks
- [ ] Implement incremental processing

## 🤝 Contributing

1. Fork repo
2. Create feature branch
3. Make changes
4. Test with `run-all.ps1`
5. Submit PR

## 📝 License

MIT

---

**Made with ❤️ for BigData learning**

**Questions?** Check [SETUP_FROM_SCRATCH.md](SETUP_FROM_SCRATCH.md) or [QUICKSTART.md](QUICKSTART.md)
