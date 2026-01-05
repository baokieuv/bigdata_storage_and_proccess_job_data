# Dùng đúng phiên bản với Master/Worker để tránh lỗi version
FROM apache/spark:3.5.0

# Copy file code vào vị trí cố định trong ảnh
COPY job.py /opt/spark/job.py

# (Tùy chọn) Có thể set WORKDIR để lệnh chạy ngắn hơn
WORKDIR /opt/spark