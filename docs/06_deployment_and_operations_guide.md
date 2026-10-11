# Hướng Dẫn Vận Hành & Triển Khai Hệ Thống (Deployment & Operations Guide)

Tài liệu này cung cấp hướng dẫn từng bước để triển khai, vận hành và giám sát toàn bộ nền tảng gợi ý thời gian thực trên cả môi trường cục bộ (Local / Docker Compose) và môi trường phân tán doanh nghiệp (Kubernetes Production).

---

## 1. Yêu Cầu Môi Trường (Prerequisites)

Trước khi bắt đầu, đảm bảo máy chủ của bạn đã cài đặt các công cụ sau:
- **Docker Engine:** $\ge 24.0$ & **Docker Compose:** $\ge 2.20$
- **Python:** $\ge 3.10$ (khuyến nghị Python 3.11)
- **Node.js:** $\ge 20.0$ (khuyến nghị Node.js v24.x) & **npm:** $\ge 10.x$
- **Bộ nhớ RAM tối thiểu:** 8 GB (khuyến nghị 16 GB để chạy đồng thời toàn bộ hạ tầng Kafka, Redis, MinIO, Postgres, Airflow và Frontend).

---

## 2. Triển Khai Nhanh Bằng Docker Compose (Quickstart)

Hệ thống đã cấu hình sẵn toàn bộ hạ tầng trong tệp `docker-compose.yml`.

### 2.1. Khởi Động Toàn Bộ Cụm Dịch Vụ
Chạy lệnh duy nhất tại thư mục gốc của dự án:
```bash
docker compose up -d --build
```

### 2.2. Bảng Địa Chỉ & Cổng Dịch Vụ (Service Endpoints)

| Dịch Vụ | Địa Chỉ Truy Cập | Mục Đích |
| :--- | :--- | :--- |
| **React Storefront UI** | `http://localhost:3000` | Giao diện mua sắm Amazon / AWS Console & Telemetry Monitor |
| **FastAPI Serving Gateway** | `http://localhost:8000` | API phục vụ gợi ý độ trễ < 5ms |
| **FastAPI Swagger Docs** | `http://localhost:8000/docs` | Tài liệu tương tác kiểm thử API |
| **Kafka UI Dashboard** | `http://localhost:8085` | Giám sát Topics, Consumer Groups và độ lệch tin nhắn (Lag) |
| **MinIO S3 Console** | `http://localhost:9001` | Quản lý Lakehouse Object Storage (User: `minioadmin` / Pass: `minioadminpassword`) |
| **Redis Feature Store** | `localhost:6379` | In-memory Feature Store & Session Cache |
| **PostgreSQL Metadata** | `localhost:5432` | Lưu trữ metadata dbt / Airflow (User: `postgres` / Pass: `abc123password`) |

### 2.3. Kiểm Tra Trạng Thái Sức Khỏe Toàn Hệ Thống
```bash
# Kiểm tra danh sách container đang chạy
docker compose ps

# Kiểm tra log của backend serving gateway
docker compose logs -f backend

# Kiểm tra log của streaming consumer
docker compose logs -f cart-consumer
```

---

## 3. Chạy Từng Thành Phần Trong Môi Trường Phát Triển (Local Dev)

Nếu bạn muốn debug trực tiếp mã nguồn trên máy cá nhân:

### Bước 1: Khởi động Hạ tầng Core (Kafka, Redis, MinIO, Postgres)
```bash
docker compose up -d zookeeper kafka redis minio postgres
```

### Bước 2: Cài Đặt Môi Trường Python
```bash
python -m venv .venv
source .venv/bin/activate  # Trên Linux/macOS
# Hoặc trên Windows: .venv\Scripts\activate

pip install -r backend/requirements.txt
pip install -r streaming/requirements.txt
pip install pytest pytest-cov
```

### Bước 3: Tải Dữ Liệu & Chuẩn Bị Lakehouse
```bash
python data_pipeline/ingest_kaggle_data.py
```
> Script sẽ chuẩn hóa 805,549 dòng giao dịch sạch vào tệp `data/lakehouse/real_transactions.parquet`.

### Bước 4: Khai Phá Luật FP-Growth & Đồng Bộ Lên Redis
```bash
python mlops/train_fpgrowth.py
python mlops/sync_redis_store.py
```
> Mined 386 luật kết hợp và đồng bộ trực tiếp vào Redis Feature Store trong < 3 giây.

### Bước 5: Khởi Động Streaming Consumer
Mở một cửa sổ terminal riêng biệt:
```bash
python streaming/consumers/cart_session_consumer.py
```

### Bước 6: Khởi Động FastAPI Backend
Mở một cửa sổ terminal riêng biệt:
```bash
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Bước 7: Khởi Động React TypeScript Storefront
Mở một cửa sổ terminal riêng biệt:
```bash
cd frontend
npm install
npm run dev
```
> Truy cập `http://localhost:5173` để trải nghiệm ứng dụng với tính năng Hot Module Replacement (HMR).

---

## 4. Chạy Kiểm Thử Tự Động (Running Test Suite)

Chạy toàn bộ 17 bài test tự động bất cứ lúc nào:
```bash
# Chạy toàn bộ test suite
python -m pytest tests/ -v

# Chạy có đo lường tỷ lệ bao phủ mã nguồn (Code Coverage)
python -m pytest tests/ --cov=backend --cov=mlops --cov=streaming

# Chạy xuất báo cáo JUnit XML cho Jenkins
python -m pytest tests/ --junitxml=test-reports/junit-results.xml
```

---

## 5. Triển Khai Trên Cụm Kubernetes (Kubernetes Production Guide)

Mã nguồn triển khai Kubernetes đã sẵn sàng tại thư mục `k8s/`:

### 5.1. Triển Khai Toàn Bộ Stack Bằng Kustomize
```bash
# Tạo namespace chuyên dụng
kubectl create namespace recsys-platform

# Áp dụng cấu hình k8s qua Kustomize
kubectl apply -k k8s/

# Theo dõi tiến độ khởi động của các Pods
kubectl get pods -n recsys-platform -w
```

### 5.2. Cấu Hình Tự Động Co Giãn (Horizontal Pod Autoscaling - HPA)
Dịch vụ FastAPI Serving Gateway được cấu hình HPA tự động co giãn từ 3 Pods lên tối đa 15 Pods khi lượng CPU vượt ngưỡng 70% hoặc lượng request tăng đột biến:
```bash
kubectl get hpa -n recsys-platform
```

---

## 6. Giám Sát & Xử Lý Sự Cố (Troubleshooting & Observability)

### 6.1. Đo Lường Độ Trễ Phục Vụ Của Redis
Để kiểm tra độ trễ vật lý của Redis Feature Store:
```bash
docker exec -it recsys_redis redis-cli --latency
```
> Kết quả thông thường: `min: 0, max: 1, avg: 0.12 (mili-giây)`.

### 6.2. Kiểm Tra Độ Trễ Dòng Tin Nhắn Kafka (Consumer Lag)
```bash
docker exec -it recsys_kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group recsys-cart-session-consumer
```
> Đảm bảo chỉ số `LAG` luôn xấp xỉ `0`, chứng minh consumer tiêu thụ kịp thời lượng sự kiện được sinh ra từ storefront.

### 6.3. Kiểm Tra Trạng Thái Sức Khỏe API
Gửi lệnh curl kiểm tra SLA:
```bash
curl -i http://localhost:8000/health
```
Header trả về bắt buộc phải có:
```http
HTTP/1.1 200 OK
content-type: application/json
x-response-time-ms: 1.84
```
Nếu `x-response-time-ms < 5.0`, hệ thống đạt chuẩn thỏa thuận mức dịch vụ (SLA) cam kết.
