# Kiến Trúc Hệ Thống: Enterprise Hybrid Recommender Platform

> **Phiên bản:** 2.0 (Lakehouse, Streaming & MLOps Architecture)  
> **Tác giả:** Senior Cloud Architect & Technical Lead  
> **Tài liệu liên kết:** [imgs/architecture.png](../imgs/architecture.png)

---

## 1. Tổng Quan Kiến Trúc (Architecture Overview)

Hệ thống được thiết kế theo mô hình **Lambda / Lakehouse Architecture kết hợp Event-Driven Microservices**, phục vụ bài toán gợi ý sản phẩm cho sàn thương mại điện tử quy mô lớn với hàng triệu giao dịch và người dùng đồng thời.

Kiến trúc giải quyết triệt để 3 thách thức cốt lõi:
1. **Cold-start & Real-time Intent (Ý định mua tức thời):** Khách hàng vừa bỏ một món hàng vào giỏ, hệ thống phải gợi ý ngay lập tức các món mua kèm phù hợp nhất trong vòng **< 5ms**.
2. **Long-term Personalization (Sở thích dài hạn):** Sử dụng mạng nơ-ron sâu **PyTorch NCF** để học không gian vector tiềm ẩn (latent vector) 32 chiều biểu diễn gu thẩm mỹ và lịch sử của từng khách hàng.
3. **High-Throughput Ingestion & Lakehouse Analytics:** Tiếp nhận luồng sự kiện hành vi phễu (`view` $\rightarrow$ `add_to_cart` $\rightarrow$ `purchase`) qua **Apache Kafka** với tốc độ hàng chục nghìn events/giây, đồng thời lưu trữ vào **MinIO Lakehouse (Apache Parquet)** để phục vụ huấn luyện định kỳ.

```
+---------------------------------------------------------------------------------------------------+
|                                      CLIENT LAYER (React 19 + TS)                                 |
|                         [Storefront :3000] <----> [Telemetry Monitor]                             |
+---------------------------------------------------------------------------------------------------+
             | (1. User Clickstream)                                  ^ (Query Recs < 5ms)
             v                                                        |
+-------------------------------------+             +-----------------------------------------------+
|     APACHE KAFKA (Broker :9092)     |             |             FASTAPI GATEWAY (:8000)           |
|      Topic: ecom.events.raw         |             |   - Sub-5ms Serving Gateway                   |
+-------------------------------------+             |   - Hybrid Ensemble (FP-Growth + NCF)         |
      |                           |                 +-----------------------------------------------+
      | (Stream Consumer)         | (S3 Sink)                                 |
      v                           v                                           | O(1) Key Lookups
+--------------------+   +-------------------+                                v
| CART CONSUMER POD  |   | MINIO S3 LAKEHOUSE|      +-----------------------------------------------+
| - Session Window   |   | - Bronze Parquet  |      |           REDIS FEATURE STORE (:6379)         |
| - Sliding TTL 3600 |   | - Curated Marts   | <--- |   - cart:session:{id} (Active Items)          |
+--------------------+   +-------------------+      |   - rules:fpgrowth:{item} (Association Rules) |
      | (HSET)                    |                 |   - ncf:user:{id} (Precomputed Embeddings)    |
      +-------------------------> |                 +-----------------------------------------------+
                                  |
                                  v
+---------------------------------------------------------------------------------------------------+
|                            MLOPS & AIRFLOW ORCHESTRATION PIPELINE                                 |
|        [Airflow DAGs] ➔ [dbt Transformation] ➔ [FP-Growth & NCF Retrain] ➔ [MLflow Registry]       |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Phân Tầng Hệ Thống (Layered Architecture Breakdown)

### 2.1. Client & Presentation Layer (Giao diện người dùng)
- **Công nghệ:** React 19, TypeScript 6.0, Tailwind CSS v4, Vite 8, Lucide Icons.
- **Cổng dịch vụ:** Cổng `3000` (Docker Nginx) hoặc `5173` (Vite Dev Server).
- **Chức năng:**
  - **Storefront e-commerce:** Hiển thị sản phẩm thực tế từ Lakehouse dataset (Jumbo Bags, Cakestand, Lunchbox, v.v.).
  - **Giỏ hàng trượt (Slide-over Cart Drawer):** Đồng bộ trực tiếp với Session ID trên Redis, hiển thị TTL 3600s, tăng giảm số lượng, và nút Checkout mô phỏng bắn Kafka `purchase` event.
  - **Khối gợi ý thời gian thực:** 3 tab thuật toán (Hybrid Ensemble, Frequently Bought Together - FP-Growth, Personalized - NCF).
  - **AWS CloudWatch Telemetry Dashboard:** Theo dõi trực tiếp log sự kiện Kafka, độ trễ phản hồi của hệ thống theo thời gian thực.

### 2.2. Ingestion & Real-Time Streaming Layer (Tầng luồng dữ liệu)
- **Công nghệ:** Apache Kafka 3.6, Zookeeper, Kafka Connect.
- **Topic cốt lõi:** `ecom.events.raw` (Partition theo `user_id` để bảo toàn thứ tự sự kiện).
- **Chức năng:**
  - Tiếp nhận các event clickstream: `view`, `add_to_cart`, `purchase`.
  - Phân tán tải thông qua Kafka Consumer Group `recsys-cart-session-consumer`.
  - Đảm bảo tính chịu lỗi (Fault-tolerant), khả năng buffer dữ liệu khi có đỉnh tải (traffic spikes).

### 2.3. Feature Store & In-Memory State Layer (Tầng lưu trữ trạng thái)
- **Công nghệ:** Redis 7.2 (In-memory Data Store).
- **Cấu trúc dữ liệu tối ưu:**
  - `cart:session:{session_id}` (Redis Hash): Lưu trữ các món hàng hiện có trong giỏ kèm timestamp. Thiết lập `EXPIRE 3600` (sliding session window).
  - `rules:fpgrowth:{stock_code}` (Redis String/JSON): Lưu danh sách sản phẩm mua kèm và độ tin cậy Confidence/Lift. Truy vấn độ phức tạp $O(1)$.
  - `ncf:user:{user_id}` (Redis List/JSON): Lưu top-K sản phẩm được chấm điểm cao nhất cho từng người dùng từ mô hình Deep Learning.

### 2.4. Low-Latency Serving Layer (Tầng phục vụ API)
- **Công nghệ:** FastAPI 0.115, Uvicorn, Pydantic v2, Python 3.11.
- **Cổng dịch vụ:** Cổng `8000`.
- **Đặc tính kỹ thuật:**
  - Độ trễ phục vụ trung bình (P99 Serving Latency): **< 5ms** nhờ 100% dữ liệu được lấy từ Redis Feature Store trong bộ nhớ RAM.
  - Hỗ trợ cơ chế Fallback thông minh: Nếu Redis gặp sự cố, hệ thống tự động tra cứu fallback rules trong bộ nhớ đệm tiến trình mà không làm gián đoạn người dùng.
  - Middleware tự động đo lường độ trễ và trả về header `X-Response-Time-Ms`.

### 2.5. Data Lakehouse & Analytical Batch Layer (Tầng lưu trữ hồ dữ liệu)
- **Công nghệ:** MinIO (S3-compatible Object Storage), Apache Parquet, dbt Core 1.9, Apache Spark.
- **Mô hình Medallion:**
  - **Bronze Layer (`data/lakehouse/bronze/`):** Lưu trữ toàn bộ raw events dạng Parquet partitioned theo ngày và loại sự kiện.
  - **Silver Layer (`data/lakehouse/silver/`):** Dữ liệu đã làm sạch qua dbt, lọc các giao dịch trả hàng (`Quantity < 0`), định dạng chuẩn Customer ID và Invoice.
  - **Gold Layer (`data/lakehouse/gold/`):** Bảng Marts phục vụ huấn luyện:
    - `transaction_fpgrowth`: Danh sách các rổ hàng gộp theo `InvoiceNo` cho bài toán khai phá luật kết hợp.
    - `user_item_dl`: Ma trận tương tác ẩn giữa User và Item với trọng số hành vi (view=1, cart=3, purchase=5).

### 2.6. Continuous MLOps & Orchestration Layer (Tầng điều phối & MLOps)
- **Công nghệ:** Apache Airflow 2.8, PyTorch 2.0 (CUDA), MLflow Tracking & Model Registry.
- **DAGs điều phối:**
  - `daily_lakehouse_elt_pipeline`: Kích hoạt dbt và kiểm tra Data Quality Gates mỗi đêm.
  - `retrain_recommender_dag`: Kích hoạt khai phá lại luật FP-Growth (386 rules) và huấn luyện lại PyTorch NCF trên GPU, xuất artifact `.pt` và tự động đồng bộ vào Redis Feature Store.

### 2.7. CI/CD & Automated Quality Gates (Tầng kiểm thử & triển khai liên tục)
- **Công nghệ:** Jenkins Declarative Pipeline, pytest, Docker, Kubernetes Kustomize.
- **Pipeline tự động:** 9 stages từ linting, kiểm thử 17 test cases, xác thực trọng số mô hình, build multi-container image đến kiểm tra an ninh (Trivy) và triển khai tự động.

---

## 3. Luồng Dữ Liệu Hai Chiều (Dual-Loop Data Flow)

Hệ thống hoạt động theo 2 vòng lặp độc lập nhưng bổ trợ chặt chẽ cho nhau:

### Vòng lặp thời gian thực (Real-Time Online Loop - Tốc độ < 5ms)
1. Khách hàng click "Thêm vào giỏ" trên React Storefront.
2. Trình duyệt gửi request tới `/cart/add` của FastAPI, đồng thời bắn event `add_to_cart` vào Kafka topic `ecom.events.raw`.
3. Consumer đọc từ Kafka, cập nhật Hash `cart:session:{id}` trên Redis với TTL 3600s.
4. Giao diện frontend gọi `/recommend/hybrid?session_id=...&user_id=...`.
5. FastAPI đọc danh sách món trong giỏ từ Redis ($O(1)$) $\rightarrow$ tra cứu luật FP-Growth ($O(1)$) $\rightarrow$ kết hợp điểm NCF của user ($O(1)$) $\rightarrow$ trả về kết quả xếp hạng trong **2-4ms**.

### Vòng lặp Batch & Huấn luyện (Offline Batch & Training Loop)
1. Kafka Connect đẩy các micro-batch event từ Kafka vào MinIO Lakehouse dưới dạng file Parquet nén.
2. Airflow định kỳ kích hoạt dbt biến đổi dữ liệu thành các bảng Gold Mart.
3. Script huấn luyện MLOps đọc dữ liệu sạch, chạy thuật toán FP-Growth và mạng PyTorch NCF.
4. Checkpoint mô hình được ghi lại trên MLflow, và script `sync_redis_store.py` nạp toàn bộ luật và điểm số mới vào Redis Feature Store bằng kỹ thuật Redis Pipeline mà không gây gián đoạn (Zero-downtime hot reload).
