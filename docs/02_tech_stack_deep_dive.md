# Phân Tích Chuyên Sâu Tech Stack (Tech Stack Deep Dive)

Tài liệu này giải thích chi tiết từng công nghệ trong hệ thống theo 4 câu hỏi định hướng:
1. **Để làm gì?** (Purpose & Core Responsibilities)
2. **Dùng như thế nào?** (Usage & Configuration trong dự án)
3. **Pipeline ra sao?** (Luồng xử lý dữ liệu và vị trí trong kiến trúc)
4. **Tại sao dùng nó?** (Lý do chọn, so sánh với giải pháp thay thế, trade-offs)

---

## 1. Apache Kafka & Kafka Connect (Event Streaming Ingestion)

### Để làm gì?
Đóng vai trò là xương sống trung chuyển tin nhắn phân tán (Distributed Event Streaming Backbone). Tiếp nhận toàn bộ clickstream events (`view`, `add_to_cart`, `purchase`) được sinh ra từ các phiên mua sắm với tốc độ cao, đảm bảo không mất mát dữ liệu và cách ly tải giữa tầng phục vụ người dùng và tầng lưu trữ phân tích.

### Dùng như thế nào?
- **Broker:** Chạy phiên bản `confluentinc/cp-kafka:7.5.0` với Zookeeper (`:2181`), lắng nghe trên cổng `9092` (cho client ngoài) và `29092` (cho nội bộ container network).
- **Topic:** `ecom.events.raw` được cấu hình 3 partitions, replication factor 1 (môi trường local/staging) hoặc 3 (môi trường production).
- **Partition Key:** Gán `key=str(user_id)` để đảm bảo mọi sự kiện của cùng một khách hàng luôn rơi vào cùng một partition theo đúng thứ tự thời gian.
- **Kafka Connect:** Sử dụng connector S3 Sink để định kỳ gom các micro-batches sự kiện và đẩy trực tiếp vào MinIO Lakehouse dưới định dạng Parquet.

### Pipeline ra sao?
```
[React Web / API] ➔ [Kafka Producer (JSON)] ➔ [Topic: ecom.events.raw] 
                                                      ├──> [Cart Session Consumer] ➔ [Redis]
                                                      └──> [Kafka Connect S3 Sink] ➔ [MinIO S3]
```

### Tại sao dùng nó?
- **So với REST API trực tiếp vào database:** Nếu ghi trực tiếp vào PostgreSQL khi có hàng chục nghìn lượt click mỗi giây, database quan hệ sẽ bị tắc nghẽn I/O và chết kết nối. Kafka đóng vai trò là hàng đợi đệm (buffer), cho phép ghi tuần tự (sequential disk I/O) với thông lượng vượt trội (> 100k events/s).
- **So với RabbitMQ:** RabbitMQ là message broker hướng tin nhắn (message-oriented) tập trung vào việc xóa tin sau khi tiêu thụ. Kafka là log phân tán (distributed commit log) cho phép nhiều consumers khác nhau (Consumer giỏ hàng, Consumer chống gian lận, Kafka Connect lưu trữ Lakehouse) cùng đọc độc lập tại các offset khác nhau và có thể replay lại khi cần.

---

## 2. Redis 7.2 (Feature Store & In-Memory Session Cache)

### Để làm gì?
Lưu trữ trạng thái giỏ hàng thời gian thực (Active Cart State) và đóng vai trò là **Online Feature Store** lưu trữ toàn bộ luật kết hợp FP-Growth và vector nhúng người dùng từ mô hình NCF, bảo đảm thời gian truy vấn ở mức microsecond ($O(1)$ time complexity).

### Dùng như thế nào?
- **Cấu hình:** `redis:7.2-alpine`, bật cơ chế bền vững `appendonly yes`, giới hạn RAM `maxmemory 512mb` và chính sách dọn dẹp `allkeys-lru`.
- **Cấu trúc dữ liệu được sử dụng:**
  - `cart:session:{session_id}` $\rightarrow$ **Redis Hash**: Mỗi trường (field) là `stock_code`, giá trị là JSON chi tiết (`quantity`, `price`, `timestamp`). Áp dụng lệnh `EXPIRE 3600` để tự động dọn rác sau 1 giờ không hoạt động.
  - `rules:fpgrowth:{stock_code}` $\rightarrow$ **Redis String (JSON)**: Chứa danh sách các mặt hàng mua kèm được tính toán trước kèm `confidence` và `lift`.
  - `active:sessions` $\rightarrow$ **Redis Set**: Lưu danh sách session ID đang hoạt động phục vụ giám sát.

### Pipeline ra sao?
```
[Kafka Consumer] ➔ HSET cart:session:{id} + EXPIRE 3600 ➔ [Redis Memory]
[MLOps Sync Task] ➔ MSET rules:fpgrowth:{item}           ➔ [Redis Memory]
[FastAPI Serving] ➔ HGETALL / MGET (< 1ms lookup)        ➔ Trả về gợi ý cho Client
```

### Tại sao dùng nó?
- **Đạt chuẩn độ trễ < 5ms (Sub-5ms SLA):** Không có cơ sở dữ liệu quan hệ hay tài liệu nào (PostgreSQL, MongoDB) có thể cạnh tranh với Redis khi thực hiện tra cứu rổ hàng trong vòng chưa đầy 1 mili-giây.
- **Tính năng Sliding Window TTL nguyên tử:** Redis quản lý vòng đời phiên làm việc (Session Lifecycle) hoàn toàn tự động thông qua lệnh `EXPIRE`, giải phóng hoàn toàn code backend khỏi việc phải viết các cronjob nặng nề để quét và xóa các session quá hạn trong DB.

---

## 3. PyTorch (Neural Collaborative Filtering - NCF)

### Để làm gì?
Huấn luyện mô hình lọc cộng tác dạng mạng nơ-ron sâu (Deep Learning Recommender) để học các đặc trưng tiềm ẩn (latent representations) của người dùng và sản phẩm, từ đó giải quyết bài toán gợi ý mang tính cá nhân hóa dài hạn (Personalized Discovery).

### Dùng như thế nào?
- **Mã nguồn:** Triển khai tại `mlops/train_ncf.py` và load phục vụ trong `backend/app/services/recommender.py`.
- **Cấu trúc mạng:**
  - Gồm 2 ma trận nhúng: `torch.nn.Embedding(n_users, 32)` và `torch.nn.Embedding(n_items, 32)`.
  - Kết hợp hai nhánh: Phép nhân từng phần ma trận Generalized Matrix Factorization (GMF) và nhánh mạng Perceptron đa tầng (MLP: Linear 64 $\rightarrow$ ReLU $\rightarrow$ Linear 32 $\rightarrow$ Dropout).
  - Lớp đầu ra: Hàm kích hoạt `Sigmoid` đưa điểm số dự đoán về khoảng xác suất $[0.0, 1.0]$.
- **Môi trường:** Tự động phát hiện GPU `cuda` (NVIDIA RTX 4090 / T4) và fallback về CPU nếu không có phần cứng chuyên dụng.

### Pipeline ra sao?
```
[Lakehouse Gold Mart (user_item_dl)] ➔ [PyTorch DataLoader] ➔ [Huấn luyện NCF trên GPU]
➔ [Xuất Checkpoint: models/ncf_model.pt] ➔ [MLflow Versioning] ➔ [Precompute Top-K vào Redis]
```

### Tại sao dùng nó?
- **Vượt trội so với Ma trận phân rã truyền thống (Classic SVD / Matrix Factorization):** SVD tuyến tính chỉ mô hình hóa được phép nhân vô hướng đơn giản giữa các vector. Mạng NCF của He et al. (2017) học được các hàm tương tác phi tuyến tính phức tạp giữa User và Item, tăng độ chính xác gợi ý (Hit Ratio@10 và NDCG@10) lên hơn 15%.
- **Hệ sinh thái chuẩn công nghiệp:** PyTorch cho phép lưu toàn bộ cấu trúc mạng cùng từ điển trọng số và metadata ánh xạ (`user2idx`, `item2idx`) trong một file duy nhất, nạp cực nhanh vào bộ nhớ phục vụ.

---

## 4. Thuật Toán FP-Growth (Frequent Pattern Growth - Market Basket Analysis)

### Để làm gì?
Khai phá các tập mục phổ biến và sinh các luật kết hợp (Association Rules) từ lịch sử đơn hàng để trả lời câu hỏi: *"Khách hàng mua món A thường sẽ mua kèm món B nào nhất?"* (Cross-selling).

### Dùng như thế nào?
- **Triển khai:** `mlops/train_fpgrowth.py` sử dụng thư viện tối ưu `mlxtend.frequent_patterns`.
- **Tham số cấu hình:**
  - `min_support = 0.015`: Lọc các sản phẩm xuất hiện trong ít nhất 1.5% tổng số rổ hàng.
  - `metric = "confidence"`, `min_threshold = 0.3`: Chỉ giữ các luật có xác suất mua kèm khi đã chọn tiền đề $\ge 30\%$.
  - Tiêu chí lọc Lift: Chỉ giữ các luật có $\text{Lift} > 1.2$ (chứng minh mối quan hệ tương quan đồng biến thực sự, loại bỏ trùng lặp ngẫu nhiên).

### Pipeline ra sao?
```
[805k Giao dịch Parquet] ➔ [Nhóm theo Invoice: Basket One-Hot Matrix]
➔ [Xây dựng FP-Tree] ➔ [Sinh 386 Association Rules] ➔ [Xuất JSON data/models/fpgrowth_rules.json]
```

### Tại sao dùng nó?
- **So với thuật toán Apriori:** Thuật toán kinh điển Apriori phải quét lại toàn bộ database nhiều lần (multiple database scans) và sinh ra tập ứng viên khổng lồ (candidate generation) gây cạn kiệt bộ nhớ RAM. FP-Growth nén toàn bộ database vào một cây tiền tố **FP-Tree** chỉ sau 2 lần quét, cho phép đào luật từ **805,000 dòng dữ liệu chỉ trong 2.90 giây**.
- **Tính giải thích được cao (Explainable AI):** Mỗi gợi ý đưa ra cho người dùng đều có chỉ số toán học chứng minh rõ ràng: *"Mặt hàng này có Lift 5.42x vì 68% khách mua Túi A cũng mua kèm Túi B"*.

---

## 5. FastAPI (Serving Gateway Microservice)

### Để làm gì?
Đóng vai trò là Cổng giao tiếp API (API Gateway) tốc độ cao, tiếp nhận các yêu cầu từ phía Frontend, truy vấn Feature Store và thực thi giải thuật xếp hạng phối hợp (Hybrid Ranking Ensemble).

### Dùng như thế nào?
- Triển khai trong `backend/app/main.py` với máy chủ Uvicorn bất đồng bộ (Asynchronous ASGI).
- **Các API chính:**
  - `GET /health`: Kiểm tra trạng thái kết nối Redis và tính sẵn sàng của mô hình.
  - `POST /cart/add`: Ghi nhận mặt hàng vào phiên làm việc.
  - `GET /cart/{session_id}`: Xem trạng thái giỏ hàng hiện tại.
  - `GET /recommend/cart/{session_id}`: Trả về luật mua kèm tức thì từ giỏ hàng.
  - `GET /recommend/user/{user_id}`: Trả về gợi ý cá nhân hóa từ NCF.
  - `POST & GET /recommend/hybrid`: Xếp hạng hỗn hợp theo công thức kết hợp trọng số $\alpha$.

### Pipeline ra sao?
```
Client Request ➔ [FastAPI Middleware (đo latency)] ➔ [Pydantic Validation]
➔ [Tra cứu Redis Feature Store] ➔ [Hybrid Ensemble Blending] ➔ Trả về JSON (< 5ms)
```

### Tại sao dùng nó?
- **Hiệu năng dẫn đầu:** Xây dựng trên nền tảng Starlette và uvloop, FastAPI đạt tốc độ tương đương NodeJS và Golang trong các tác vụ I/O bất đồng bộ.
- **Tự động sinh tài liệu Swagger/OpenAPI:** Cung cấp sẵn UI kiểm thử tương tác tại `/docs` mà không cần viết tài liệu thủ công.
- **Pydantic Data Validation:** Tự động kiểm tra tính hợp lệ của kiểu dữ liệu đầu vào, ngăn chặn hoàn toàn các lỗi type error trong quá trình runtime.

---

## 6. React 19 + TypeScript 6 + Tailwind CSS v4 (Modern Storefront UI)

### Để làm gì?
Cung cấp giao diện cửa hàng thương mại điện tử hiện đại, sinh động, lấy cảm hứng từ Amazon và AWS Management Console, cho phép người dùng trải nghiệm thực tế khả năng gợi ý thời gian thực và theo dõi luồng dữ liệu phân tán.

### Dùng như thế nào?
- Đặt tại thư mục `frontend/`, đóng gói bằng Vite 8 và phục vụ trong container bằng Nginx 1.27.
- **Tính năng nổi bật:**
  - `Navbar.tsx`: Tích hợp bộ chọn User ID giả lập để quan sát sự thay đổi ngay lập tức của mô hình NCF tương ứng với từng hồ sơ khách hàng.
  - `RecommendationSection.tsx`: Cho phép chuyển đổi giữa 3 góc nhìn thuật toán (Hybrid, FP-Growth, NCF) kèm biểu đồ phần trăm tin cậy.
  - `CartDrawer.tsx`: Giỏ hàng trượt hiển thị Session ID Redis và TTL 3600s.
  - `TelemetryDrawer.tsx`: Bảng điều khiển giả lập bắn event Kafka trực tiếp và đo đạc độ trễ serving.

### Pipeline ra sao?
```
[User Clicks / Search] ➔ [React State & Custom Hooks] ➔ [Fetch API /api proxy]
➔ [FastAPI Backend] ➔ [Cập nhật DOM & Render UI trong < 16ms]
```

### Tại sao dùng nó?
- **TypeScript Type Safety:** Ngăn chặn triệt để lỗi logic giao diện bằng cách đồng bộ kiểu dữ liệu chặt chẽ với schema API backend.
- **Tailwind CSS v4:** Biên dịch CSS cực nhanh dựa trên engine Lightning CSS mới, không cần file config cồng kềnh, tối ưu bundle size nhỏ dưới 35kB.

---

## 7. Jenkins Declarative Pipeline (Continuous Integration & Delivery)

### Để làm gì?
Tự động hóa hoàn toàn chu trình DevOps/MLOps: từ kiểm tra mã nguồn, chạy kiểm thử tự động, kiểm tra tính toàn vẹn của mô hình ML, đóng gói Docker images đa thành phần đến triển khai lên môi trường staging/production.

### Dùng như thế nào?
- Định nghĩa trong tệp `Jenkinsfile` với cú pháp Declarative Pipeline chuẩn mực.
- **9 Stages tự động:**
  1. `Environment Diagnosis & Checkout`
  2. `Code Quality & Linting` (Python `flake8`, Frontend `npm run build`)
  3. `Automated Unit & Integration Tests` (chạy `pytest`, xuất báo cáo `junit-results.xml`)
  4. `ML Artifact & Model Gate` (kiểm tra tồn tại và dung lượng file `.pt`, `.json`)
  5. `Automated Model Retrain & Sync` (kích hoạt có điều kiện khi tham số `RETRAIN_MODELS=true`)
  6. `Multi-Container Image Build` (build đồng thời backend, frontend, consumer)
  7. `Security Vulnerability Scan` (kiểm tra lỗ hổng qua Trivy)
  8. `Continuous Deployment` (triển khai Kubernetes / Docker Compose)
  9. `Post-Deployment SLA Check` (gửi probe kiểm tra `/health` và độ trễ < 5ms)

### Pipeline ra sao?
```
Git Push ➔ [Jenkins Webhook] ➔ [Chạy 9 Stages] ➔ [Cổng duyệt Chất Lượng] ➔ [Triển khai Staging/Prod]
```

### Tại sao dùng nó?
- **Khả năng kiểm soát tuyệt đối (Enterprise Standard):** Khác với GitHub Actions phụ thuộc vào cloud vendor và bị giới hạn thời gian chạy máy tính GPU, Jenkins có thể chạy on-premise trên chính hạ tầng của doanh nghiệp, dễ dàng mount trực tiếp ổ cứng chứa dữ liệu Lakehouse hàng trăm GB và GPU chuyên dụng để huấn luyện mô hình.

---

## 8. pytest (Automated Testing Framework)

### Để làm gì?
Đảm bảo chất lượng phần mềm, tính toàn vẹn của API contract, logic tích lũy giỏ hàng và tính chính xác của các thuật toán ML trước khi bất kỳ đoạn code nào được đưa lên môi trường sản xuất.

### Dùng như thế nào?
- Đặt tại thư mục `tests/` với file cấu hình `pytest.ini`.
- Gồm **17 test cases tự động** chia thành 4 nhóm:
  - `test_api.py`: 9 bài test kiểm tra toàn bộ endpoints của FastAPI, Pydantic schema validation và header đo độ trễ.
  - `test_fpgrowth_mlops.py`: 2 bài test kiểm tra cấu trúc luật khai phá và pipeline đồng bộ vào Redis.
  - `test_ncf_model.py`: 3 bài test kiểm tra tính toán tensor PyTorch, khoảng chặn Sigmoid, top-K ranking và tính toàn vẹn của checkpoint model.
  - `test_streaming_consumer.py`: 3 bài test kiểm tra schema sự kiện JSON Kafka, cơ chế tích lũy session giỏ hàng với TTL 3600s và xóa giỏ khi checkout.
- Cung cấp `MockRedis` trong `tests/conftest.py` mô phỏng 100% các lệnh Redis trong RAM để chạy test siêu tốc (2.25 giây) mà không cần cài đặt Redis server vật lý.

---

## 9. Apache Airflow 2.8 (Workflow & MLOps Orchestration)

### Để làm gì?
Điều phối các luồng công việc phức tạp theo đồ thị có hướng không chu trình (Directed Acyclic Graph - DAG), đảm bảo các tác vụ biến đổi dữ liệu, kiểm tra chất lượng dữ liệu và huấn luyện lại mô hình diễn ra tuần tự, đúng lịch trình và có khả năng tự động thử lại khi gặp sự cố (Auto-retry).

### Dùng như thế nào?
- DAG `retrain_recommender_dag.py` được lập lịch chạy định kỳ mỗi tuần hoặc kích hoạt qua Webhook.
- Các Task được cấu hình phụ thuộc:
  `Check_Lakehouse_Data` $\rightarrow$ `Train_FPGrowth_Rules` $\rightarrow$ `Train_PyTorch_NCF` $\rightarrow$ `Sync_Redis_Feature_Store` $\rightarrow$ `Validate_Serving_Health`.

### Tại sao dùng nó?
- **Trực quan hóa và Giám sát:** Cung cấp Web UI chi tiết để theo dõi trạng thái thực thi của từng node trong DAG, thời gian chạy, log lỗi cụ thể.
- **Backfilling:** Dễ dàng chạy bù dữ liệu quá khứ cho bất kỳ khoảng thời gian nào.

---

## 10. MinIO (S3-Compatible Lakehouse Object Storage)

### Để làm gì?
Lưu trữ toàn bộ dữ liệu lịch sử sự kiện (Historical Events) và các bảng dữ liệu dạng cột (Columnar Data) dưới định dạng Apache Parquet, đóng vai trò là tầng lưu trữ bền vững giá rẻ (Cheap, Durable Storage Layer) cho Lakehouse.

### Dùng như thế nào?
- Chạy container `minio/minio` với console web tại cổng `9001` và S3 API tại cổng `9000`.
- Dữ liệu được lưu trữ dạng file Parquet phân vùng theo ngày: `s3://lakehouse/bronze/year=2026/month=10/day=12/events.parquet`.

### Tại sao dùng nó?
- **Tương thích hoàn toàn với AWS S3 API:** Cho phép code chạy mượt mà ở môi trường phát triển local (qua MinIO) và chuyển dịch lên AWS S3 trên Cloud mà không cần sửa đổi bất kỳ dòng mã nào.
- **Tối ưu hóa dung lượng:** Định dạng Parquet kết hợp thuật toán nén Snappy giúp tiết kiệm hơn **75% dung lượng đĩa** so với định dạng JSON hoặc CSV truyền thống.
