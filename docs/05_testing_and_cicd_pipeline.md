# Kiểm Thử Tự Động & Pipeline CI/CD Jenkins (Testing & CI/CD Pipeline)

Tài liệu này hướng dẫn chi tiết về bộ kiểm thử tự động (Automated Test Suite) đạt chuẩn 100% tỷ lệ vượt qua và Pipeline phân phối liên tục (Declarative CI/CD Pipeline) bằng Jenkins cho nền tảng gợi ý lai.

---

## 1. Chiến Lược Kiểm Thử (Testing Strategy & Architecture)

Hệ thống tuân thủ mô hình **Kim Tự Tháp Kiểm Thử (Test Pyramid)** kết hợp cơ chế cô lập dịch vụ ngoài (Service Isolation via Mocking):

```
                       ▲
                      / \       [E2E SLA Check (<5ms Probe)] (1 Test)
                     /   \
                    /     \     [Integration Tests (API & Redis Pipeline)] (9 Tests)
                   /───────\
                  /         \   [Unit Tests (ML Tensor, Rules, Kafka Schema)] (7 Tests)
                 /───────────\
```

### 1.1. Kiến Trúc Giả Lập Redis In-Memory (Redis Mock Architecture)
Triển khai tại file `tests/conftest.py` và `tests/test_streaming_consumer.py`.
- **Thách thức:** Nếu các bài kiểm thử unit test phụ thuộc vào một server Redis thật đang chạy bên ngoài, môi trường test sẽ trở nên mong manh (flaky), dễ thất bại khi chạy trên máy tính của developer khác hoặc trên máy chủ Jenkins không có sẵn service Redis.
- **Giải pháp:** Xây dựng `MockRedis` và `MockStreamingRedisClient` hỗ trợ 100% các thao tác bộ nhớ RAM:
  - Lệnh cơ bản: `get`, `set`, `setex`, `delete`, `flushdb`, `ping`.
  - Lệnh băm: `hset`, `hget`, `hgetall` cho giỏ hàng.
  - Lệnh tập hợp: `sadd`, `srem`.
  - Khối thực thi hàng loạt: `pipeline()` hỗ trợ gom lệnh nguyên tử và thực thi đồng loạt với hàm `execute()`.
- **Hiệu năng:** Toàn bộ **17 bài test chạy hoàn tất chỉ trong 2.25 giây**, không tiêu tốn I/O mạng vật lý.

---

## 2. Chi Tiết 17 Test Cases Trong Bộ Kiểm Thử (pytest)

### 2.1. Nhóm Kiểm Thử API Phục Vụ (`tests/test_api.py` - 9 Tests)
1. `test_health_check`: Đảm bảo endpoint `/health` trả về mã HTTP `200`, báo cáo trạng thái `healthy` hoặc `degraded`, cờ kết nối Redis và thông tin thời gian thực.
2. `test_cart_add_item`: Kiểm tra việc gửi payload hợp lệ tới `/cart/add`, xác nhận phản hồi `status: "ok"` và thông báo bổ sung mặt hàng thành công.
3. `test_get_cart_existing_session`: Xác thực việc lấy thông tin giỏ hàng của phiên đã có dữ liệu, kiểm tra `total_items >= 1` và danh sách chi tiết các món hàng.
4. `test_get_cart_non_existent_session`: Đảm bảo khi truy vấn một session không tồn tại, API trả về an toàn mã `200` với từ điển rỗng `{}` thay vì gây lỗi sập server (500 Internal Error).
5. `test_recommend_cart_fpgrowth`: Kiểm tra việc gợi ý sản phẩm mua kèm từ giỏ hàng hiện tại qua giải thuật FP-Growth, bảo đảm các ứng viên có điểm số và lý do gợi ý rõ ràng.
6. `test_recommend_user_personalized`: Kiểm tra endpoint gợi ý cá nhân hóa theo User ID dựa trên mô hình Deep Learning NCF.
7. `test_recommend_hybrid_ensemble_post`: Xác thực endpoint gợi ý lai `/recommend/hybrid` qua phương thức `POST` với payload JSON chứa tham số trọng số $\alpha$.
8. `test_recommend_hybrid_ensemble_get`: Xác thực endpoint `/recommend/hybrid` qua phương thức `GET` với Query Parameters, đảm bảo tính tiện lợi khi gọi từ trình duyệt.
9. `test_process_time_header_middleware`: Đảm bảo middleware đo lường độ trễ hoạt động chuẩn xác và trả về header `x-response-time-ms >= 0.0`.

### 2.2. Nhóm Kiểm Thử MLOps & FP-Growth (`tests/test_fpgrowth_mlops.py` - 2 Tests)
10. `test_association_rule_schema_validation`: Xác thực schema của bảng luật kết hợp, đảm bảo có đủ các cột `antecedents`, `consequents`, `confidence`, `lift`, và độ tin cậy nằm trong khoảng toán học $[0.0, 1.0]$.
11. `test_sync_rules_to_mock_redis`: Kiểm tra script `sync_redis_store.py` đọc đúng định dạng file JSON luật và sử dụng Redis Pipeline để ghi vào key `rules:fpgrowth:{item}`.

### 2.3. Nhóm Kiểm Thử Mô Hình Mạng Nơ-ron PyTorch NCF (`tests/test_ncf_model.py` - 3 Tests)
12. `test_ncf_forward_pass_dimensions`: Kiểm tra quá trình forward pass của mạng nơ-ron: nhận batch User tensor và Item tensor, xuất vector đầu ra 1 chiều có giá trị bị chặn bởi hàm Sigmoid trong $[0.0, 1.0]$.
13. `test_ncf_batch_ranking`: Kiểm tra việc xếp hạng Top-K ứng viên cho một người dùng mục tiêu, xác nhận kết quả trả về được sắp xếp giảm dần nghiêm ngặt theo điểm số.
14. `test_ncf_checkpoint_weights_integrity`: Kiểm tra việc lưu và nạp tệp checkpoint PyTorch `.pt`, đảm bảo cấu trúc ma trận nhúng `user2idx` và `item2idx` không bị sai lệch dữ liệu.

### 2.4. Nhóm Kiểm Thử Streaming Consumer (`tests/test_streaming_consumer.py` - 3 Tests)
15. `test_kafka_event_payload_schema`: Kiểm tra schema sự kiện JSON tiếp nhận từ Kafka topic `ecom.events.raw` đầy đủ các trường nghiệp vụ.
16. `test_session_cart_accumulation`: Kiểm tra hàm tích lũy giỏ hàng theo session ID và thiết lập chính xác sliding TTL 3600 giây trên Redis.
17. `test_purchase_event_clears_cart`: Kiểm tra sự kiện thanh toán thành công (`purchase`): toàn bộ rổ hàng trong session bị xóa dọn sạch khỏi Redis và được lưu trữ vào lịch sử người dùng.

---

## 3. Quy Trình CI/CD Jenkins (Jenkins Declarative Pipeline)

Được cấu hình đầy đủ tại tệp **`Jenkinsfile`** ở thư mục gốc của repository.

```
+---------------------------------------------------------------------------------------+
| JENKINS CI/CD WORKFLOW (9 AUTOMATED STAGES)                                           |
|                                                                                       |
|  [Stage 1] Environment Diagnosis & Checkout (Python, Node, Docker tools)              |
|      ↓                                                                                |
|  [Stage 2] Code Quality & Linting (flake8 Python + npm build TypeScript)              |
|      ↓                                                                                |
|  [Stage 3] Automated Unit & Integration Tests (17 pytest cases + JUnit Report)        |
|      ↓                                                                                |
|  [Stage 4] ML Artifact & Model Gate (Xác thực models/ncf_model.pt & rules.json)      |
|      ↓                                                                                |
|  [Stage 5] Automated Model Retrain & Sync (Conditional: RETRAIN_MODELS=true)          |
|      ↓                                                                                |
|  [Stage 6] Multi-Container Image Build (Backend, Frontend Nginx, Stream Consumer)     |
|      ↓                                                                                |
|  [Stage 7] Security Vulnerability Scan (Trivy Container Scan)                         |
|      ↓                                                                                |
|  [Stage 8] Continuous Deployment (Rolling deploy Kubernetes / Docker Compose)         |
|      ↓                                                                                |
|  [Stage 9] Post-Deployment Verification & SLA Check (Probe /health < 5ms)             |
+---------------------------------------------------------------------------------------+
```

### 3.1. Các Tham Số Điều Khiển (Pipeline Parameters)
- `RETRAIN_MODELS` (boolean, default: `false`): Cho phép kích hoạt việc khai phá lại luật từ dữ liệu Lakehouse và huấn luyện lại PyTorch NCF ngay trong pipeline.
- `SYNC_REDIS_FEATURE_STORE` (boolean, default: `true`): Điều khiển việc nạp các luật mới vào cụm Redis.
- `DEPLOY_TARGET` (choice: `staging`, `production`, `dry-run`): Chỉ định môi trường triển khai mục tiêu.
- `DOCKER_REGISTRY` (string, default: `ghcr.io/nghon4maeri`): Tiền tố container registry.
- `KUBE_NAMESPACE` (string, default: `recommendation-platform`): Kubernetes namespace đích.

### 3.2. Cổng Chất Lượng & Báo Cáo (Quality Gates & Reporting)
1. **Báo cáo JUnit Test Report:** Lưu trữ tại `test-reports/junit-results.xml`, tích hợp trực tiếp vào giao diện Jenkins UI với biểu đồ xu hướng test (Test Trend Chart).
2. **Cổng kiểm tra trọng số mô hình (Model Gate):** Nếu tệp trọng số `models/ncf_model.pt` bị hỏng hoặc dung lượng bằng 0 byte, pipeline sẽ chủ động dừng lại trước khi đóng gói container, ngăn chặn việc đẩy mô hình lỗi lên môi trường production.
3. **Cổng kiểm tra SLA sau triển khai (Post-Deploy Probe):** Jenkins tự động gửi HTTP probe tới endpoint `/health` trên máy chủ production, đo lường thời gian phản hồi thực tế. Nếu thời gian phản hồi vượt quá 50ms hoặc trạng thái không phải `healthy`, pipeline sẽ cảnh báo và kích hoạt rollback.
