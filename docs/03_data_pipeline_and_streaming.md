# Pipeline Dữ Liệu & Event Streaming (Data Pipeline & Streaming)

Tài liệu này mô tả chi tiết toàn bộ hành trình của dữ liệu: từ tập dữ liệu bán lẻ thực tế của Kaggle, qua thuật toán nội suy hành vi phễu mua sắm, đẩy vào dòng sự kiện thời gian thực Apache Kafka, cho đến cơ chế tiêu thụ phiên giỏ hàng trên Redis và lưu trữ phân tích tại Lakehouse.

---

## 1. Nguồn Dữ Liệu & Chuẩn Hóa Ban Đầu (Raw Ingestion)

### 1.1. Tập Dữ Liệu Thực Tế
Hệ thống sử dụng tập dữ liệu thực tế nổi tiếng **Kaggle UCI Online Retail II**:
- **Tổng số dòng thô:** `1,067,371` dòng giao dịch thương mại điện tử từ năm 2009 đến 2011 của một nhà bán lẻ quà tặng trực tuyến tại Anh.
- **Các trường dữ liệu gốc:**
  - `Invoice`: Mã số hóa đơn 6 chữ số (nếu bắt đầu bằng chữ 'C' là giao dịch hủy/trả hàng).
  - `StockCode`: Mã định danh sản phẩm (ví dụ: `85123A`, `22423`, `85099B`).
  - `Description`: Tên mô tả mặt hàng bằng tiếng Anh.
  - `Quantity`: Số lượng mặt hàng trên mỗi giao dịch.
  - `InvoiceDate`: Dấu thời gian giao dịch thực tế.
  - `Price`: Đơn giá trên mỗi sản phẩm (đồng Bảng Anh - GBP).
  - `Customer ID`: Mã định danh khách hàng (số nguyên 5 chữ số).
  - `Country`: Quốc gia của người mua.

### 1.2. Làm Sạch & Chuẩn Hóa Lakehouse
Dữ liệu thô được tải và làm sạch tự động qua script `data_pipeline/ingest_kaggle_data.py`:
1. **Lọc đơn hủy:** Loại bỏ các đơn hàng có `Invoice` bắt đầu bằng ký tự `'C'` hoặc `Quantity <= 0`.
2. **Khử nhiễu mô tả:** Loại bỏ các mã phí bưu điện (`POST`), điều chỉnh thủ công (`D`), kiểm tra kho (`AMAZONFEE`, `BAD`).
3. **Xử lý thiếu Customer ID:** Gán mã định danh khách hàng giả lập có kiểm soát cho các phiên khách vãng lai nhằm bảo toàn tính liên tục của rổ hàng.
4. **Kết quả:** Sinh ra file **`data/lakehouse/real_transactions.parquet`** gồm **`805,549` dòng giao dịch sạch**, được nén dạng Parquet Snappy với dung lượng chỉ **14.2 MB** (tiết kiệm hơn 85% dung lượng so với file CSV gốc 100MB).

---

## 2. Kỹ Thuật Nội Suy Hành Vi Phễu (Reverse Funnel Augmentation)

Trong thực tế, cơ sở dữ liệu bán hàng (Sales Database) chỉ lưu trữ các đơn hàng đã thanh toán thành công (`purchase`). Để phục vụ kiến trúc gợi ý thời gian thực và mô hình học máy hiện đại, hệ thống triển khai kỹ thuật **Nội suy hành vi ngược (Reverse Funnel Synthesis)**:

```
+---------------------------------------------------------------------------------------+
| HÀNH TRÌNH KHÁCH HÀNG THỰC TẾ (Synthesized Customer Journey)                          |
|                                                                                       |
|   [T - 300s]  VIEW Event          (Khách xem chi tiết sản phẩm A)                     |
|        ↓                                                                              |
|   [T - 180s]  VIEW Event          (Khách xem thêm sản phẩm phụ kiện B)                |
|        ↓                                                                              |
|   [T - 120s]  ADD_TO_CART Event   (Khách thêm sản phẩm A vào giỏ hàng)                |
|        ↓                                                                              |
|   [T - 60s]   ADD_TO_CART Event   (Khách thêm sản phẩm B vào giỏ hàng)                |
|        ↓                                                                              |
|   [T - 0s]    PURCHASE Event      (Khách thanh toán toàn bộ giỏ hàng Invoice #123456) |
+---------------------------------------------------------------------------------------+
```

### Các trường metadata được bổ sung vào từng event:
- `event_id`: UUID định danh duy nhất cho từng hành động.
- `event_type`: Loại sự kiện (`view`, `add_to_cart`, `purchase`).
- `session_id`: Định danh phiên mua sắm theo định dạng `sess_{user_id}_{hash}`.
- `user_id`: ID khách hàng.
- `stock_code`: Mã sản phẩm tương tác.
- `item_name`: Tên sản phẩm.
- `price`: Giá tiền.
- `quantity`: Số lượng.
- `device_type`: Thiết bị truy cập (`mobile_ios`, `mobile_android`, `desktop_chrome`, `desktop_safari`).
- `referrer`: Nguồn truy cập (`google_search`, `facebook_ads`, `direct`, `internal_recommendation`).
- `timestamp`: Epoch timestamp mili-giây.

---

## 3. Kiến Trúc Ingestion Dòng Sự Kiện Apache Kafka

### 3.1. Kafka Producer & Tốc Độ Truyền Tải
Script `data_pipeline/stream_producer.py` hoạt động như một Event Generator thông lượng cao:
- Sử dụng thư viện `kafka-python` với cơ chế gom lô tin nhắn `batch_size=16384` bytes và thời gian chờ `linger_ms=10`.
- Đạt tốc độ streaming lên đến **20,480 events/giây**, mô phỏng chính xác lưu lượng truy cập của các đợt Flash Sale / Mega Campaign trên các sàn TMĐT lớn.

### 3.2. Cấu Trúc Topic & Chiến Lược Phân Vùng (Partitioning Strategy)
- **Topic Name:** `ecom.events.raw`
- **Partitions:** 3 partitions (mở rộng linh hoạt lên 12 partitions trên production Kubernetes).
- **Partition Key:** Bắt buộc sử dụng `key=str(user_id)`.
  > **Ý nghĩa kỹ thuật quan trọng:** Kafka đảm bảo rằng toàn bộ các tin nhắn có cùng Partition Key sẽ được đưa vào cùng một partition duy nhất theo thuật toán MurmurHash2. Điều này bảo đảm **tính toàn vẹn thứ tự thời gian (Strict Ordering)**: sự kiện `view` luôn xảy ra trước `add_to_cart`, và `add_to_cart` luôn xảy ra trước `purchase` cho cùng một khách hàng, loại bỏ hoàn toàn hiện tượng Race Condition khi nhiều consumer xử lý song song.

---

## 4. Consumer Giỏ Hàng Thời Gian Thực (Cart Session Consumer)

Triển khai tại file `streaming/consumers/cart_session_consumer.py`.

### 4.1. Cơ Chế Xử Lý Theo Loại Sự Kiện
Microservice này liên tục lắng nghe topic `ecom.events.raw` thuộc Consumer Group `recsys-cart-session-consumer` và điều hướng nghiệp vụ:

1. **Sự kiện `view`:**
   - Đẩy mã món hàng và timestamp vào đầu danh sách lịch sử duyệt web: `LPUSH user:recent_views:{user_id} {item_json}`.
   - Giữ tối đa 20 lượt xem gần nhất: `LTRIM user:recent_views:{user_id} 0 19`.
   - Thiết lập thời gian sống 7 ngày: `EXPIRE user:recent_views:{user_id} 604800`.

2. **Sự kiện `add_to_cart` (Tâm điểm phục vụ Sub-5ms):**
   - Cập nhật thông tin món hàng vào Redis Hash:  
     `HSET cart:session:{session_id} {stock_code} {item_data_json}`
   - Gia hạn thời gian sống của phiên (Sliding Window TTL):  
     `EXPIRE cart:session:{session_id} 3600`  
     *(Mỗi khi khách hàng thêm món mới, cửa sổ phiên 60 phút lại được làm mới tự động).*
   - Lưu ánh xạ người dùng: `SET user:session:{user_id} {session_id} EX 3600`.
   - Ghi nhận vào tập active sessions: `SADD active:sessions {session_id}`.

3. **Sự kiện `purchase` (Checkout Hoàn Tất):**
   - Đẩy toàn bộ các món hàng trong rổ vào lịch sử mua hàng dài hạn của khách: `LPUSH user:purchase_history:{user_id} ...`
   - **Xóa trắng giỏ hàng hiện tại để tránh gợi ý lại các món đã mua:**  
     `DEL cart:session:{session_id}`  
     `SREM active:sessions {session_id}`

### 4.2. Khả Năng Chịu Lỗi & Graceful Shutdown
- Bắt tín hiệu `SIGINT` và `SIGTERM` từ Docker/Kubernetes.
- Tự động đóng commit offset cuối cùng trước khi tiến trình tắt hoàn toàn, không gây hiện tượng tiêu thụ lặp tin nhắn (duplicate processing).
- Bọc Redis Pipeline trong khối try-catch an toàn, hỗ trợ reconnect tự động khi cluster Redis failover.

---

## 5. Kiến Trúc Hồ Dữ Liệu Lakehouse (Medallion Architecture)

Dữ liệu lịch sử không bị mất đi mà được Kafka Connect đồng bộ liên tục vào MinIO Lakehouse:

### Tầng Đồng (Bronze Layer)
- Lưu trữ 100% dữ liệu sự kiện dạng thô, nén định dạng Parquet theo sơ đồ phân vùng:  
  `s3://lakehouse/bronze/events/year=YYYY/month=MM/day=DD/part-*.parquet`
- Bảo đảm tính bất biến (Immutable raw data), cho phép audit và huấn luyện lại bất cứ lúc nào.

### Tầng Bạc (Silver Layer)
- Dữ liệu được dbt làm sạch: chuẩn hóa kiểu dữ liệu, loại bỏ các giao dịch lỗi hoặc bản ghi trùng lặp (deduplication).

### Tầng Vàng (Gold Layer)
- Các Data Marts được tổng hợp sẵn sàng cho MLOps:
  - **`gold.transaction_fpgrowth`:** Danh sách rổ hàng đã gom nhóm theo `session_id` hoặc `invoice_no`, lọc các rổ có độ dài $\ge 2$ món để cấp cho thuật toán FP-Growth.
  - **`gold.user_item_interactions`:** Bảng điểm tương tác người dùng - sản phẩm được tính theo trọng số hành vi:
    $$\text{Score} = 1.0 \times \text{Views} + 3.0 \times \text{Carts} + 5.0 \times \text{Purchases}$$
    Bảng này được cấp trực tiếp cho mạng nơ-ron PyTorch NCF để học ma trận nhúng tiềm ẩn.
