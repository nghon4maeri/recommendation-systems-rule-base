# MLOps & Các Mô Hình Gợi Ý Lai (MLOps & Recommender Models)

Tài liệu này trình bày chi tiết nền tảng toán học, kiến trúc mạng nơ-ron, quy trình huấn luyện và cơ chế phối hợp (Ensemble) của hệ thống gợi ý lai (Hybrid Recommender System: FP-Growth + PyTorch NCF).

---

## 1. Thuật Toán Khai Phá Luật Kết Hợp (FP-Growth Association Mining)

### 1.1. Nền Tảng Toán Học
Cho tập rổ hàng $\mathcal{D} = \{T_1, T_2, \dots, T_N\}$, mỗi rổ hàng $T_k$ là tập hợp các sản phẩm $T_k \subseteq \mathcal{I}$. Luật kết hợp có dạng $X \Rightarrow Y$, trong đó $X$ là tiền đề (antecedent - các món đang có trong giỏ) và $Y$ là hệ quả (consequent - món được gợi ý mua kèm).

Hệ thống đánh giá các luật dựa trên 3 chỉ số cốt lõi:

1. **Độ hỗ trợ (Support):** Tần suất xuất hiện đồng thời của cả $X$ và $Y$ trong toàn bộ dữ liệu:
   $$\text{Support}(X \Rightarrow Y) = P(X \cup Y) = \frac{|\{T \in \mathcal{D} \mid X \cup Y \subseteq T\}|}{N}$$

2. **Độ tin cậy (Confidence):** Xác suất có điều kiện khách hàng sẽ chọn $Y$ khi họ đã chọn $X$:
   $$\text{Confidence}(X \Rightarrow Y) = P(Y \mid X) = \frac{\text{Support}(X \cup Y)}{\text{Support}(X)}$$

3. **Độ nâng (Lift) - Chỉ số quan trọng nhất:** Đo lường mức độ tương quan thực tế giữa $X$ và $Y$ so với trường hợp chúng xuất hiện ngẫu nhiên độc lập:
   $$\text{Lift}(X \Rightarrow Y) = \frac{P(X \cup Y)}{P(X) \cdot P(Y)} = \frac{\text{Confidence}(X \Rightarrow Y)}{\text{Support}(Y)}$$

   - $\text{Lift} = 1$: $X$ và $Y$ độc lập ngẫu nhiên (không có giá trị gợi ý).
   - $\text{Lift} < 1$: $X$ và $Y$ triệt tiêu lẫn nhau (mua món này thì không mua món kia).
   - $\text{Lift} > 1$: $X$ và $Y$ có mối tương quan thuận mạnh mẽ. Trong hệ thống này, các luật chỉ được chấp nhận đưa vào Serving Feature Store khi $\mathbf{\text{Lift}} \ge \mathbf{1.2}$.

### 1.2. Cơ Chế Hoạt Động của Cây FP-Tree
Khác với thuật toán Apriori cổ điển tốn kém tài nguyên, FP-Growth nén toàn bộ cơ sở dữ liệu giao dịch thành một cấu trúc cây tiền tố nhỏ gọn (**FP-Tree**) theo 2 bước:
1. **Quét 1:** Đếm tần số xuất hiện của từng sản phẩm đơn lẻ, sắp xếp các sản phẩm có độ hỗ trợ $\ge \text{min\_support}$ theo thứ tự giảm dần.
2. **Quét 2:** Duyệt từng giao dịch, loại bỏ các mục không đủ ngưỡng, sắp xếp lại theo thứ tự tần số và chèn vào cây FP-Tree. Các nhánh có chung tiền tố sẽ dùng chung node và chỉ tăng bộ đếm tần số.
3. **Khai phá đệ quy:** Xây dựng cơ sở mẫu điều kiện (Conditional Pattern Bases) và cây FP-Tree điều kiện cho từng mục để sinh luật trực tiếp mà không cần sinh tập ứng viên.

### 1.3. Kết Quả Huấn Luyện Thực Tế
- Dữ liệu huấn luyện: `data/lakehouse/real_transactions.parquet` (805k dòng giao dịch).
- Thời gian đào luật: **2.90 giây**.
- Tổng số luật khai phá thành công: **386 luật**.
- Dữ liệu xuất: `data/models/fpgrowth_rules.json`.

---

## 2. Mô Hình Lọc Cộng Tác Mạng Nơ-ron (PyTorch NCF - Neural Collaborative Filtering)

### 2.1. Hạn Chế của Matrix Factorization Truyền Thống
Các mô hình ma trận phân rã truyền thống (SVD, PMF) xấp xỉ điểm tương tác $\hat{y}_{ui}$ giữa User $u$ và Item $i$ bằng tích vô hướng giữa 2 vector tiềm ẩn:
$$\hat{y}_{ui} = \mathbf{p}_u^T \mathbf{q}_i = \sum_{f=1}^K p_{uf} \cdot q_{if}$$

Tích vô hướng là một phép toán tuyến tính đơn giản, gán trọng số đồng đều cho từng chiều tiềm ẩn, dẫn đến việc không thể học được các mối tương quan phi tuyến tính phức tạp giữa sở thích người dùng và đặc tính món hàng.

### 2.2. Kiến Trúc Mạng NCF (Neural Matrix Factorization - NeuMF)
Hệ thống triển khai kiến trúc lai kết hợp giữa Generalized Matrix Factorization (GMF) và Multi-Layer Perceptron (MLP):

```
                       Output: Prediction Score \hat{y}_{ui} ∈ [0, 1]
                                        ▲
                                        │ (Sigmoid)
                                 [Linear Layer]
                                        ▲
                                        │
                         [Concat GMF & MLP Vectors]
                                  ▲           ▲
                    ┌─────────────┘           └─────────────┐
                    │                                       │
            [GMF Branch]                             [MLP Branch]
        Element-wise Product                    Linear(64, 32) + ReLU
        p_u^{GMF} ⊙ q_i^{GMF}                               ▲
            ▲           ▲                       Linear(128, 64) + ReLU
            │           │                                   ▲
      [User Emb]    [Item Emb]                    [Concat Embeddings]
        (32-dim)      (32-dim)                       ▲             ▲
            ▲           ▲                            │             │
            │           │                       [User Emb]     [Item Emb]
            │           │                         (32-dim)       (32-dim)
            │           │                            ▲             ▲
            └───────────┼────────────────────────────┘             │
                        └──────────────────────────────────────────┘
                         User ID: u                Item ID: i
```

1. **Nhánh GMF:** Mô hình hóa tương tác bậc một thông qua phép nhân từng phần Hadamard:
   $$\mathbf{\phi}^{GMF} = \mathbf{p}_u^{GMF} \odot \mathbf{q}_i^{GMF}$$
2. **Nhánh MLP:** Nối 2 vector nhúng và truyền qua các tầng nơ-ron dày đặc với hàm kích hoạt phi tuyến ReLU để học các biểu diễn bậc cao:
   $$\mathbf{z}_1 = [\mathbf{p}_u^{MLP}, \mathbf{q}_i^{MLP}]$$
   $$\mathbf{z}_2 = \text{ReLU}(\mathbf{W}_1^T \mathbf{z}_1 + \mathbf{b}_1)$$
   $$\mathbf{\phi}^{MLP} = \text{ReLU}(\mathbf{W}_2^T \mathbf{z}_2 + \mathbf{b}_2)$$
3. **Tầng Hội Tụ (Prediction Layer):** Ghép nối đầu ra của 2 nhánh và áp dụng hàm kích hoạt Sigmoid để dự đoán xác suất tương tác:
   $$\hat{y}_{ui} = \sigma \left( \mathbf{h}^T [\mathbf{\phi}^{GMF}, \mathbf{\phi}^{MLP}] \right)$$

### 2.3. Huấn Luyện & Checkpoint Artifacts
- **Hàm mất mát (Binary Cross-Entropy Loss):**
  $$\mathcal{L} = -\sum_{(u, i) \in \mathcal{Y} \cup \mathcal{Y}^-} \left[ y_{ui} \log \hat{y}_{ui} + (1 - y_{ui}) \log (1 - \hat{y}_{ui}) \right]$$
  Trong đó $\mathcal{Y}^-$ là các cặp tương tác âm được sinh ngẫu nhiên qua kỹ thuật Negative Sampling (tỷ lệ 4:1).
- **Trọng số mô hình:** Được lưu trữ tại `models/ncf_model.pt` kèm từ điển ánh xạ `user2idx` và `item2idx`.

---

## 3. Thuật Toán Xếp Hạng Lai (Hybrid Recommendation Ensemble)

Khi một yêu cầu gợi ý được gửi tới FastAPI Gateway (`/recommend/hybrid`), hệ thống thực thi thuật toán phối hợp 3 giai đoạn nhằm đảm bảo thời gian phản hồi **< 5ms**:

```
+---------------------------------------------------------------------------------------+
| THUẬT TOÁN HYBRID ENSEMBLE TRONG FASTAPI SERVING GATEWAY                              |
|                                                                                       |
|   1. GIAI ĐOẠN LẤY ỨNG VIÊN GIỎ HÀNG (Cart Candidates - FP-Growth):                   |
|      - Lấy danh sách item đang có trong rổ: C = [c_1, c_2, ..., c_m] từ Redis Hash    |
|      - Tra cứu song song O(1) rules cho từng item qua Redis Pipeline:                 |
|            rules:fpgrowth:{c_k} ➔ Danh sách ứng viên R_cart                           |
|      - Điểm số ứng viên giỏ hàng: S_cart(i) = max(Confidence) × min(1.0, Lift / 10)   |
|                                                                                       |
|   2. GIAI ĐOẠN LẤY ỨNG VIÊN HỒ SƠ NGƯỜI DÙNG (User Profile Candidates - NCF):        |
|      - Lấy top-K điểm số cá nhân hóa của User ID từ Redis Cache:                       |
|            ncf:user:{user_id} ➔ Danh sách ứng viên R_user                              |
|      - Nếu User chưa có trong cache (Cold-Start): Fallback về phổ biến (Popularity).  |
|                                                                                       |
|   3. GIAI ĐOẠN XẾP HẠNG HỖN HỢP CÓ TRỌNG SỐ (Weighted Blending & Ranking):           |
|      - Loại bỏ toàn bộ các món ĐÃ CÓ trong giỏ hàng hiện tại: i ∉ C                   |
|      - Tính điểm tổng hợp với hệ số ưu tiên giỏ hàng α = 0.65:                         |
|            Score_final(i) = α × S_cart(i) + (1 - α) × S_user(i)                       |
|      - Sắp xếp giảm dần theo Score_final và lấy Top-K kết quả.                         |
+---------------------------------------------------------------------------------------+
```

### Tại sao trọng số $\alpha = 0.65$ lại tối ưu?
Trong hành vi mua sắm thương mại điện tử:
- **Tín hiệu trong giỏ hàng (Cart Intent - 65%):** Đại diện cho nhu cầu cấp bách, tức thời ngay tại thời điểm hiện tại của khách hàng (ví dụ: khách vừa nhặt chiếc bình trà thì họ có nhu cầu mua kèm tách trà hoặc hộp bánh ngay trong 1 phút tới).
- **Hồ sơ dài hạn (User Profile - 35%):** Đóng vai trò như bộ lọc phong cách và phân khúc giá (ví dụ: gợi ý món mua kèm nhưng phải phù hợp với sở thích phong cách vintage hoặc mức chi tiêu quen thuộc của khách hàng đó).

---

## 4. Cơ Chế Đồng Bộ Trực Tiếp Lên Redis Feature Store

Triển khai tại file `mlops/sync_redis_store.py`.

Để đạt được thời gian phục vụ **< 5ms**, việc tính toán mô hình học máy **không được phép diễn ra trên luồng phục vụ người dùng (online request thread)**. Thay vào đó, toàn bộ kết quả được tính toán trước (precomputed) và đồng bộ nguyên tử vào Redis:

1. **Kỹ thuật Redis Pipeline theo Batch:**
   - Dữ liệu luật được gom thành từng batch 1,000 keys và đẩy qua socket mạng duy nhất, giảm 95% chi phí round-trip network latency.
2. **Khóa tra cứu $O(1)$:**
   - `rules:fpgrowth:{stock_code}` $\rightarrow$ Lưu JSON danh sách gợi ý.
   - `rules:all_items` $\rightarrow$ Redis Set chứa toàn bộ danh mục sản phẩm có luật, phục vụ kiểm tra nhanh tính sẵn sàng.
3. **Cơ chế Fallback không gián đoạn (Zero-Downtime Fallback):**
   - Nếu cụm Redis bị ngắt kết nối hoặc chưa hoàn tất đồng bộ, `HybridRecommenderService` trong FastAPI tự động chuyển sang đọc file `data/models/fpgrowth_rules.json` đã nạp sẵn trong bộ nhớ RAM của tiến trình Python, đảm bảo tỷ lệ sẵn sàng dịch vụ (Service Availability) luôn đạt **99.99%**.
