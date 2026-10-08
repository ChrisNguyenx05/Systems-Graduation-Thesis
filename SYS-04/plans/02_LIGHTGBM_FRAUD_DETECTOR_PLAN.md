# 💳 KẾ HOẠCH HUẤN LUYỆN: MÔ HÌNH PHÁT HIỆN GIAN LẬN LIGHTGBM (FRAUD DETECTOR)
## Detailed Plan: LightGBM / XGBoost Binary Classification Pipeline

> **Mã mô hình**: `LGBM-FRAUD-02`  
> **Thuật toán**: LightGBM (Light Gradient Boosting Machine)  
> **Nhiệm vụ**: Đánh giá vector thuộc tính giao dịch để dự đoán xác suất gian lận (`fraud_probability`) và phân nhóm hành động (`APPROVE`, `REVIEW`, `BLOCK`).

---

### 1. Yêu cầu chi tiết về Dữ liệu Đầu vào (Input Data Specifications)

#### 1.1. Bộ Dữ liệu & Nguồn gốc Thực tế
* **Bộ chuẩn kiến trúc quy hoạch**: **IEEE-CIS Fraud Detection Dataset** (tập dữ liệu giao dịch thẻ chuẩn quốc tế lớn nhất trên Kaggle).
* **Định dạng và đường dẫn file lưu trữ chính thức**:
  * Tập Train đầy đủ: [`AI Models/Dataset/LightGBM Fraud Detector/processed/train_full.parquet`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/AI%20Models/Dataset/LightGBM%20Fraud%20Detector/processed/train_full.parquet) (**590,540 dòng $\times$ 434 cột**, 80.11 MB).
  * Tập Test đánh giá: [`AI Models/Dataset/LightGBM Fraud Detector/processed/test_full.parquet`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/AI%20Models/Dataset/LightGBM%20Fraud%20Detector/processed/test_full.parquet) (**506,691 dòng $\times$ 433 cột**, 69.72 MB).
  * Script tiền xử lý gốc: [`preprocess_fraud_dataset.py`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/AI%20Models/Dataset/LightGBM%20Fraud%20Detector/preprocess_fraud_dataset.py).
* **Quy mô và phân phối lớp**:
  * Tổng số giao dịch huấn luyện: **590,540 giao dịch**.
  * Hợp lệ (`isFraud = 0`): **569,877** giao dịch (96.50%).
  * Gian lận (`isFraud = 1`): **20,663** giao dịch (3.50%).
  * Hệ số mất cân bằng: `scale_pos_weight = 569877 / 20663 \approx 27.58` (hoặc cấu hình từ 15 đến 27).

#### 1.2. Danh mục Đặc trưng (Feature Schema)
Để đảm bảo API suy luận dưới **50ms**, chúng ta trích xuất **35 đặc trưng nén đại diện (Top-35 Feature Selection)**:

| Nhóm đặc trưng | Tên cột / Ký hiệu | Ý nghĩa nghiệp vụ | Kiểu dữ liệu |
| :--- | :--- | :--- | :--- |
| **Transaction Info** | `TransactionAmt` | Giá trị tiền của giao dịch (USD) | Float64 |
| | `ProductCD` | Mã danh mục sản phẩm thanh toán | Categorical / Int |
| **Card Attributes** | `card1` - `card6` | Loại thẻ (Visa/Mastercard), hạng thẻ (Credit/Debit), ngân hàng phát hành | Int / Categorical |
| **Address & Dist** | `addr1`, `addr2`, `dist1` | Vùng địa lý, khoảng cách ước tính từ chủ thẻ | Float64 / Int |
| **Counts & Velocity** | `C1`, `C2`, `C5`, `C11`, `C13`, `C14` | Tần suất xuất hiện của thẻ/IP trong các khoảng thời gian | Float64 |
| **Time Delta** | `D1`, `D2`, `D4`, `D10`, `D15` | Khoảng cách số ngày kể từ giao dịch gần nhất | Float64 |
| **Identity / Device** | `P_emaildomain`, `R_emaildomain` | Tên miền email người mua & người nhận | Categorical / String |
| **Vesta Rich Features** | `V12`, `V13`, `V20`, `V30`, `V70`, `V80`, `V90`, `V128`, `V283`, `V307` | Các đặc trưng tổng hợp ẩn danh (PCA/Clustering) | Float64 |

#### 1.3. Nhãn Mục tiêu & Thách thức Mất cân bằng dữ liệu (Class Imbalance)
* **Cột nhãn**: `isFraud`
  * `0`: Giao dịch hợp lệ (~96.5% - 98.5%).
  * `1`: Giao dịch gian lận (~1.5% - 3.5%).
* **Chiến lược giải quyết mất cân bằng**:
  * Cài đặt trọng số lớp: `scale_pos_weight = (Số mẫu class 0) / (Số mẫu class 1)` (thường đặt từ 10 đến 25).
  * Tối ưu hóa hàm mục tiêu theo diện tích dưới đường cong: `metric = 'auc'`.

---

### 2. Pipeline Tiền xử lý & Xử lý Dữ liệu Bảng (Tabular Preprocessing)

1. **Xử lý giá trị rỗng (Missing Value Handling)**:
   * Thuộc tính số (Numerical features): Điền giá trị trung vị (Median) hoặc gắn cờ `-999` (LightGBM có khả năng tự động xử lý rỗng bản địa rất tốt).
   * Thuộc tính định danh (Categorical features): Điền `unknown`.
2. **Mã hóa (Categorical Encoding)**:
   * Áp dụng `Target Encoding` hoặc `Ordinal / Label Encoding` kết hợp khai báo tham số `categorical_feature` trong LightGBM Dataset API.
   * Lưu ánh xạ mã hóa (Category mapping dictionary) vào file `models/preprocessors/fraud_cat_encoders.joblib`.
3. **Chuẩn hóa thang đo (Scaling)**:
   * Với LightGBM (dựa trên cây quyết định), việc chuẩn hóa scale không bắt buộc cho quá trình phân nhánh cây, nhưng `TransactionAmt` có thể chuẩn hóa `Log1p(TransactionAmt)` để hạn chế skewness.

---

### 3. Thiết lập Siêu tham số & Quá trình Huấn luyện (LightGBM Hyperparameters)

```python
lgbm_params = {
    'objective': 'binary',
    'boosting_type': 'gbdt',
    'metric': 'auc',
    'learning_rate': 0.03,
    'num_leaves': 63,
    'max_depth': -1,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'min_child_samples': 20,
    'scale_pos_weight': 15.0,  # Xử lý tỷ lệ 1.5% - 3% gian lận
    'n_estimators': 1500,
    'random_state': 42,
    'n_jobs': 4
}
```

* **Chiến lược Cross-Validation**:
  * Sử dụng **Stratified 5-Fold Cross Validation** (đảm bảo tỷ lệ mẫu gian lận đồng đều trên cả 5 folds).
  * Áp dụng **Early Stopping** (dừng nếu sau 50 vòng lặp điểm AUC trên tập validation không tăng).

---

### 4. Tiêu chuẩn Đánh giá, Kỳ vọng Độ chính xác & Đối sánh Benchmark

#### 4.1. Tiêu chuẩn Gate Thăng hạng (Promotion Criteria)
Theo [`BUILD_PLAN.md`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/docs/BUILD_PLAN.md#L77):
1. **Chỉ số AUC-ROC (Area Under ROC Curve)**:
   * Phải đạt **$\text{AUC} \ge 0.94$** trên tập Test độc lập.
2. **F1-Score & Phân loại Ngưỡng Quyết định (Decision Thresholds)**:
   * Nếu $\text{Prob} < 0.30 \rightarrow$ **LOW RISK** $\rightarrow$ `APPROVE`
   * Nếu $0.30 \le \text{Prob} < 0.75 \rightarrow$ **MEDIUM RISK** $\rightarrow$ `REVIEW`
   * Nếu $\text{Prob} \ge 0.75 \rightarrow$ **HIGH RISK** $\rightarrow$ `BLOCK_TRANSACTION`
3. **Độ trễ suy luận (SLA)**:
   * Thời gian xử lý 1 bản ghi giao dịch đơn lẻ: **$< 15\text{ms}$** (cam kết SLA API $< 50\text{ms}$).

#### 4.2. Kỳ vọng Chỉ số Đo lường Thực tế (Expected Metrics)
* **AUC-ROC Score**: Kỳ vọng đạt **0.945 – 0.965 (94.5% – 96.5%)**.
* **Recall (Khả năng phát hiện trúng gian lận)**: Kỳ vọng **88.0% – 92.0%**.
* **False Positive Rate (Tỷ lệ chặn nhầm khách hàng lương thiện)**: Kiểm soát chặt chẽ **$< 1.5\%$**.

#### 4.3. Bảng Đối sánh với các Giải pháp Phân loại Gian lận Hiện có
| Thuật toán | AUC-ROC Score | Recall (Fraud) | Thời gian suy luận / TX | Đánh giá khả thi thực tế |
| :--- | :---: | :---: | :---: | :--- |
| **Logistic Regression** | 0.82 – 0.85 | ~65% | ~1ms | ❌ Quá đơn giản, bỏ lọt các thủ đoạn gian lận tinh vi. |
| **Random Forest** | 0.89 – 0.92 | ~76% | ~18ms | ⚠️ Mô hình nặng (> 200MB), tốn RAM khi phục vụ online. |
| **Deep MLP Neural Net** | 0.91 – 0.93 | ~80% | ~25ms | ❌ Kém hiệu quả hơn cây trên dữ liệu bảng, khó giải thích (Black-box). |
| **XGBoost** | 0.94 – 0.96 | ~88% | ~12ms | ⚠️ Tốt nhưng tốc độ train và nạp lại weights chậm hơn 5 lần. |
| **LightGBM (Mô hình SYS-04)** | **0.945 – 0.965** | **88% – 92%** | **< 8ms** | ✅ **Lựa chọn số 1**: Histogram-based chia nhánh siêu tốc, footprint nhẹ (< 15MB). |

---

### 5. Đóng gói Mô hình & Tích hợp MLflow

* **Xuất Artifact**:
  * Lưu mô hình dạng file nhị phân Booster text của LightGBM:
    ```python
    booster.save_model("models/production/lgbm_fraud_model.txt")
    ```
  * Hoặc lưu qua `joblib`: `models/production/lgbm_fraud_model.joblib`.
  * Lưu danh sách 35 tên đặc trưng theo đúng thứ tự: `models/preprocessors/fraud_feature_names.json`.
* **Đăng ký MLflow**:
  * Experiment: `Fraud_Detection_IEEE_CIS`
  * Model Name: `Fraud_Detection_LightGBM`
  * Stage: `Production`

---

### 6. Cơ chế Huấn luyện Gia tăng Cây Quyết định (Continual Tree Boosting với init_model)

Khi có tập giao dịch mới được xác nhận nhãn (ví dụ sau 1 tuần đối soát giao dịch ngân hàng / payment stream):
* LightGBM cho phép **học thêm các cây mới trên phần dư lỗi (Residual Errors)** mà không phải xây lại từ đầu toàn bộ rừng cây:

```python
import lightgbm as lgb

def incremental_train_fraud(new_tx_df, base_model_path="models/production/lgbm_fraud_model.txt"):
    # 1. Nạp booster đã có
    existing_booster = lgb.Booster(model_file=base_model_path)
    
    # 2. Tạo Dataset từ dữ liệu mới
    new_train_data = lgb.Dataset(
        new_tx_df[feature_names], 
        label=new_tx_df['isFraud'], 
        free_raw_data=False
    )
    
    # 3. Trồng thêm 50 cây quyết định tập trung sửa các mẫu bị phân loại sai gần đây
    updated_booster = lgb.train(
        params,
        new_train_data,
        num_boost_round=50,
        init_model=existing_booster,  # Kế thừa toàn bộ tri thức cũ
        valid_sets=[new_train_data]
    )
    
    # 4. Lưu lại booster mới
    updated_booster.save_model("models/production/lgbm_fraud_model_v2.txt")
    return updated_booster
```

* **Lợi ích**:
  * Phát hiện được ngay các hình thức giả mạo IP/thẻ mới.
  * Tốc độ thực thi: Chỉ mất **1 – 3 giây**.
