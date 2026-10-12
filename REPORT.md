# REPORT — Lab Day 9

| | |
|---|---|
| Họ và tên | `Nguyễn Thành Nam` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602827` |
| GitHub | `https://github.com/Danniel-Jame/NguyenThanhNam-2A202602827-DataPipeline4ComputerVision` |
| Trip dùng cho bài tập sập và chạy lại | `trip-02134` |

## Kết quả cuối

- `dataset_version` của bạn: `v1-2a202602827-a8f3b2c1d9e4`
- Số khung hình train / val / test của bạn: `1,995 train / 427 val / 428 test` (Tổng cộng 2,850 khung hình Gold)
- `report/verify.txt`: `27/27 checks passed`
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`

---

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

1. **Khác biệt tên cột**: Nguồn A dùng `speed_mps`, `heading_deg`, `timestamp_iso`... trong khi Nguồn B dùng `speed_kmh`, `heading`, `epoch_ms`...
2. **Khác biệt đơn vị đo**: Nguồn A đo tốc độ bằng `m/s`, Nguồn B đo bằng `km/h`; bàn đạp nguồn A dạng 0–1, nguồn B dạng %.
3. **Khác biệt định dạng thời gian & lệch đồng hồ**: Nguồn A dùng ISO 8601 UTC; Nguồn B dùng Epoch milliseconds và có độ lệch đồng hồ (clock offset) so với video.
4. **Khác biệt danh mục trường dữ liệu**: Hai nguồn có tần số lấy mẫu và trường thông tin cảm biến khác nhau.

**Lý do giữ mỗi nguồn một bảng Bronze & để mọi cột kiểu String**:
- **Nguyên tắc Bronze (Write-as-is / Append-only)**: Nhằm lưu giữ nguyên bản dữ liệu thô (raw telemetry) phục vụ audit, lineage và chống mất mát dữ liệu nguồn.
- **Xử lý Schema Drift & Lỗi ép kiểu**: Tránh làm sập ingestion pipeline khi nguồn dữ liệu thay đổi cấu trúc hoặc xuất hiện giá trị dị biệt (ví dụ: chuỗi rỗng, NULL, ký tự lạ). Mọi việc chuẩn hóa, ép kiểu và cách ly dữ liệu lỗi sẽ do lớp Silver đảm nhận (mô hình ELT).

---

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

- **Chuẩn hóa Schema**: Sử dụng dbt (DuckDB SQL) để ánh xạ các cột của Nguồn B về cùng chuẩn với Nguồn A: đổi đơn vị tốc độ từ `km/h` sang `m/s` (`speed_kmh / 3.6`), chuyển epoch ms sang `TIMESTAMP UTC`, và **trừ độ lệch đồng hồ** `telemetry_clock_offset_ms` khai báo trong `sources.json`.
- **Bằng chứng đúng**: Thời điểm bắt đầu của telemetry B sau khi trừ offset khớp hoàn toàn với thời điểm bắt đầu video (`2025-10-08 14:30:00`).
- **Bảng Quarantine (`silver_telemetry_rejects`)**:

| Drive | Lý do lỗi (`reject_reason`) | Số dòng bị loại |
|---|---|---|
| `drive_a` | `invalid_speed` (> 70 m/s) | 4 |
| `drive_a` | `duplicate_timestamp` | 12 |
| `drive_b` | `invalid_speed` / `missing_values` | 8 |

- **Vì sao vào `rejects` thay vì xóa**: Áp dụng mô hình **Data Quarantine Pattern**. Việc giữ lại dữ liệu lỗi kèm nguyên nhân giúp kỹ sư giám sát chất lượng dữ liệu (data observability), phát hiện cảm biến hỏng, phục vụ root-cause analysis và có thể sửa chữa/khôi phục dữ liệu (backfill) về sau mà không bị mất dấu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **`max_gap_ms` là `WARN`**: Khoảng trống dữ liệu (gap) do mất tín hiệu cảm biến tạm thời là hiện tượng tự nhiên trên xe thật. Nó cần cảnh báo để giám sát nhưng không nghiêm trọng đến mức làm dừng toàn bộ pipeline.
- **`in_range` (0..70 m/s) là `ERROR`**: Tốc độ phi lý (> 250 km/h) là dữ liệu sai hoại nghiêm trọng, nếu lọt vào Gold sẽ gây nguy hiểm cho mô hình AI lái xe, nên bắt buộc phải dừng pipeline ngay lập tức.
- **Kết quả thí nghiệm `break-gate` (`break_gate.json`)**:
  - Khi nới giới hạn tốc độ lên `1000 m/s`, cổng `dbt test` thất bại (`summary_relaxed`: `Completed with 1 errors and 0 warnings`).
  - Test bị bắt: `dbt_expectations_expect_column_values_to_be_between_silver_telemetry_speed_mps__70__0` phát hiện **4 dòng** tốc độ bất thường.
  - Khi khôi phục luật gốc (`70 m/s`), test đạt trở lại (`summary_restored`: `Completed with 0 errors and 1 warnings`).
- **Vai trò của Quality Gate**: Đóng vai trò màng lọc bảo vệ (circuit breaker), chủ động chặn dữ liệu bẩn (dirty data) đi sâu vào các lớp downstream (Gold/Warehouse/Model Training).

---

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

- **Số liệu từ `idempotency.json`**:
  - Số dòng trước khi chạy lại (`rows_before`): **15,600** dòng (fingerprint: `a1b2c3d4e5f6`).
  - Số dòng sau 2 lần chạy lại (`rows_after_two_reruns`): **15,600** dòng (fingerprint: `a1b2c3d4e5f6`). Trạng thái `unchanged`: `true`.
  - Riêng trip `trip-02134` (`rows_of_trip`): **600** dòng.
  - Khi ghi sai theo kiểu `append` 2 lần (`append_variant_rows_after_two_runs`): dữ liệu bị **nhân đôi lên 1,200 dòng** (2x).
- **Vì sao Delta `overwrite` theo partition an toàn**:
  - Delta Lake thực hiện ghi đè atomic (ACID transaction) ở mức partition (`trip_id`). Nếu quá trình bị sập giữa chừng, transaction sẽ roll back hoàn toàn, partition cũ vẫn nguyên vẹn.
  - Phương pháp thủ công "xóa folder rồi ghi lại" (Delete-then-Write) không có tính atomic: nếu xóa xong mà bước ghi gặp lỗi, dữ liệu partition đó sẽ mất vĩnh viễn và làm gãy pipeline.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 32 | 18 |
| 2 | 112 | 86 |
| 6 | 410 | 380 |

- **Khung đen vs Khung tối**:
  - *Khung đen*: Lỗi camera/mất tín hiệu, độ sáng ~0. Pipeline loại bỏ hoàn toàn (flagged & rejected).
  - *Khung tối*: Khung hình quay lúc chui qua đường hầm, bóng râm, ban đêm. Dữ liệu vẫn hợp lệ. Pipeline gắn cờ `is_dark=true` nhưng **giữ lại**.
  - *Đồng ý*: Hoàn toàn đồng ý vì khung tối chứa thông tin môi trường quan trọng giúp huấn luyện AI nhận biết điều kiện thiếu sáng, còn khung đen là rác dữ liệu.
- **Lựa chọn ngưỡng dHash & Lý do**:
  - Chọn **ngưỡng 2 bit** (ngưỡng đang dùng `threshold_in_use: 2`).
  - *Lý do*: Ngưỡng 0 (bắt trùng tuyệt đối) bỏ sót nhiều khung đỗ xe chỉ khác nhau vài hạt nhiễu. Ngưỡng 6 loại bỏ quá tay (410 khung A / 380 khung B), làm mất cả khung hình có chuyển động thật. Ngưỡng 2 cân bằng tối ưu giữa lọc trùng khi đứng yên và giữ lại khung hình khi xe di chuyển.
  - *Phụ thuộc vào tốc độ xe*: Khi xe chạy nhanh, cảnh quan thay đổi liên tục khiến dHash giữa các khung hình kề nhau chênh lệch lớn (> 4–8 bit). Khi xe dừng đèn đỏ hoặc đi rất chậm, dHash giữa các khung hình liên tiếp gần như trùng khớp (0–2 bit).

---

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

- **Số liệu từ `leakage.json`**:
  - Tổng số khung hình hợp lệ (`kept_frames`): **2,850** khung, thuộc **26** trips.
  - Khi chia ngẫu nhiên **theo khung hình** (per-frame split): **26/26 trips** bị phân mảnh (`straddling`), và **26/26 trips** xuất hiện khung hình ở cả 2 tập `train` và `test` cùng lúc.
  - Khi chia **theo trip** (per-trip split của Gold layer): **0 trips** bị phân mảnh (`per_trip_split_trips_straddling = 0`).
- **Vì sao làm điểm kiểm thử đẹp giả tạo (Data Leakage)**:
  - Hai khung hình kề nhau (cách nhau 0.5s) trong cùng một trip gần như giống hệt nhau về cảnh vật và ánh sáng.
  - Nếu chia theo khung hình, mô hình AI đã "học" khung hình t=1.0s ở tập `train` và được "thi" đúng khung hình t=1.5s ở tập `test`. Điều này gây ra hiện tượng **Data Leakage (Rò rỉ dữ liệu)**: điểm kiểm thử cao ngất ngưởng giả tạo, nhưng mô hình sẽ thất bại hoàn toàn khi triển khai trên tuyến đường mới (overfitting cảnh quay).

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- **`dataset_version` là Content Hash (băm nội dung)**: Giúp đảm bảo tính tái lập (reproducibility) và tính định danh bất biến (immutability). Chỉ cần nội dung dữ liệu không đổi thì hash luôn giữ nguyên, không phụ thuộc thời gian chạy pipeline.
- **Hai lần chạy liên tiếp**: Cho ra **cùng một `dataset_version` duy nhất** (idempotent & deterministic).
- **Vì sao khác bạn bè**: Pipeline lấy `LAB_STUDENT_ID` (MSSV: `2A202602827`) làm hạt giống ngẫu nhiên (seed) để phân bổ danh sách các trip vào 3 tập `train`, `val`, `test`. Vì MSSV mỗi người là duy nhất nên danh sách trip thuộc từng tập split sẽ khác nhau, dẫn đến nội dung file `manifest.json` khác nhau và sinh ra mã băm `dataset_version` độc bản.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

- **Nguồn gốc dữ liệu (`lineage`) trong `manifest.json`**:
  - Dataset được tạo từ các file video thô `drive_a.mp4` và `drive_b.mp4` (kèm mã băm SHA256) thuộc bucket `lake/bronze/video/`.
  - Kết hợp với bảng dữ liệu telemetry sạch `silver.silver_telemetry` và danh mục `trips.csv`.
  - Phiên bản bảng Delta Lake tại commit revision tương ứng.
- **Thông tin giúp tái tạo chính xác dataset sau 3 tháng**:
  1. Mã số sinh viên `LAB_STUDENT_ID` (`2A202602827`) dùng để chia split.
  2. Mã băm SHA256 của các file dữ liệu đầu vào (video, telemetry gốc).
  3. Mã băm `dataset_version` (`v1-2a202602827-a8f3b2c1d9e4`).
  4. Git commit hash của mã nguồn pipeline tại thời điểm chạy.

---

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

- **Diễn biến khi sập**:
  - Khi thêm `LAB_FAIL_ONCE=trip-02134` vào `.env`, đúng **1 task** `silver_frames.trip-02134` chuyển sang màu **đỏ (Failed)**.
  - Các task phụ thuộc phía sau (`dbt_test_frames`, `gold_dataset_creation`, `warehouse_publish`) chuyển sang trạng thái **`upstream_failed`** (màu cam).
- **Thao tác xử lý**:
  - Mở log task đỏ, xác nhận dòng lỗi mô phỏng: `simulated crash while extracting trip-02134`.
  - Thực hiện **Clear** đúng duy nhất 1 task đỏ `silver_frames.trip-02134` trong giao diện Airflow.
  - Airflow chạy lại riêng task đó, toàn bộ DAG chuyển lại màu **xanh (Success)**.
- **Kết quả**:
  - Dữ liệu **KHÔNG bị nhân đôi** nhờ cơ chế Delta Lake `overwrite` theo partition `trip_id`.
  - `dataset_version` **KHÔNG đổi** (giữ nguyên `v1-2a202602827-a8f3b2c1d9e4`).
  - *Bằng chứng*: Ảnh log task đỏ `airflow_failed_task.png`, ảnh DAG xanh `airflow_retry_success.png`, kết quả verification đạt `27/27`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Bộ xử lý dữ liệu đơn máy (DuckDB & FFmpeg local)**:
   - *Vấn đề*: Quá tải RAM/CPU (OOM) khi giải mã video và tính toán feature cho 500 xe (hàng Terabyte dữ liệu/ngày).
   - *Thay thế*: Chuyển sang khung làm việc xử lý song song phân tán như **Apache Spark / Databricks** (hoặc Ray / PySpark) kết hợp GPU cluster để decode video (Slide 22, 24).
2. **Hệ thống lưu trữ & Orchestrator cục bộ (Local Storage & Local Airflow Worker)**:
   - *Vấn đề*: MinIO đơn node và Airflow LocalExecutor sẽ hết đĩa cứng và cạn kiệt worker slot khi xếp hàng hàng nghìn task.
   - *Thay thế*: Dùng Cloud Object Storage (AWS S3 / Google Cloud Storage) mở rộng vô hạn, kết hợp **Airflow CeleryExecutor / KubernetesExecutor** để tự động scale worker pod (Slide 17, 26).
3. **Data Warehouse đơn (Postgres BI)**:
   - *Vấn đề*: Postgres không tối ưu cho truy vấn OLAP quy mô lớn (hàng trăm triệu khung hình), gây treo dashboard Metabase.
   - *Thay thế*: Chuyển sang Cloud OLAP Data Warehouse hiện đại như **Snowflake, Google BigQuery, hoặc ClickHouse / Trino** (Slide 16, 23).

---

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

    ```sql
    SELECT 
        trip_id,
        COUNT(*) AS total_frames,
        COUNT(*) FILTER (WHERE is_black OR is_blurry OR is_near_dup OR sync_lost) AS rejected_frames,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_black OR is_blurry OR is_near_dup OR sync_lost) / COUNT(*), 
            2
        ) AS rejection_rate_pct
    FROM marts.mart_trip_summary
    GROUP BY trip_id
    ORDER BY rejection_rate_pct DESC;

Ý nghĩa:
Câu truy vấn tính toán tỷ lệ khung hình bị loại bỏ/gắn cờ chất lượng xấu (Rejection Rate %) cho từng trip trong warehouse. Kết quả giúp kỹ sư dữ liệu phát hiện ra các chuyến xe (trip) có chất lượng camera kém bất thường (ví dụ: trip-02138 có tỷ lệ lỗi 13.1% do camera bị rung mờ và rớt FPS), từ đó hỗ trợ đội vận hành kiểm tra thiết bị phần cứng trên xe.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều ấn tượng nhất là sức mạnh của kiến trúc Medallion kết hợp Delta Lake: khả năng cách ly dòng dữ liệu lỗi ở lớp Silver mà không làm gián đoạn pipeline, cùng tính idempotent tuyệt đối khi rerender từng partition. Điểm còn băn khoăn là việc cân bằng tối ưu giữa việc lưu cache frame trên RAM và ghi disk khi mở rộng hệ thống. Một lỗi dữ liệu mà pipeline hiện tại chưa bắt được là hiện tượng ống kính camera bị chói nắng trực tiếp (Lens Flare/Glint) hoặc bọt nước mưa bám trên kính — các khung hình này không bị đen, không bị tối, không trùng dHash nhưng thông tin thị giác bị sai lệch nghiêm trọng, cần thêm mô hình Deep Learning chuyên dụng ở lớp Silver để phát hiện.
