# CLO5 — Chi phí mô phỏng và đánh đổi kiến trúc

Chạy `python code/simulation/analyze_cloud_costs.py` sau benchmark CloudSim để tạo
[`cost_comparison.csv`](../output/cloudsim_benchmark/cost_comparison.csv) (9 cấu hình × quy mô)
và [`storage_lifecycle.csv`](../output/cloudsim_benchmark/storage_lifecycle.csv)
(3 tầng lưu trữ × quy mô). Script đọc trực tiếp
`storage_benchmark.csv`, `cloudsim_results.csv`, `workload_profile.csv` và đo dung
lượng file Bronze/Weather đang có trong repo. Giá VM là giá **đề bài quy định**;
chi phí compute lấy nguyên từ CloudSim, không tính lại theo một mô hình khác.

## Phạm vi và cách tính

- Mỗi kịch bản chạy **một lần**; `compute_usd_per_run` đã bao gồm VM boot 30 giây
  và quy tắc tính phí tối thiểu 60 giây từ CloudSim. Toàn bộ VM tồn tại đến khi
  tác vụ ANN hoàn thành.
- Mỗi tỉnh có một truy vấn dữ liệu đầu vào. `gcs_read_gb_per_run` = 9 ×
  `bytes_scanned_mb` của định dạng tương ứng trong `storage_benchmark.csv`.
  Đây là **thể tích I/O ước tính cho kịch bản**, vì phép đo gốc chạy Spark trên
  file local, chưa ghi log request của Cloud Storage.
- Kịch bản chính đặt bucket Cloud Storage và VM cùng vùng Singapore
  `asia-southeast1`, các VM cùng zone và truyền qua IP nội bộ. Phí truyền dữ
  liệu vùng/zone này bằng **0 USD**, dù thời gian truyền mạng vẫn nằm trong
  makespan CloudSim. [Cloud Storage không tính data transfer khi cùng location](https://cloud.google.com/storage/pricing),
  còn [VM cùng zone qua IP nội bộ không bị tính phí truyền dữ liệu](https://cloud.google.com/vpc/pricing).
  Nếu đặt VM khác zone trong cùng vùng, GCP công bố mức **0,01 USD/GiB** cho
  VM-to-VM; cột `cross_zone_io_usd_upper` là **giới hạn trên** nếu toàn bộ Gold
  output đi qua một ranh giới zone. Nó không được cộng vào tổng kịch bản chính.
- Giá lưu trữ theo đề bài: Hot Standard **0,020 USD/GB/tháng** cho Silver/Gold,
  Warm Nearline **0,010** và Cold Archive **0,0025** cho Bronze. GB là đơn vị
  thập phân (1 GB = 1.000 MB); phí chuyển VM khác zone dùng GiB theo bảng GCP.
  Bronze gồm file nguồn `tedp_raw_monthly` và `era5_cds_blh` của **toàn dự án**,
  không cộng các bản CSV Bronze đã được xử lý lại. Vì Bronze bao gồm cả 11 tỉnh,
  phí này là chi phí nền chung cho mọi cấu hình. Silver TEDP lấy kích thước
  Parquet đã đo ở từng quy mô; Silver Weather và Gold lấy byte thực ở 1x rồi
  nhân 10/50 ở quy mô giả định. Đây là **ước tính dung lượng**, không phải phép
  đo Gold/Weather thật ở 10x/50x.
- Vòng đời mẫu: Bronze ở Nearline 1 tháng, sau đó Archive 13 tháng; Silver/Gold
  ở Standard 14 tháng. 13 tháng Archive đáp ứng thời hạn giữ tối thiểu 365 ngày
  kể cả khi tính mỗi tháng 30 ngày. [Cloud Storage quy định tối thiểu Nearline
  30 ngày và Archive 365 ngày](https://cloud.google.com/storage/pricing).
  `steady_month_one_run_total_usd` = một lần compute + một tháng Hot/Archive;
  `storage_14_month_usd` là tổng lưu trữ của 14 tháng, không gồm compute.

## So sánh chi phí và hiệu năng

| Quy mô | Cấu hình | Makespan | Speedup | Hiệu suất CPU | Compute/lần (USD) | I/O đọc (GB/lần) | I/O tính phí cùng vùng/zone (USD) | Compute + lưu trữ 1 tháng ổn định, 1 lần chạy (USD) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1x | 1 · Single CSV | 418,8 s | 1,00 | 90,18% | 0,008566 | 0,63801 | 0 | 0,010029 |
| 1x | 2 · 3 VM Parquet | 215,4 s | 1,94 | 29,22% | 0,026496 | 0,00711 | 0 | 0,027959 |
| 1x | 3 · 9 VM Parquet | 121,9 s | 3,43 | 8,60% | 0,140773 | 0,00711 | 0 | 0,142236 |
| 10x | 1 · Single CSV | 3.892,1 s | 1,00 | 97,06% | 0,079590 | 6,37965 | 0 | 0,094219 |
| 10x | 2 · 3 VM Parquet | 1.873,3 s | 2,08 | 33,61% | 0,229877 | 0,07110 | 0 | 0,244506 |
| 10x | 3 · 9 VM Parquet | 944,0 s | 4,12 | 11,11% | 1,081552 | 0,07110 | 0 | 1,096181 |
| 50x | 1 · Single CSV | 19.297,7 s | 1,00 | 97,88% | 0,394537 | 31,89843 | 0 | 0,467681 |
| 50x | 2 · 3 VM Parquet | 9.243,5 s | 2,09 | 34,06% | 1,133931 | 0,35532 | 0 | 1,207075 |
| 50x | 3 · 9 VM Parquet | 4.598,4 s | 4,20 | 11,41% | 5,263555 | 0,35532 | 0 | 5,336699 |

### Chi phí lưu trữ theo vòng đời (USD)

| Quy mô | Hot Standard/tháng | Bronze Nearline tháng đầu | Bronze Archive/tháng sau chuyển tầng | Tổng lưu trữ 14 tháng |
|---|---:|---:|---:|---:|
| 1x | 0,001128 | 0,001339 | 0,000335 | 0,021484 |
| 10x | 0,011282 | 0,013388 | 0,003347 | 0,214845 |
| 50x | 0,056409 | 0,066941 | 0,016735 | 1,074226 |

Ở cả ba cấu hình, dữ liệu nền và chính sách lưu trữ giống nhau nên phần lưu
trữ không đổi thứ tự xếp hạng. Sau tháng đầu, chuyển Bronze từ Nearline sang
Archive giảm đơn giá xuống 1/4; việc chuyển tầng chỉ hợp lý nếu dữ liệu ít
được đọc lại. [Archive có phí retrieval 0,05 USD/GiB và Nearline 0,01
USD/GiB](https://cloud.google.com/storage/pricing), ngoài phí thao tác và mạng.
Benchmark chỉ đọc Silver nên chưa phát sinh retrieval của Bronze.

## Quyết định kiến trúc

Config 1 có **compute rẻ nhất** và **hiệu suất CPU cao nhất**, nhưng thời gian
hoàn thành gần gấp đôi Config 2 ở mọi quy mô; một truy vấn CSV cũng phải đọc
toàn bộ file. Config 2 giảm thời gian 1,94–2,09 lần, trong khi khối lượng đọc
I/O cho 9 truy vấn giảm khoảng 90 lần nhờ Parquet phân vùng. Đây là lựa chọn
cân bằng khi cần thời gian hoàn thành dưới mức của Config 1 và có ngân sách
compute thấp hơn Config 3.

Config 3 có **makespan ngắn nhất**, nên vẫn thích hợp nếu deadline thật sự
nghiêm ngặt. Nhưng so với Config 2, chi phí compute cao **4,65–5,31 lần**;
chi phí tăng thêm để tiết kiệm một phút tăng từ khoảng **0,0044–0,0053 USD**
(chuyển 1→2) lên **0,053–0,073 USD** (chuyển 2→3). ANN toàn cục chỉ chạy trên
một VM sau khi 9 MAP hoàn tất, khiến nhiều lõi nhàn rỗi; hiệu suất CPU của
Config 3 chỉ 8,60–11,41%. Phần VM boot 30 giây trong chi phí 1x là 0,034335
USD của Config 3 so với 0,003680 USD của Config 2. **Mức tính phí tối thiểu
60 giây không kích hoạt ở bất kỳ kịch bản nào**, vì mỗi VM tồn tại hơn 60 giây;
không nên dùng quy tắc đó làm lý do giải thích chênh lệch trong kết quả này.

Ba cấu hình tạo thành các điểm đánh đổi: Config 1 rẻ nhất, Config 3 nhanh
nhất, Config 2 ở giữa. Không có cấu hình nào tối ưu tuyệt đối nếu chưa đặt
deadline và ngân sách; đề xuất Config 2 là lựa chọn **cân bằng** với các ưu
tiên trên. Số CPU cho 10x/50x vẫn là ngoại suy từ workload 1x, còn thời gian
đọc CSV/Parquet là số đo Spark trên dữ liệu nhân bản vật lý. Bảng không phải
hóa đơn GCP: chưa đo số request Class A/B, phí chuyển tầng, retrieval khi đọc
Bronze, Internet egress, dung lượng bản sao/soft delete hoặc thuế. Những khoản
đó cần log truy cập và chính sách triển khai thực tế trước khi dự toán sản xuất.
