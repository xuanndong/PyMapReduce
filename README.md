# PyMapReduce: Tài Liệu Kỹ Thuật Toàn Diện & Kiến Trúc Hệ Thống Điện Toán Phân Tán

<div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin-bottom: 24px;">
  <p style="margin: 0; font-size: 16px; line-height: 1.6; color: #334155;">
    <strong>PyMapReduce</strong> là hệ thống điện toán phân tán thế hệ mới kết hợp giữa lõi xử lý bất đồng bộ viết bằng ngôn ngữ <strong>Rust</strong> và giao diện lập trình cấp cao trên <strong>Python</strong>. Nền tảng hiện đại hóa mô hình tính toán <strong>MapReduce</strong> kinh điển, mở rộng sang kiến trúc <strong>Stateful Actors</strong> (diễn viên phân tán lưu trạng thái), <strong>Tiered Zero-Copy Shared Memory</strong> (bộ nhớ chia sẻ đa tầng), và <strong>Distributed Dataset API</strong> để phục vụ các bài toán xử lý dữ liệu lớn (Big Data), tính toán song song đa tiến trình (Data Parallelism) và các mô hình Trí tuệ Nhân tạo / Học máy (AI/ML) với chi phí phụ tải tối thiểu (Zero-cost Abstractions) và khả năng chịu lỗi toàn diện.
  </p>
</div>

---

## MỤC LỤC TÀI LIỆU

1. [Tư Duy Lập Trình MapReduce Cốt Lõi & Kiến Trúc Tổng Thể](#1-tư-duy-lập-trình-mapreduce-cốt-lõi--kiến-trúc-tổng-thể)
2. [Hai Mô Hình Tính Toán: Stateless Tasks vs Stateful Actors](#2-hai-mô-hình-tính-toán-stateless-tasks-vs-stateful-actors)
3. [Chi Tiết 5 Chiến Lược Lập Lịch Phân Tán (Scheduling Strategies)](#3-chi-tiết-5-chiến-lược-lập-lịch-phân-tán-scheduling-strategies)
4. [Cấu Trúc Hàng Đợi Ưu Tiên & Mailbox (TaskQueue Architecture)](#4-cấu-trúc-hàng-đợi-ưu-tiên--mailbox-taskqueue-architecture)
5. [Hệ Thống Write-Ahead Log (WAL) & Phục Hồi Thảm Họa](#5-hệ-thống-write-ahead-log-wal--phục-hồi-thảm-họa)
6. [Global Control Store (GCS) & Chính Sách Dọn Dẹp Hybrid](#6-global-control-store-gcs--chính-sách-dọn-dẹp-hybrid)
7. [Bộ Lưu Trữ Đa Tầng (Tiered Object Store) & Zero-Copy](#7-bộ-lưu-trữ-đa-tầng-tiered-object-store--zero-copy)
8. [Khả Năng Chịu Lỗi Mạng, Re-attachment & Giám Sát Sức Khỏe](#8-khả-năng-chịu-lỗi-mạng-re-attachment--giám-sát-sức-khỏe)
9. [Cơ Chế Hủy 2 Giai Đoạn, Cô Lập Tiến Trình & Xử Lý 7 Điểm Mù Hệ Thống](#9-cơ-chế-hủy-2-giai-đoạn-cô-lập-tiến-trình--xử-lý-7-điểm-mù-hệ-thống)
10. [Chuẩn Hóa Ma Trận Timeout Hệ Thống](#10-chuẩn-hóa-ma-trận-timeout-hệ-thống)
11. [Mô Hình Lập Trình Python & Distributed Dataset API](#11-mô-hình-lập-trình-python--distributed-dataset-api)
12. [Hướng Dẫn Vận Hành CLI & Triển Khai Docker](#12-hướng-dẫn-vận-hành-cli--triển-khai-docker)
    - [12.1. Cài Đặt Gói Thư Viện](#121-cài-đặt-gói-thư-viện)
    - [12.2. Tra Cứu Toàn Diện Các Cờ Dòng Lệnh CLI](#122-tra-cứu-toàn-diện-các-cờ-dòng-lệnh-cli-cli-arguments-reference)
    - [12.3. Các Kịch Bản Vận Hành Mẫu](#123-các-kịch-bản-vận-hành-mẫu-execution-recipes)
    - [12.4. Cơ Chế Auto-Scaling Cụm Máy Vật Lý (Warm Standby Architecture)](#124-cơ-chế-auto-scaling-cụm-máy-vật-lý-warm-standby-architecture)
    - [12.5. Triển Khai Cụm Bằng Docker Compose](#125-triển-khai-cụm-bằng-docker-compose)
13. [Bảng Tra Cứu Mã Nguồn Toàn Hệ Thống](#13-bảng-tra-cứu-mã-nguồn-toàn-hệ-thống)

---

## 1. TƯ DUY LẬP TRÌNH MAPREDUCE CỐT LÕI & KIẾN TRÚC TỔNG THỂ

### 1.1. Bản Chất Triết Lý MapReduce Trong Hệ Thống
Mô hình MapReduce chia nhỏ bài toán tính toán khối lượng lớn thành 3 giai đoạn nền tảng:

```text
+-------------------+       +-----------------------+       +---------------------+
|    MAP PHASE      |  ==>  |   SHUFFLE & ROUTE     |  ==>  |    REDUCE PHASE     |
| (Phân vùng song   |       | (Định tuyến & chuyển  |       | (Tổng hợp & gom     |
|  song độc lập)    |       |  giao dữ liệu P2P)    |       |  nhóm kết quả)      |
+-------------------+       +-----------------------+       +---------------------+
```

1. **Map Phase (Phân vùng & Chuyển đổi Song song):**
   * Tập dữ liệu lớn được chia thành các phân vùng độc lập (Partitions / Shards).
   * Mỗi Worker thực thi hàm `Map` trên phân vùng cục bộ của mình mà **hoàn toàn không cần đồng bộ hay chờ đợi** các Worker khác.
   * Kết quả trung gian của Map được ghi thẳng vào Hot-Tier RAM Shared Memory (`/dev/shm`) dưới dạng một `ObjectRef` duy nhất.
2. **Shuffle & Route Phase (Định tuyến & Chuyển giao Ngang hàng):**
   * Các tác vụ Reduce cần dữ liệu đầu ra từ nhiều Map tasks trước đó sẽ khai báo danh sách `dependencies: Vec<ObjectId>`.
   * Thay vì chuyển toàn bộ dữ liệu dồn về HeadNode làm nghẽn cổ chai băng thông mạng, Worker thực thi Reduce chủ động kéo dữ liệu trực tiếp từ các Worker lưu trữ qua giao thức ngang hàng (**Peer-to-Peer Object Transfer**).
3. **Reduce Phase (Tổng hợp & Gom nhóm Dữ liệu):**
   * Khi thu thập đủ các `dependencies`, hàm `Reduce` tổng hợp các mảng dữ liệu (ví dụ: gộp DataFrame, tính tổng, huấn luyện gradient) và trả về kết quả cuối cùng cho Driver.

### 1.2. Tại Sao Lại Là Kiến Trúc Lai Rust & Python?
* **Python:** Cung cấp hệ sinh thái khoa học dữ liệu và AI/ML hàng đầu thế giới (PyTorch, TensorFlow, Pandas, NumPy, Scikit-learn, PyArrow). Tuy nhiên, Python bị giới hạn nghiêm trọng bởi cơ chế **Global Interpreter Lock (GIL)** và tốn nhiều phụ tải cho việc điều phối mạng đa luồng.
* **Rust (Tokio Asynchronous Engine):** Đóng vai trò là Mặt phẳng Điều khiển (Control Plane). Rust quản lý hàng nghìn kết nối mạng đồng thời không cần khóa (Lock-free), lập lịch hàng đợi Min-Heap, giám sát nhịp tim và nhật ký phục hồi với độ trễ micro-giây và mức chiếm dụng RAM chỉ vài chục MB.
* **Cầu Nối PyO3 FFI & IPC Process Group:** Rust spawn và quản lý Pool tiến trình con Python (`daemon.py`), giao tiếp qua IPC pipe và chia sẻ bộ nhớ `/dev/shm` đạt tốc độ Zero-copy.

---

### 1.3. Sơ Đồ Kiến Trúc Hệ Thống Toàn Diện (System Architecture Blueprint)

```text
+--------------------------------------------------------------------------------------------------------------------+
|                                    TẦNG 3: DRIVER LAYER (Client / User Application)                                |
|                                                                                                                    |
|   +---------------------------------------+  +-----------------------------------+  +--------------------------+   |
|   |          User Python Script           |  |       Distributed Dataset API     |  |       Driver Handle      |   |
|   |  - app.py / ML Training Pipelines     |  |  - read_csv / from_pandas         |  |  - RAII Drop Guard       |   |
|   |  - @pymapreduce.remote (Fn & Actor)   |  |  - map / filter / repartition     |  |  - Auto-Reconnect (30s)  |   |
|   |  - pymapreduce.kill(actor)            |  |  - reduce / to_pandas             |  |  - get_job_status()      |   |
|   +---------------------------------------+  +-----------------------------------+  +--------------------------+   |
+--------------------------------------------------------------------------------------------------------------------+
                                                         |
                                                         | Framed TCP (4-byte Length-Delimited + Bincode)
                                                         v
+--------------------------------------------------------------------------------------------------------------------+
|                                 TẦNG 1: HEAD NODE (Cluster Master & Coordinator Engine)                            |
|                                                                                                                    |
|   +-----------------------------------+  +------------------------------------+  +-----------------------------+   |
|   |    Global Control Store (GCS)     |  |       Dispatcher & TaskQueue       |  |    Write-Ahead Log (WAL)    |   |
|   |  - state.jobs & state.nodes       |  |  - Min-Heap Priority (0 vs 128)    |  |  - append() & sync_data()   |   |
|   |  - state.active_tasks & actors    |  |  - Locality & Actor Mailboxes      |  |  - 5-min Checkpoint (bin)   |   |
|   |  - Hybrid Retention (2000 / 3600s)|  |  - 5 Strategies (Adaptive, Least..)|  |  - wal.truncate() & replay  |   |
|   +-----------------------------------+  +------------------------------------+  +-----------------------------+   |
|                                     ^                      ^                                    ^                  |
|                                     |                      |                                    |                  |
|   +---------------------------------+----------------------+------------------------------------+--------------+   |
|   |                          HealthMonitor & Circuit Breaker (Heartbeat 30s / Priority 0 Re-queue)            |   |
|   +-----------------------------------------------------------------------------------------------------------+   |
+--------------------------------------------------------------------------------------------------------------------+
                                                         |
                                                         | Framed TCP Messages (AssignTask / CancelTask / Heartbeat)
                                                         v
+--------------------------------------------------------------------------------------------------------------------+
|                                      TẦNG 2: WORKER NODES (Compute & Storage Layer)                                |
|                                                                                                                    |
|  +----------------------------------------------------+    +----------------------------------------------------+  |
|  |                   WORKER NODE 1                    |    |                   WORKER NODE 2                    |  |
|  |                                                    |    |                                                    |  |
|  |  +----------------------------------------------+  |    |  +----------------------------------------------+  |  |
|  |  | Rust Worker Engine (Tokio Concurrency Limiter)|  |    |  | Rust Worker Engine (Tokio Concurrency Limiter)|  |  |
|  |  +----------------------------------------------+  |    |  +----------------------------------------------+  |  |
|  |  | Tiered ObjectStore (Hot /dev/shm + Warm Mmap)|  |    |  | Tiered ObjectStore (Hot /dev/shm + Warm Mmap)|  |  |
|  |  +----------------------------------------------+  |    |  +----------------------------------------------+  |  |
|  |  | Python Daemon Pool (setpgid / SIGTERM/SIGKILL|  |    |  | Python Daemon Pool (setpgid / SIGTERM/SIGKILL|  |  |
|  |  | _ACTORS Map / CUDA empty_cache / malloc_trim)|  |    |  | _ACTORS Map / CUDA empty_cache / malloc_trim)|  |  |
|  |  +----------------------------------------------+  |    |  +----------------------------------------------+  |  |
|  |  | Docker Init (PID 1 tini Zombie Reaper)       |  |    |  | Docker Init (PID 1 tini Zombie Reaper)       |  |  |
|  |  +----------------------------------------------+  |    |  +----------------------------------------------+  |  |
|  +----------------------------------------------------+    +----------------------------------------------------+  |
|                            ^                                                         ^                             |
|                            +======== P2P Direct Fetch (GetObjectWorker) =============+                             |
+--------------------------------------------------------------------------------------------------------------------+
```

---

## 2. HAI MÔ HÌNH TÍNH TOÁN: STATELESS TASKS VS STATEFUL ACTORS

PyMapReduce cung cấp hai mô hình điện toán bổ trợ lẫn nhau, giải quyết triệt để từ các bài toán xử lý dữ liệu hàng loạt (Batch ETL) cho đến các bài toán AI/ML đòi hỏi duy trì trạng thái dài hạn.

<table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
  <thead>
    <tr style="background: #f1f5f9; text-align: left;">
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Đặc Điểm So Sánh</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Stateless Tasks (Tác Vụ Không Trạng Thái)</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Stateful Actors (Diễn Viên Có Trạng Thái)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>Enum Định Danh</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>TaskKind::Map</code>, <code>TaskKind::Reduce</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>TaskKind::ActorCreation</code>, <code>TaskKind::ActorTask</code>, <code>TaskKind::ActorDestruction</code></td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>Vòng Đời (Lifecycle)</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Ngắn hạn. Chạy một lần duy nhất với input $\rightarrow$ trả về output rồi giải phóng bộ nhớ.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Dài hạn (Long-lived). Instance của class được duy trì liên tục trong RAM của Worker qua nhiều lời gọi method.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>Trạng Thái Bộ Nhớ</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Phi trạng thái (Pure function). Không lưu biến nội tại giữa các lần chạy.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Lưu trạng thái (Stateful). Giữ trọng số Model AI, bộ đệm cache, hoặc kết nối DB trong <code>self</code>.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>Cơ Chế Điều Phối</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Phân tán tự do ra bất kỳ Worker nào theo thuật toán lập lịch chung.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Định tuyến tuần tự qua <strong>Actor Mailbox</strong> đến chính xác Worker đang lưu giữ instance.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>Khả Năng Phục Hồi Lỗi</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Tự động retry tối đa 3 lần và re-queue sang Worker khác khi Worker bị sập.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Tự động <strong>Lazy Re-creation</strong> sang Worker mới dựa trên <code>creation_payload</code> lưu trong GCS.</td>
    </tr>
  </tbody>
</table>

### 2.1. Chi Tiết Vòng Đời Của Stateful Actor & Quản Lý Bộ Nhớ Daemon

```text
+----------------------------------------------------------------------------------------------------+
|                                 TẠO ACTOR: MyClass.remote(*args)                                   |
|  - Driver đóng gói payload khởi tạo gửi lên HeadNode                                               |
|  - HeadNode lưu ActorRecord (trạng thái: Creating) vào GCS và giao cho Worker                      |
|  - Worker Daemon nạp class, tạo instance _ACTORS[actor_id] -> Cập nhật trạng thái GCS: Alive       |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                             GỌI METHOD: actor.my_method.remote(*args)                              |
|  - Đẩy task vào actor_mailboxes[actor_id] trong TaskQueue                                          |
|  - Dispatcher định tuyến TUẦN TỰ (FIFO) về đúng Worker đang lưu trữ instance                       |
+----------------------------------------------------------------------------------------------------+
                                                  |
                         +------------------------+------------------------+
                         |                                                 |
            (Sự Cố Crash / Hết Idle Timeout)                     (Người Dùng Gọi Hủy Tường Minh)
                         |                                                 |
                         v                                                 v
+--------------------------------------------------+     +------------------------------------------+
|          TỰ ĐỘNG PHỤC HỒI (Lazy Re-creation)     |     |       HỦY ACTOR (ActorDestruction)       |
|  - HeadNode kích hoạt bản sao creation_payload   |     |  - Worker gọi close/destroy/cleanup      |
|    lưu trong GCS                                 |     |  - torch.cuda.empty_cache() (GPU RAM)    |
|  - Tự động nạp lại Actor trên Worker mới ngay    |     |  - libc.malloc_trim(0) (Hệ điều hành)    |
|    khi có lời gọi method tiếp theo               |     |  - GCS đánh dấu trạng thái: Dead         |
+--------------------------------------------------+     +------------------------------------------+
```

1. **Khởi Tạo (`ActorCreation`):** Khi người dùng gọi `actor = await MyClass.remote(*args)`, Driver đóng gói payload khởi tạo gửi lên HeadNode. HeadNode lưu `ActorRecord` với trạng thái `Creating` vào GCS và giao cho một Worker. Worker nạp class và khởi tạo instance trong `_ACTORS[actor_id]` rồi chuyển trạng thái thành `Alive`.
2. **Gọi Method (`ActorTask`):** Khi gọi `await actor.my_method.remote(*args)`, task được đẩy vào `actor_mailboxes[actor_id]` trong `TaskQueue`. Dispatcher luôn định tuyến task này đến đúng Worker đang giữ instance, thực thi tuần tự (FIFO) để tránh tranh chấp trạng thái.
3. **Tự Động Phục Hồi (Lazy Re-creation):** Nếu Worker chứa Actor bị dừng do hết `idle_timeout` hoặc bị crash:
   * Bản sao `creation_payload` trong GCS tại [src/gcs/state.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/gcs/state.rs) được kích hoạt để tự động khởi tạo lại Actor instance trên một Worker mới khi có lời gọi method tiếp theo.
4. **Hủy Rõ Ràng (`ActorDestruction`):** Khi người dùng gọi `await pymapreduce.kill(actor)` hoặc `await actor.destroy.remote()`, Worker gọi các lifecycle hooks (`close`, `destroy`, `cleanup`), giải phóng GPU CUDA cache và dọn sạch RAM. GCS đánh dấu trạng thái `Dead` (hỗ trợ Idempotency nếu hủy lặp lại).
5. **Thuật Toán Dọn Rác & Giải Phóng Bộ Nhớ Daemon (`daemon.py`):**
   * Giới hạn tối đa 32 Actor instances đồng thời trên một Worker (`MAX_ACTORS_PER_WORKER = 32`). Khi vượt ngưỡng hoặc áp lực RAM hệ thống $\ge 85\%$ (`MEMORY_PRESSURE_THRESHOLD = 85.0`), Worker tự động kích hoạt **LRU Actor Unload** (giải phóng instance của actor ít dùng nhất).
   * Khi dọn rác, daemon tự động gọi `torch.cuda.empty_cache()` (nếu có PyTorch) và gọi hàm hệ thống Linux C `libc.malloc_trim(0)` để giải phóng toàn bộ bộ nhớ fragmentation trả lại cho hệ điều hành.

---

## 3. CHI TIẾT 5 CHIẾN LƯỢC LẬP LỊCH PHÂN TÁN (SCHEDULING STRATEGIES)

Mô-đun lập lịch đặt tại [src/scheduler/strategy.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/scheduler/strategy.rs), triển khai trait `SchedulingStrategy`. Trước khi bất kỳ thuật toán nào chạy, hàm `poll_actor_task` luôn được ưu tiên kiểm tra trước để xử lý ngay các tác vụ Actor đang chờ trong mailbox.

<div style="display: grid; grid-template-columns: 1fr; gap: 16px; margin: 16px 0;">

<div style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #3b82f6; border-radius: 4px; padding: 16px;">
<h3 style="margin-top: 0; color: #1e40af;">3.1. LeastLoadStrategy (Chiến Lược Tải Thấp Nhất)</h3>

* **Nguyên lý cốt lõi:** Cân bằng tải động (Dynamic Load Balancing). Hệ thống theo dõi liên tục số lượng tác vụ đang xử lý (`worker_load`) trên từng Worker.
* **Mã nguồn thực thi chi tiết:**
  ```rust
  let mut my_load = 0;
  let mut min_load = usize::MAX;
  for (w_id, _) in &gcs.nodes {
      let load = gcs.worker_load.get(w_id).copied().unwrap_or(0);
      if load < min_load { min_load = load; }
      if *w_id == worker.id { my_load = load; }
  }
  if my_load > min_load {
      return None; // Từ chối giao việc, nhường task cho node rảnh hơn
  }
  queue.pop()
  ```
* **Kịch bản tối ưu:** Rất hiệu quả khi các tác vụ có thời gian thực thi lệch nhau nhiều (ví dụ: tác vụ tính toán mất 10s xen lẫn tác vụ mất 0.1s), ngăn chặn hiện tượng dồn ứ cục bộ.
</div>

<div style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #10b981; border-radius: 4px; padding: 16px;">
<h3 style="margin-top: 0; color: #065f46;">3.2. WeightedCapacityStrategy (Chiến Lược Năng Lực Trọng Số)</h3>

* **Nguyên lý cốt lõi:** Phù hợp cho cụm máy chủ không đồng nhất (Heterogeneous Cluster). Tác vụ được phân bổ tỷ lệ thuận theo sức mạnh phần cứng (CPU Cores và RAM).
* **Công thức toán học:**
  $$\text{Capacity} = \text{Physical CPUs} + \frac{\text{Total RAM (MB)}}{1024}$$
  $$\text{Score}(w) = \frac{\text{Capacity}(w)}{\text{worker\_load}(w) + 1.0}$$
* **Mã nguồn thực thi:** HeadNode tính `best_score = max(Score)`. Nếu Worker yêu cầu có điểm `my_score < best_score - 0.0001`, hệ thống từ chối để dành task cho máy có cấu hình mạnh hơn.
</div>

<div style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #f59e0b; border-radius: 4px; padding: 16px;">
<h3 style="margin-top: 0; color: #92400e;">3.3. LocalityFirstStrategy (Chiến Lược Ưu Tiên Vị Trí Dữ Liệu)</h3>

* **Nguyên lý cốt lõi:** *"Chuyển tính toán đến dữ liệu"* (Compute-over-Data). Triệt tiêu độ trễ truyền dữ liệu qua mạng LAN/WAN.
* **Mã nguồn thực thi:**
  ```rust
  queue.pop_by_locality(&worker.id.to_string())
       .or_else(|| queue.pop())
  ```
  Worker ưu tiên rút các tác vụ có sẵn Object đầu vào trong RAM `/dev/shm` cục bộ của mình trước khi rút từ hàng đợi chung.
</div>

<div style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #8b5cf6; border-radius: 4px; padding: 16px;">
<h3 style="margin-top: 0; color: #5b21b6;">3.4. AdaptiveStrategy (Chiến Lược Thích Ứng Toàn Diện - Mặc Định)</h3>

* **Nguyên lý cốt lõi:** Kết hợp đa tầng giữa Locality-First, Least-Load và thuật toán trộm việc (**Work-Stealing**).
* **Quy trình 3 bước:**
  1. *Bước 1 (Locality):* Rút task có sẵn dữ liệu cục bộ (`pop_by_locality`).
  2. *Bước 2 (Global Heap):* Rút task từ Min-Heap toàn cục (`pop`).
  3. *Bước 3 (Work-Stealing):* Nếu hàng đợi chung rỗng nhưng Worker khác đang bị nghẽn hàng đợi locality, Worker rảnh rỗi này tự động "trộm" task sang máy mình (`steal_any_local`), loại bỏ 100% thời gian chờ rảnh rỗi.
</div>

<div style="background: #ffffff; border: 1px solid #e2e8f0; border-left: 4px solid #64748b; border-radius: 4px; padding: 16px;">
<h3 style="margin-top: 0; color: #334155;">3.5. RoundRobinStrategy (Chiến Lược Phân Phối Vòng Tròn)</h3>

* **Nguyên lý cốt lõi:** Phân phối tuần tự vòng tròn giữa các Worker sử dụng biến đếm nguyên tử `AtomicUsize`. Thích hợp cho môi trường kiểm thử hoặc cụm máy chủ đồng nhất tuyệt đối.
</div>

</div>

---

## 4. CẤU TRÚC HÀNG ĐỢI ƯU TIÊN & MAILBOX (TASKQUEUE ARCHITECTURE)

Hàng đợi [src/scheduler/queue.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/scheduler/queue.rs) quản lý phân luồng tác vụ với 3 cấu trúc đồng thời:

```rust
pub struct TaskQueue {
    pending: BinaryHeap<PriorityTask>,
    locality: HashMap<CacheKey, VecDeque<Task>>,
    pub actor_mailboxes: HashMap<ActorId, VecDeque<Task>>,
    seq: u64,
}
```

* **Cơ Chế Min-Heap Ưu Tiên (`BinaryHeap<PriorityTask>`):**
  * `Priority 0`: Tác vụ phục hồi sau lỗi (Fault Recovery) khi một Worker bị sập. Được ưu tiên chạy ngay lập tức.
  * `Priority 128`: Tác vụ thông thường do người dùng gửi từ Driver.
  * `seq` (Sequence Number): Đảm bảo thứ tự FIFO công bằng giữa các tác vụ cùng mức ưu tiên.
* **Cắt Tỉa DAG Khi Hủy Job (`cancel_job_tasks`):** Khi Job bị hủy hoặc timeout, HeadNode gọi `cancel_job_tasks(job_id)` để cắt tỉa triệt để task trên cả 3 cấu trúc (`pending`, `locality`, và `actor_mailboxes`), giải phóng hàng đợi tức thì.

---

## 5. HỆ THỐNG WRITE-AHEAD LOG (WAL) & PHỤC HỒI THẢM HỌA

Tại [src/fault/wal.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/fault/wal.rs) và [src/cluster/head.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/cluster/head.rs), WAL đảm bảo an toàn tuyệt đối cho trạng thái cụm:

### 5.1. Định Dạng Nhị Phân & Luồng Ghi
Mỗi bản ghi được lưu dưới dạng **Binary Length-Delimited Framing**:
1. `Length Prefix (4 bytes)`: Độ dài payload dạng Big-Endian 32-bit integer.
2. `Payload`: Dữ liệu nhị phân tuần tự hóa qua thư viện `bincode` của enum `Message`.
3. Gọi `file.sync_data().await` đảm bảo dữ liệu ghi xuống phiến đĩa vật lý (hỗ trợ tắt đồng bộ bằng cờ `DISABLE_WAL_SYNC=1` để đo benchmark).

### 5.2. Chu Kỳ Checkpointing & Khôi Phục

```text
[ Ghi Nhật Ký Thời Gian Thực (WAL Append) ]
+----------------------+     Length-Delimited     +----------------------+     fsync()     +--------------------+
|  HeadNode Dispatcher |  ======================> |    /tmp/wal.log      |  =============> | Phiến Đĩa Vật Lý   |
|  (Task/State Event)  |    (4-byte len + bin)    |  (Bincode Framing)   |                 | (Persisted Storage)|
+----------------------+                          +----------------------+                 +--------------------+
                                                             |
[ Chu Kỳ Checkpoint 5 Phút (300 Giây) ]                      |
+------------------------------------------------------------+--------------------------------------------------+
| 1. Khóa ghi GCS -> Chạy enforce_job_retention()                                                               |
| 2. Ghi snapshot nguyên tử: /tmp/.../snapshot.bin.tmp  ==[ atomic rename ]==>  /tmp/.../snapshot.bin           |
| 3. wal.truncate() đưa file /tmp/wal.log về 0 byte                                                             |
+---------------------------------------------------------------------------------------------------------------+
                                                             |
[ Quy Trình Khôi Phục Thảm Họa (Crash Recovery) ]            v
+--------------------------+       +-------------------------+       +------------------------------------------+
|  Đọc snapshot.bin nạp    |  ==>  |  wal.replay() nạp các   |  ==>  |  Worker Reconnect: Worker gửi bù kết quả |
|  vào RAM GcsState        |       |  event chưa checkpoint  |       |  các task đã hoàn thành trong 30s        |
+--------------------------+       +-------------------------+       +------------------------------------------+
```

* **Checkpointing Định Kỳ 5 Phút (300 giây):**
  1. Khóa ghi GCS, chạy `enforce_job_retention` để dọn dẹp các Job cũ.
  2. Ghi snapshot nguyên tử ra `/tmp/pymapreduce/snapshot.bin.tmp`, sau đó `rename` đè sang `/tmp/pymapreduce/snapshot.bin`.
  3. Gọi `wal.truncate().await` đưa file `/tmp/wal.log` về 0 byte.
* **Quy Trình Khôi Phục (Recovery):** Khi HeadNode khởi động lại, nạp `snapshot.bin` vào RAM trước, sau đó gọi `wal.replay().await` áp dụng tuần tự các thông điệp chưa kịp checkpoint vào `GcsState` và `Dispatcher`.
* **Multi-task Concurrency & Reconciliation Tại Worker:** Khi HeadNode bị sập, các Worker vẫn tiếp tục tính toán hoàn thành toàn bộ các task được giao trong hàng đợi cục bộ. Khi HeadNode khởi động lại trong vòng 30s, Worker tự động kết nối và gửi bù toàn bộ kết quả (`ReportReady` / `TaskSuccess`) mà không làm mất công tính toán.

---

## 6. GLOBAL CONTROL STORE (GCS) & CHÍNH SÁCH DỌN DẸP HYBRID

Toàn bộ siêu dữ liệu của cụm được lưu trữ trong [src/gcs/state.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/gcs/state.rs):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsState {
    pub jobs: HashMap<JobId, JobRecord>,
    pub nodes: HashMap<WorkerId, NodeInfo>,
    pub active_tasks: HashMap<TaskId, TaskRecord>,
    pub worker_load: HashMap<WorkerId, usize>,
    pub bandwidth_ema: HashMap<WorkerId, f64>,
    pub object_catalog: HashMap<ObjectId, ObjectLocation>,
    pub object_subscribers: HashMap<ObjectId, HashSet<WorkerId>>,
    pub actors: HashMap<ActorId, ActorRecord>,
    #[serde(default)]
    pub finished_jobs_queue: VecDeque<(JobId, u64)>,
}
```

### Chính Sách Dọn Dẹp Hybrid Retention Policy

```text
+----------------------------------------------------------------------------------------------------+
|                               Job Kết Thúc (Completed / Failed)                                    |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                  Đẩy vào hàng đợi: finished_jobs_queue.push_back((job_id, now_secs))               |
+----------------------------------------------------------------------------------------------------+
                                                  |
                         +------------------------+------------------------+
                         |                                                 |
                         v                                                 v
+--------------------------------------------------+     +------------------------------------------+
|     1. Quét TTL Hết Hạn (Mặc định: 3600s)        |     |    2. Khống Chế FIFO Cap (Tối đa: 2000)  |
|  now - finish_time >= ttl_secs                   |     |  finished_jobs_queue.len() > 2000        |
+--------------------------------------------------+     +------------------------------------------+
                         |                                                 |
                         +------------------------+------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                                remove_job_if_not_running(job_id)                                   |
|   - Kiểm tra: status != JobStatus::Running  ==>  state.jobs.remove(&job_id) (Giải phóng RAM)      |
|   - Nếu status == Running: Bảo lưu tuyệt đối, không bao giờ xóa nhầm                               |
+----------------------------------------------------------------------------------------------------+
```

* **TTL Pruning ($\tau = 3600\text{s}$ - 1 Giờ):** Tự động giải phóng các Job đã kết thúc (`Completed` hoặc `Failed`) quá 1 giờ.
* **Bounded FIFO Cap ($N = 2000$ Jobs):** Giới hạn tối đa 2.000 Job lịch sử trong RAM. Khi đạt ngưỡng, tự động pop các Job hoàn tất cũ nhất.
* **Bảo Vệ Job Đang Chạy:** Hàm `remove_job_if_not_running` bảo đảm chỉ xóa khi `status != JobStatus::Running`. Các Job đang chạy không bao giờ bị xóa.

---

## 7. BỘ LƯU TRỮ ĐA TẦNG (TIERED OBJECT STORE) & ZERO-COPY

Đặt tại [src/object_store/store.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/object_store/store.rs):

```text
+----------------------------------------------------------------------------------------------------+
|                                        TIERED OBJECT STORE                                         |
|                                                                                                    |
|   +--------------------------------------------------------------------------------------------+   |
|   |  HOT TIER: POSIX Shared Memory (/dev/shm/pymapreduce)                                      |   |
|   |  - Giới hạn: 50% RAM vật lý của hệ thống                                                   |   |
|   |  - Đọc / Ghi: Zero-Copy trực tiếp giữa tiến trình Rust và Python                           |   |
|   |  - Theo dõi truy cập: AtomicU64 Touch Timestamp                                            |   |
|   +--------------------------------------------------------------------------------------------+   |
|                                                  |                                                 |
|                                                  | Tràn ngưỡng 50% RAM / Kích thước Object lớn     |
|                                                  | (Kích hoạt thuật toán LRU Eviction)             |
|                                                  v                                                 |
|   +--------------------------------------------------------------------------------------------+   |
|   |  WARM TIER: Disk Spilling qua Memory Mapping (memmap2)                                     |   |
|   |  - Ghi xuống ổ đĩa cục bộ (/tmp/pymapreduce/spill_*)                                       |   |
|   |  - Ánh xạ không gian địa chỉ ảo, nạp trang nhớ theo nhu cầu (Page Fault Demand Paging)     |   |
|   +--------------------------------------------------------------------------------------------+   |
+----------------------------------------------------------------------------------------------------+
                                                  ^
                                                  | Message::GetObjectWorker (P2P Direct Fetch)
                                                  | (Không đi qua HeadNode, thông báo qua tokio Notify)
                                                  v
+----------------------------------------------------------------------------------------------------+
|                        Worker downstream kéo dữ liệu để thực thi Reduce Task                       |
+----------------------------------------------------------------------------------------------------+
```

* **Hot Tier (RAM Shared Memory):** Lưu trữ trong `/dev/shm/pymapreduce` (POSIX Shared Memory). Tự động nhận diện và giới hạn tối đa 50% RAM vật lý (`detect_system_hot_tier_limit`). Dữ liệu đọc/ghi Zero-copy trực tiếp giữa Rust và Python.
* **Warm Tier (Memory-Mapped Disk Spilling):** Khi Hot Tier đầy hoặc kích thước Object vượt quá ngưỡng, dữ liệu được ghi xuống đĩa và ánh xạ không gian địa chỉ bộ nhớ ảo qua `memmap2`.
* **LRU Eviction:** Cấu trúc `HotEntry` duy trì `AtomicU64` touch timestamp. Object ít truy cập nhất sẽ tự động được chuyển xuống Warm Tier.
* **Truyền Dữ Liệu Ngang Hàng (Peer-to-Peer):** Worker yêu cầu dữ liệu trực tiếp từ Worker lưu trữ qua `Message::GetObjectWorker`, thông báo qua `tokio::sync::Notify` (Zero Polling Delay).

---

## 8. KHẢ NĂNG CHỊU LỖI MẠNG, RE-ATTACHMENT & GIÁM SÁT SỨC KHỎE

```text
+----------------------------------------------------------------------------------------------------+
|                          Đường truyền mạng giữa Driver và HeadNode bị ngắt                         |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|             Driver tự động kích hoạt Exponential Backoff: [1s, 2s, 4s, 6s, 8s, 9s]                 |
|             (Tổng thời gian chờ tối đa 30s trước khi báo lỗi Fatal)                                |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v  (Kết nối lại thành công)
+----------------------------------------------------------------------------------------------------+
|                         Driver gửi gói tin: Message::AttachJob { job_id }                          |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                               HeadNode tra cứu trạng thái trong GCS                                |
+----------------------------------------------------------------------------------------------------+
                         |                                                 |
                         | Job đã hoàn thành                               | Job vẫn đang chạy
                         v                                                 v
+--------------------------------------------------+     +------------------------------------------+
|  HeadNode gửi ngay: Message::JobComplete         |     |  Driver tiếp tục duy trì kết nối chờ     |
|  (Nhận kết quả lập tức, không chạy lại từ đầu)   |     |  kết quả trả về như bình thường          |
+--------------------------------------------------+     +------------------------------------------+
```

* **Driver Auto-Reconnect & Re-attachment ([src/driver/mod.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/driver/mod.rs)):** Khi mất kết nối mạng, Driver kích hoạt Exponential Backoff 6 bước `[1s, 2s, 4s, 6s, 8s, 9s]` (tối đa 30s). Khi kết nối lại thành công, Driver gửi `Message::AttachJob { job_id }`. Nếu Job đã hoàn thành trong lúc mất mạng, HeadNode gửi trả ngay kết quả `Message::JobComplete` mà không cần chạy lại từ đầu.
* **API Tra Cứu Tiến Độ (`get_job_status`):** Method `await driver.get_job_status(job_id)` trả về chuỗi `{completed}/{total} completed` để xây dựng thanh tiến trình thời gian thực.
* **HealthMonitor & Circuit Breaker ([src/fault/monitor.rs](file:///home/xuandong/Documents/Projects/MapReduce/src/fault/monitor.rs)):** Heartbeat chu kỳ 30s, cho phép 1 lần ân hạn. Khi Worker chết, tự động thu hồi task đưa vào hàng đợi ưu tiên cao nhất (**Priority 0**) và tái tạo Stateful Actor sang Worker khác.
* **Quản Lý Cụm Động (Cluster Management):** Cung cấp API `await pymapreduce.connect_worker()` và `await worker.disconnect(graceful=True)` gửi thông điệp `Message::WorkerDraining` để rút Worker an toàn mà không làm gián đoạn các task đang chạy.

---

## 9. CƠ CHẾ HỦY 2 GIAI ĐOẠN, CÔ LẬP TIẾN TRÌNH & XỬ LÝ 7 ĐIỂM MÙ HỆ THỐNG

### 9.1. Quy Trình Hủy 2 Giai Đoạn & Cô Lập Process Group

```text
+----------------------------------------------------------------------------------------------------+
|            Sự kiện Hủy Job: Timeout Watchdog / asyncio Drop Guard / Lệnh Hủy Tường Minh            |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|  1. Cắt tỉa hàng đợi DAG: cancel_job_tasks(job_id) (Heap Priority + Locality + Actor Mailbox)      |
|  2. HeadNode gửi gói tin: Message::CancelTask { task_id } tới các Worker đang thực thi             |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                             Worker Node: Xử lý Hủy 2 Giai Đoạn                                     |
|                                                                                                    |
|  [Giai Đoạn 1: Graceful Shutdown]                                                                  |
|  - Gửi tín hiệu: libc::killpg(pgid, SIGTERM) tới toàn bộ Process Group (-pgid)                    |
|  - Python daemon gọi hooks, dọn GPU CUDA memory, giải phóng tài nguyên                             |
|                                                                                                    |
|  [Giai Đoạn 2: Forceful Kill (Sau 3s Grace Period)]                                                |
|  - Nếu tiến trình con bị treo trong vòng lặp C/C++ hoặc deadlock không thoát:                      |
|  - Gửi tín hiệu: libc::killpg(pgid, SIGKILL) cưỡng chế giải phóng                                  |
|                                                                                                    |
|  [Dọn Dẹp Bộ Nhớ Đệm & Zombie Process]                                                             |
|  - Gửi Message::DeleteObject thu hồi phân vùng /dev/shm nếu task vừa kịp ghi RAM                   |
|  - Tini (PID 1 trong Docker init: true) gọi waitpid() thu dọn 100% tiến trình Zombie               |
+----------------------------------------------------------------------------------------------------+
```

* **Process Group Isolation ([pymapreduce/src/ipc.rs](file:///home/xuandong/Documents/Projects/MapReduce/pymapreduce/src/ipc.rs)):** `libc::setpgid(0, 0)` cô lập Worker daemon và toàn bộ cây tiến trình con (`multiprocessing`, `concurrent.futures`, `joblib`, C-extensions).
* **2-Phase Cancellation:**
  1. *Giai đoạn 1 (Graceful - SIGTERM):* Gửi `SIGTERM` tới Process Group (`-pgid`) để dọn dẹp tài nguyên êm đẹp.
  2. *Giai đoạn 2 (Forceful - SIGKILL sau 3s):* Nếu bị treo trong vòng lặp C/C++ thuần hoặc deadlock, tầng Rust gửi `killpg(pgid, SIGKILL)` cưỡng chế giải phóng.

### 9.2. Bảng Xử Lý 7 Điểm Mù Hệ Thống Phân Tán Đã Triển Khai Thực Tế

<table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
  <thead>
    <tr style="background: #f1f5f9; text-align: left;">
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Điểm Mù Kỹ Thuật</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Rủi Ro / Vấn Đề Gặp Phải</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Giải Pháp Đã Triển Khai Trong Mã Nguồn</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>1. Drop Safety</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Future Python bị drop qua <code>asyncio.wait_for</code> nhưng HeadNode/Worker vẫn chạy tiếp.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Triển khai <code>Drop</code> trait guard trên <code>JobHandle</code> (<a href="file:///home/xuandong/Documents/Projects/MapReduce/src/driver/handle.rs">src/driver/handle.rs</a>) tự động gửi <code>Message::CancelJob</code>.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>2. Race Condition TaskSuccess & Cancel</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Task vừa xong ghi vào RAM đúng lúc lệnh Cancel tới $\rightarrow$ rò rỉ object rác vĩnh viễn.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">HeadNode đối soát trạng thái Job; nếu Job đã Failed/Cancelled thì lập tức gửi <code>Message::DeleteObject</code> thu hồi RAM <code>/dev/shm</code>.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>3. Treo Hàng Đợi DAG Dependencies</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Map task bị hủy khiến Reduce task downstream trong queue <code>Pending</code> bị treo vĩnh viễn.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Hàm <code>cancel_job_tasks(job_id)</code> (<a href="file:///home/xuandong/Documents/Projects/MapReduce/src/scheduler/queue.rs">src/scheduler/queue.rs</a>) cắt tỉa đồng thời trên cả 3 cấu trúc (Heap, Locality, Actor Mailbox).</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>4. Lệch Nhịp Framing IPC (Broken Pipe)</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Tiến trình Python bị hủy giữa chừng làm dở dang luồng stdout $\rightarrow$ task sau đọc byte rác sinh <code>UnpicklingError</code>.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>PythonExecutor</code> (<a href="file:///home/xuandong/Documents/Projects/MapReduce/pymapreduce/src/lib.rs">pymapreduce/src/lib.rs</a>) vứt bỏ hoàn toàn <code>IpcWorker</code> hỏng ra khỏi Pool và spawn worker mới.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>5. Tích Tụ Rác Bộ Nhớ GCS</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Task retry quá 3 lần hoặc Job thất bại bỏ rơi <code>active_tasks</code> làm phình to RAM HeadNode.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Thống nhất hàm <code>cancel_job_internal</code> (<a href="file:///home/xuandong/Documents/Projects/MapReduce/src/cluster/head.rs">src/cluster/head.rs</a>) tự động dọn sạch toàn bộ <code>active_tasks</code> khỏi GCS.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>6. State Bloat Lịch Sử Job 24/7</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Bảng <code>state.jobs</code> lưu trữ vô hạn qua nhiều tháng gây cạn kiệt RAM HeadNode.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Triển khai <strong>Hybrid Job Retention Policy</strong> (Cap 2000, TTL 3600s, bảo vệ Job Running) tại <a href="file:///home/xuandong/Documents/Projects/MapReduce/src/gcs/state.rs">src/gcs/state.rs</a>.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><strong>7. Zombie Process Trong Container</strong></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Tiến trình con Python bị ngắt cưỡng bức trong Docker tối giản không có PID 1 reaper dẫn đến nghẽn bảng PID.</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Bổ sung cờ <code>init: true</code> trong <a href="file:///home/xuandong/Documents/Projects/MapReduce/docker-compose.yml">docker-compose.yml</a> để <code>tini</code> tự động gọi <code>waitpid()</code> dọn sạch 100% zombie.</td>
    </tr>
  </tbody>
</table>

---

## 10. CHUẨN HÓA MA TRẬN TIMEOUT HỆ THỐNG

Toàn bộ các tham số thời gian trong hệ thống đã được chuẩn hóa 100% về đơn vị **GIÂY (Seconds)**:

<div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 16px;">
  <ul>
    <li><code>max_time</code> (trong <code>JobConfig</code> và CLI <code>--max-time</code>): Giới hạn thời gian tối đa của Job (**Giây**). HeadNode Watchdog tự động hủy nếu quá hạn.</li>
    <li><code>idle_timeout</code> (trong <code>start_worker</code>, <code>connect_worker</code> và CLI <code>--idle-timeout</code>): Thời gian chờ rảnh rỗi của Worker daemon (**Giây**) trước khi thu hồi tiến trình con thừa (mặc định: 0 / không bao giờ tắt).</li>
    <li><code>idle_timeout</code> (trong <code>@pymapreduce.remote(idle_timeout=...)</code>): Thời gian rảnh của từng Actor instance (**Giây**) trước khi giải phóng khỏi RAM.</li>
    <li><code>heartbeat_timeout</code>: **30 Giây** để xác định Worker bị crash.</li>
    <li><code>SIGKILL Grace Period</code>: **3 Giây** sau khi gửi <code>SIGTERM</code>.</li>
  </ul>
</div>

---

## 11. MÔ HÌNH LẬP TRÌNH PYTHON & DISTRIBUTED DATASET API

### 11.1. Tác Vụ Không Trạng Thái (Stateless Tasks)
```python
import asyncio
import pymapreduce

@pymapreduce.remote
def process_partition(x: int) -> int:
    return x * x + 1

async def main():
    driver = await pymapreduce.init(strategy=pymapreduce.SchedulerStrategy.Adaptive)
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    tasks = [process_partition.remote(i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    print("Kết quả tính toán song song:", results)

if __name__ == "__main__":
    asyncio.run(main())
```

### 11.2. Diễn Viên Phân Tán Có Trạng Thái (Stateful Actors)
```python
import asyncio
import pymapreduce

@pymapreduce.remote
class ParameterServer:
    def __init__(self, initial_weights: list):
        self.weights = initial_weights

    def update_gradients(self, grads: list) -> list:
        for i in range(len(self.weights)):
            self.weights[i] -= 0.01 * grads[i]
        return self.weights

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    ps = await ParameterServer.remote(initial_weights=[0.5, 0.2, -0.1])
    updated = await ps.update_gradients.remote([0.01, -0.02, 0.05])
    print("Trọng số mô hình sau cập nhật:", updated)

    await pymapreduce.kill(ps)

if __name__ == "__main__":
    asyncio.run(main())
```

### 11.3. Xử Lý Song Song Với Distributed Dataset Pipeline
```python
import asyncio
import pymapreduce
from pymapreduce.data import read_csv

async def run_data_pipeline():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=8)

    # Đọc dữ liệu lớn phân vùng tự động
    dataset = read_csv("dataset/*.csv", block_size_mb=64)

    # Chuỗi biến đổi phân tán MapReduce
    result_df = await (
        dataset.filter(lambda df: df["status"] == "CONFIRMED")
               .map_batches(lambda df: df.assign(total=df["price"] * df["qty"]), batch_format="pandas")
               .repartition(num_blocks=8)
               .to_pandas()
    )
    print(result_df.describe())

if __name__ == "__main__":
    asyncio.run(run_data_pipeline())
```

---

## 12. HƯỚNG DẪN VẬN HÀNH CLI & TRIỂN KHAI DOCKER

### 12.1. Cài Đặt Gói Thư Viện
```bash
pip install pymapreduce-core
```

### 12.2. Tra Cứu Toàn Diện Các Cờ Dòng Lệnh CLI (CLI Arguments Reference)

Công cụ dòng lệnh `mapreduce` cung cấp 2 lệnh chính: `mapreduce start` (khởi chạy các nút cụm) và `mapreduce submit` (gửi mã nguồn Python lên cụm).

#### A. Cờ cấu hình cho lệnh `mapreduce start` (Khởi động Node trong Cụm)

| Cờ Lệnh (Flag) | Giá Trị Mặc Định | Đối Tượng Áp Dụng | Dùng Làm Gì & Hành Vi Kỹ Thuật Chi Tiết |
| :--- | :--- | :--- | :--- |
| **`--head`** | `False` | Head Node | **Khởi chạy tiến trình với vai trò Head Node (Master / Điều phối cụm).**<br>Lắng nghe kết nối mạng, khởi tạo Global Control Store (GCS), Write-Ahead Log (WAL), hàng đợi `TaskQueue`, điều phối phân phối task cho các Worker và chạy Watchdog Auto-Scaling. |
| **`--worker`** | `False` | Worker Node | **Khởi chạy tiến trình với vai trò Worker Node (Nút tính toán).**<br>Chủ động kết nối tới Head Node, báo cáo số CPU core và RAM, quản lý pool tiến trình con Python (`daemon.py`), nhận task để thực thi và ghi đối tượng trung gian vào `/dev/shm`. |
| **`--head-addr <IP:PORT>`** | `127.0.0.1:7777` | Worker & Submit | **Địa chỉ IP và Port của Head Node.**<br>Worker Node dùng cờ này để biết địa chỉ kết nối vào Master. Client `submit` dùng cờ này để gửi script tới đúng cụm cần chạy. |
| **`--port <PORT>`** | `7777` | Head Node | **Cổng mạng TCP mà Head Node mở để lắng nghe.**<br>Tất cả các Worker và Client Driver kết nối vào cổng này qua giao thức nhị phân Framed TCP có độ trễ thấp. |
| **`--cpus <N>`** | Toàn bộ core CPU | Worker Node | **Giới hạn số core CPU vật lý mà Worker được phép sử dụng.**<br>Nếu không truyền cờ này, Worker tự động nhận diện và tận dụng 100% core CPU vật lý trên máy. Dung lượng tải tác vụ tối đa tương ứng là `2 x CPUs` (theo quy tắc 2xCore chống idle). |
| **`--idle-timeout <SECS>`** | `0` (Không timeout) | Worker Node | **Thời gian chờ (giây) trước khi tự hủy tiến trình con Python rảnh rỗi.**<br>Nếu sau số giây này Worker không nhận thêm task nào, các process con Python sẽ tự ngắt để giải phóng RAM cho OS. Giá trị `0` nghĩa là process sống vĩnh viễn trong suốt vòng đời worker. |
| **`--scheduler <STRATEGY>`** | `RoundRobin` (CLI)<br>`Adaptive` (Core) | Head Node | **Lựa chọn thuật toán lập lịch phân phối tác vụ từ hàng đợi tới Worker.**<br>• `Adaptive`: Tự động thích ứng đa chiều theo băng thông mạng EMA, CPU load và data locality (Khuyến nghị cho production).<br>• `LeastLoad`: Ưu tiên worker có ít task đang chạy nhất.<br>• `WeightedCapacity`: Ưu tiên worker có cấu hình CPU + RAM lớn hơn.<br>• `LocalityFirst`: Ưu tiên worker đang giữ sẵn dữ liệu trung gian trong RAM để tránh copy qua mạng.<br>• `RoundRobin`: Phân phối xoay vòng chia đều. |
| **`--auto-scale`** | `False` | Head Node | **Kích hoạt cơ chế Tự động Co Giãn Cụm (Elastic Auto-Scaling).**<br>Khởi chạy vòng lặp Watchdog định kỳ 1 giây trên Head Node. Khi phát hiện các worker máy chính đã bão hòa và hàng đợi ứ đọng kéo dài, Watchdog sẽ tự động gửi tín hiệu `WakeUp` để đánh thức các worker dự phòng (`--standby`). |
| **`--scale-out-delay <SECS>`** | `5` (giây) | Head Node (Auto-Scale) | **Cửa sổ thời gian duy trì tắc nghẽn tối thiểu trước khi scale-out (Lớp 1).**<br>Lọc bỏ hoàn toàn các đợt tăng tải tức thời trong vài mili-giây (micro-bursts). Chỉ khi hàng đợi bị nghẽn liên tục trong ít nhất khoảng thời gian này thì máy phụ mới được đánh thức. |
| **`--scale-out-min-tasks <N>`** | `8` (task) | Head Node (Auto-Scale) | **Độ sâu hàng đợi tối thiểu (số lượng pending tasks) để scale-out (Lớp 2).**<br>Tránh việc đánh thức máy phụ dự phòng chỉ vì còn sót lại 1-2 task nhỏ lẻ mà máy chính có thể giải quyết nhanh chóng. |
| **`--scale-out-cooldown <SECS>`** | `30` (giây) | Head Node (Auto-Scale) | **Khoảng thời gian hạ nhiệt tối thiểu giữa 2 lần scale-out liên tiếp (Lớp 3).**<br>Ngăn chặn hiện tượng đánh thức ồ ạt toàn bộ máy phụ cùng lúc, cho phép cụm có thời gian hấp thụ công việc và ổn định trạng thái tải. |
| **`--standby`** | `False` | Worker Node | **Khởi động Worker ở chế độ Warm Standby Pool (Máy phụ dự phòng).**<br>Worker duy trì kết nối TCP và gửi Heartbeat định kỳ với Head Node nhưng chiếm dụng **0% CPU** (không sinh tiến trình con Python). Bị bỏ qua 100% bởi bộ lập lịch cho đến khi được Head Node kích hoạt Scale-Out. |

---

#### B. Cờ cấu hình cho lệnh `mapreduce submit` (Gửi Script Phân Tán)

| Đối Số / Cờ Lệnh | Giá Trị Mặc Định | Dùng Làm Gì & Hành Vi Kỹ Thuật Chi Tiết |
| :--- | :--- | :--- |
| **`script`** *(Vị trí đầu tiên)* | *Bắt buộc* | **Đường dẫn tới tệp mã nguồn Python cần chạy trên cụm.**<br>File script chứa code khai báo `@pymapreduce.remote` (Function hoặc Stateful Actor) hoặc Distributed Dataset pipeline. |
| **`--head-addr <IP:PORT>`** | `127.0.0.1:7777` | **Địa chỉ IP và Port của Head Node điều phối.**<br>Client Driver thiết lập kênh điều khiển tới địa chỉ này để nộp job, theo dõi tiến độ và nhận kết quả trả về. |
| **`--max-time <SECS>`** | `None` (Không giới hạn) | **Thời gian chạy tối đa cho toàn bộ Job (Watchdog Execution Timeout).**<br>Nếu job chạy vượt quá số giây này, Head Node và Driver sẽ tự động kích hoạt quy trình hủy khẩn cấp (Cancellation), ngắt các tiến trình con đang chạy và giải phóng tài nguyên. |

---

### 12.3. Các Kịch Bản Vận Hành Mẫu (Execution Recipes)

```bash
# 1. Chạy cụm tĩnh truyền thống (Static Cluster)
# Khởi động Master:
mapreduce start --head --port 7777 --scheduler Adaptive
# Khởi động Worker máy 1 (16 cores):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16
# Khởi động Worker máy 2 (32 cores):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 32

# 2. Khởi chạy Cụm Tự Động Co Giãn (Auto-Scaling với Warm Standby Pool)
# Master bật Watchdog (chống micro-burst 5s, nghẽn tối thiểu 8 tasks, cooldown 30s):
mapreduce start --head --port 7777 --scheduler Adaptive --auto-scale --scale-out-delay 5 --scale-out-min-tasks 8 --scale-out-cooldown 30
# Worker máy chính (Active sẵn sàng nhận việc):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16
# Worker máy phụ dự phòng (Standby: 0% CPU, tự động đánh thức khi quá tải):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16 --standby

# 3. Gửi script phân tán kèm timeout bảo vệ 120s
mapreduce submit pipeline.py --head-addr 127.0.0.1:7777 --max-time 120
```

---

### 12.4. Cơ Chế Auto-Scaling Cụm Máy Vật Lý (Warm Standby Architecture)

> **Lưu ý quan trọng:** Cơ chế Auto-Scaling (cụm co giãn) chỉ phục vụ các tác vụ tính toán phân tán thông thường (`Map`, `Reduce`), không áp dụng cho các Stateful Actor.

```
                            +-----------------------------------------------+
                            |            HeadNode (Watchdog Loop)           |
                            |  - Kiểm tra độ bão hòa Active (100% CPU)      |
                            |  - Cửa sổ tắc nghẽn hàng đợi >= 5s            |
                            |  - Số lượng task đọng trong queue >= 8        |
                            |  - Cooldown giữa 2 đợt scale-out >= 30s       |
                            +-----------------------+-----------------------+
                                                    |
                         +--------------------------+--------------------------+
                         |                                                     |
                         v                                                     v
            +-------------------------+                           +-------------------------+
            |   Worker Node 1         |                           |   Worker Node 2         |
            |   (Active - Máy chính)  |                           |   (Standby - Máy phụ)   |
            |   - Nhận task bình thường|                          |   - TCP & Heartbeat ON   |
            |   - 100% CPU & RAM      |                           |   - Python Process = 0  |
            +-------------------------+                           |   - 0% CPU & RAM        |
                                                                  +-------------------------+
                                                                               | (Scale-Out: WakeUp)
                                                                               v
                                                                  [Woken Up -> Active Worker]
```

1. **Khắc phục Xung Đột Cold-Start của Load Balancer:**
   * Trong các hệ thống thông thường, nếu cắm thêm worker dự phòng thì các thuật toán lập lịch (`RoundRobin`, `LeastLoad`, `WeightedCapacity`) sẽ lập tức chia đôi số task cho worker dự phòng, làm mất đi ý nghĩa của máy dự phòng.
   * PyMapReduce giải quyết triệt để vấn đề này bằng cách cô lập hoàn toàn worker có trạng thái `NodeStatus::Standby`: **Cả 5 thuật toán lập lịch bỏ qua 100% node Standby** trong điều kiện bình thường.
2. **Bộ Điều Kiện Scale-Out 3 Lớp (Tránh Báo Động Giả):**
   * *Lớp 1 (Cửa sổ thời gian duy trì tắc nghẽn):* Tắc nghẽn phải duy trì liên tục $\ge 5\text{s}$ (`--scale-out-delay`). Các đợt tăng tải đột ngột 200ms (micro-burst) sẽ được bỏ qua.
   * *Lớp 2 (Độ sâu hàng đợi):* Số lượng task pending phải $\ge 8$ (`--scale-out-min-tasks`).
   * *Lớp 3 (Thời gian hạ nhiệt):* Cooldown giữa các đợt scale-out $\ge 30\text{s}$ (`--scale-out-cooldown`) để cụm ổn định.
3. **Tách Biệt Stateful & Stateless Workloads (Kiến Trúc Cloud Elastic Chuẩn Mực):**
   * **Cấm Actor trên Elastic Worker:** Tất cả Stateful Actor (`@pymapreduce.remote`) chỉ được host trên các Core Active Nodes cố định; bộ lập lịch đảm bảo không bao giờ phân bổ Actor lên các Elastic Standby Workers.
   * **Elastic Workers chuyên trách Stateless Compute:** Khi được đánh thức, các standby worker tập trung 100% tài nguyên để giải tỏa ùn tắc cho các tác vụ tính toán phân tán thuần túy (`Map`, `Reduce`).
   * **Quy Trình Safe Scale-In Đơn Giản & Triệt Để:** Nhờ không mang gánh nặng Actor, máy phụ không bao giờ bị 'giam lỏng vĩnh viễn'. Chỉ cần: không còn task đang chạy (`current_load == 0`), không giữ intermediate object dependency còn cần cho cụm, và rảnh rỗi liên tục vượt ngưỡng idle timeout (60s) là lập tức bước vào chế độ Standby.
4. **Cơ Chế Tự Thích Ứng Thích Nghi (Fallback Adaptation):**
   * Nếu người dùng chạy worker với cờ `--standby` nhưng HeadNode lại không bật `--auto-scale`, HeadNode sẽ tự động chuyển trạng thái worker thành `Active` và gửi lệnh `WakeUp` ngay lập tức để tránh tình trạng worker bị "bỏ rơi" vĩnh viễn do sai sót cấu hình.

---

### 12.5. Triển Khai Cụm Bằng Docker Compose
```bash
# Khởi chạy toàn bộ cụm
docker compose up -d

# Mở rộng quy mô Worker lên 4 container
docker compose up -d --scale workernode=4

# Xem log giám sát thời gian thực
docker compose logs -f
```

---

## 13. BẢNG TRA CỨU MÃ NGUỒN TOÀN HỆ THỐNG

<table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
  <thead>
    <tr style="background: #f1f5f9; text-align: left;">
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Tệp Tin Mã Nguồn</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Phân Tầng Kiến Trúc</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Hàm / Struct Trọng Tâm</th>
      <th style="padding: 10px; border: 1px solid #cbd5e1;">Nhiệm Vụ Kỹ Thuật Chính</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/fault/wal.rs">src/fault/wal.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Fault Tolerance</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>WriteAheadLog</code>, <code>append</code>, <code>replay</code>, <code>truncate</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Ghi nhật ký biến đổi nhị phân, phục hồi trạng thái sau sự cố.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/gcs/state.rs">src/gcs/state.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Global State</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>GcsState</code>, <code>mark_job_finished</code>, <code>enforce_job_retention</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Quản lý metadata cụm, thực thi Hybrid Job Retention Policy.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/cluster/head.rs">src/cluster/head.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Head Node</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>HeadNode::run</code>, <code>run_autoscaler_loop</code>, <code>check_and_scale_out</code>, <code>check_and_scale_in</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Điều phối kết nối, lập lịch, xử lý snapshot 300s, watchdog timeout và tự động co giãn cụm (Auto-Scaling).</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/cluster/worker.rs">src/cluster/worker.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Worker Node</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>WorkerNode::run</code>, <code>with_standby</code>, <code>handle_assign_task</code>, <code>handle_cancel_task</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Nhận task, quản lý concurrency limiter, thu hồi RAM <code>/dev/shm</code>, hỗ trợ Warm Standby pool.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/scheduler/strategy.rs">src/scheduler/strategy.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Scheduler</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>Adaptive</code>, <code>WeightedCapacity</code>, <code>LeastLoad</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Cài đặt chi tiết 5 thuật toán lập lịch phân tán, tự động bỏ qua worker Standby.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/scheduler/queue.rs">src/scheduler/queue.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Scheduler</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>TaskQueue</code>, <code>PriorityTask</code>, <code>cancel_job_tasks</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Hàng đợi Min-Heap, Mailbox cho Actor và cắt tỉa DAG.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/object_store/store.rs">src/object_store/store.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Object Store</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>ObjectStore</code>, <code>HotEntry</code>, <code>evict_lru</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Lưu trữ đa tầng Hot RAM và Warm Memory Mapped Disk Spilling.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/fault/monitor.rs">src/fault/monitor.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Fault Tolerance</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>HealthMonitor</code>, <code>scan_and_recover</code>, <code>remove_dead_worker</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Giám sát heartbeat 30s, re-queue task ưu tiên 0 và phục hồi Actor.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/driver/mod.rs">src/driver/mod.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Driver Client</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>Driver::connect</code>, <code>get_job_status</code>, Auto-reconnect loop</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Kênh điều khiển Client, tự kết nối lại 30s mất mạng, gửi <code>AttachJob</code>.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/src/driver/handle.rs">src/driver/handle.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Driver Client</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>JobHandle</code>, <code>Drop</code> trait implementation</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">RAII Drop Guard tự động phát tín hiệu hủy khi future bị timeout/drop.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/pymapreduce/src/ipc.rs">pymapreduce/src/ipc.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Python IPC</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>IpcWorker</code>, <code>setpgid</code>, <code>kill_group</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Cô lập Process Group và điều khiển tín hiệu ngắt OS (SIGTERM/SIGKILL).</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/pymapreduce/src/lib.rs">pymapreduce/src/lib.rs</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">PyO3 Bridge</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>PythonExecutor</code>, <code>DriverWrapper</code>, <code>remote</code> decorator</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Cầu nối FFI Python-Rust, quản lý Actor metadata và Lazy Re-creation.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/pymapreduce/python/pymapreduce/daemon.py">pymapreduce/python/pymapreduce/daemon.py</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Python Daemon</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>_ACTORS</code>, <code>_collect_garbage</code>, <code>_sigterm_handler</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Tiến trình thực thi Python, quản lý bộ nhớ GPU/glibc và vòng đời Actor.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/pymapreduce/python/pymapreduce/data/dataset.py">pymapreduce/python/pymapreduce/data/dataset.py</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Data API</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>Dataset</code>, <code>map_batches</code>, <code>filter</code>, <code>repartition</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Trừu tượng hóa phân vùng dữ liệu song song cho Pandas, PyArrow và NumPy.</td>
    </tr>
    <tr>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><a href="file:///home/xuandong/Documents/Projects/MapReduce/docker-compose.yml">docker-compose.yml</a></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Deployment</td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;"><code>services.headnode</code>, <code>services.workernode</code>, <code>init: true</code></td>
      <td style="padding: 10px; border: 1px solid #cbd5e1;">Cấu hình Docker Compose với cơ chế init zombie process reaper.</td>
    </tr>
  </tbody>
</table>
