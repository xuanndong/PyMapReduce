# PyMapReduce

PyMapReduce is an asynchronous distributed computing framework that combines a Rust core engine with Python `async`/`await` bindings. It provides primitives for running stateless distributed tasks, managing long-lived stateful actors, and processing datasets across multi-core machines or networked clusters.

---

## Table of Contents

- [1. Installation](#1-installation)
  - [From PyPI](#from-pypi)
  - [From Source](#from-source)
  - [Worker Environment & Dependencies](#worker-environment--dependencies)
- [2. Quickstart](#2-quickstart)
  - [2.1. Stateless Tasks](#21-stateless-tasks)
  - [2.2. Stateful Actors](#22-stateful-actors)
  - [2.3. Distributed Datasets](#23-distributed-datasets)
- [3. Command-Line Interface (CLI)](#3-command-line-interface-cli)
  - [3.1. Starting Cluster Nodes (`mapreduce start`)](#31-starting-cluster-nodes-mapreduce-start)
  - [3.2. Submitting Scripts (`mapreduce submit`)](#32-submitting-scripts-mapreduce-submit)
  - [3.3. Usage Examples](#33-usage-examples)
- [4. Cluster Deployment & Auto-Scaling](#4-cluster-deployment--auto-scaling)
  - [4.1. Docker Compose](#41-docker-compose)
  - [4.2. Warm Standby Auto-Scaling](#42-warm-standby-auto-scaling)
- [5. Scheduling Strategies](#5-scheduling-strategies)
- [6. Memory & Fault Tolerance](#6-memory--fault-tolerance)
  - [6.1. Tiered Object Store](#61-tiered-object-store)
  - [6.2. Recovery & Health Monitoring](#62-recovery--health-monitoring)

---

## 1. Installation

### From PyPI

Pre-built binary wheels are available on PyPI:

```bash
pip install pymapreduce-core
```

### From Source

Building from source requires a Rust toolchain (1.75+) and Python 3.8+:

```bash
git clone https://github.com/xuanndong/PyMapReduce.git
cd PyMapReduce/pymapreduce

python -m venv .venv
source .venv/bin/activate

pip install maturin cloudpickle
maturin develop --release
```

### Worker Environment & Dependencies

> [!IMPORTANT]
> **Worker Dependencies Notice:**
> Tasks and actors executed via `@pymapreduce.remote` run within the local Python environment of the assigned Worker nodes. Therefore, **all third-party Python packages required by your workload must be installed on every Worker node**.
> 
> - *Example (Machine Learning):* If your remote tasks use Scikit-learn (e.g. `from sklearn.ensemble import RandomForestClassifier`), you must run `pip install scikit-learn` in the Python environment of every worker machine.
> - *Example (Data Processing / PyTorch):* Ensure packages like `pandas`, `numpy`, or `torch` are similarly installed on all workers.
> - When using Docker, list your application packages in `requirements.txt` so the worker container installs them during startup.

---

## 2. Quickstart

PyMapReduce provides two execution primitives—**Stateless Tasks** and **Stateful Actors**—along with a **Distributed Dataset** API for larger data workloads.

### 2.1. Stateless Tasks

Decorate any Python function with `@pymapreduce.remote`. Calling `.remote()` schedules and executes the task asynchronously across available cluster workers:

```python
import asyncio
import pymapreduce

@pymapreduce.remote
def process_partition(x: int) -> int:
    return x * x + 1

async def main():
    driver = await pymapreduce.init(strategy=pymapreduce.SchedulerStrategy.Adaptive)
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # Launch 10 parallel tasks across workers
    refs = [await process_partition.remote(i) for i in range(10)]

    # Collect results asynchronously
    results = await asyncio.gather(*[ref.get() for ref in refs])
    print("Results:", results)

if __name__ == "__main__":
    asyncio.run(main())
```

### 2.2. Stateful Actors

Decorating a Python class with `@pymapreduce.remote` turns it into a **Stateful Actor**. The actor instance resides on a dedicated worker, maintaining its internal state across invocations. Subsequent method calls are routed in FIFO order to that worker:

```python
import asyncio
import pymapreduce

@pymapreduce.remote(idle_timeout=60.0)
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

    # Instantiate the actor on the cluster
    ps = await ParameterServer.remote(initial_weights=[0.5, 0.2, -0.1])

    # Invoke methods remotely
    res_ref = await ps.update_gradients.remote([0.01, -0.02, 0.05])
    print("Updated weights:", await res_ref.get())

    # Release actor resources when finished
    await pymapreduce.kill(ps)

if __name__ == "__main__":
    asyncio.run(main())
```

- **`idle_timeout`**: Optional duration in seconds. If no method calls arrive within this window, the worker unloads the actor to release memory.

### 2.3. Distributed Datasets

The Dataset API reads partitioned files in blocks and supports chained distributed transformations:

```python
import asyncio
import pymapreduce
from pymapreduce.data import read_csv

async def run_data_pipeline():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=8)

    # Read data split into 64 MB blocks
    dataset = read_csv("dataset/*.csv", block_size_mb=64)

    # Distributed filter, map, and repartition pipeline
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

## 3. Command-Line Interface (CLI)

The `mapreduce` command-line tool provides two primary commands:
- `mapreduce start`: Starts cluster nodes (Head Node or Worker Node).
- `mapreduce submit`: Submits a Python script for execution on the cluster.

### 3.1. Starting Cluster Nodes (`mapreduce start`)

#### Flag Reference Table

| Flag | Default | Applies To | Description |
| :--- | :--- | :--- | :--- |
| **`--head`** | `False` | Head Node | Starts the node as a Head Node (Master). |
| **`--worker`** | `False` | Worker Node | Starts the node as a Worker Node (Compute engine). |
| **`--head-addr <IP:PORT>`** | `127.0.0.1:7777` | Worker Node | Network address (IP:port) of the Head Node to join. |
| **`--port <PORT>`** | `7777` | Head Node | Listening TCP port for the Head Node service. |
| **`--cpus <N>`** | All physical cores | Worker Node | Maximum CPU cores allocated to this worker. |
| **`--idle-timeout <SECS>`** | `0` (Disabled) | Worker Node | Idle seconds before child processes exit to free RAM. |
| **`--scheduler <STRATEGY>`** | `RoundRobin` | Head Node | Task scheduling policy (`Adaptive`, `LeastLoad`, etc.). |
| **`--auto-scale`** | `False` | Head Node | Activates the dynamic scaling watchdog loop on the Master. |
| **`--scale-out-delay <SECS>`** | `5` | Head Node | Sustained queue backlog duration required before scale-out. |
| **`--scale-out-min-tasks <N>`** | `8` | Head Node | Minimum pending task count threshold to trigger scale-out. |
| **`--scale-out-cooldown <SECS>`**| `30` | Head Node | Cooldown duration between consecutive scale-out actions. |
| **`--standby`** | `False` | Worker Node | Starts the worker in warm standby (0% CPU until awakened). |

#### Detailed Flag Explanations

- **`--head`**:
  Initializes the Head Node (Master coordinator). The Head Node listens for incoming TCP connections from workers and drivers, tracks global cluster metadata (Global Control Store), manages the prioritized task queue, and coordinates auto-scaling.
- **`--worker`**:
  Initializes a Worker Node. Workers actively connect to the designated Head Node, report available CPU/RAM resources, spawn Python worker processes to execute tasks or actors, and store intermediate data in shared memory.
- **`--head-addr <IP:PORT>`**:
  The network address where the Head Node is reachable (e.g., `127.0.0.1:7777` locally, or `192.168.1.100:7777` across a local network). Worker nodes require this flag to register with the cluster.
- **`--port <PORT>`**:
  The TCP port on which the Head Node listens for cluster traffic. Ensure this port is open and accessible through any active firewalls.
- **`--cpus <N>`**:
  Restricts the worker to use up to `N` CPU cores. When omitted, the worker automatically detects and utilizes all physical CPU cores on the machine. The worker scales its internal concurrent execution capacity based on this value.
- **`--idle-timeout <SECS>`**:
  Configures the worker daemon lifecycle. If no tasks are assigned to the worker for this duration, idle child Python processes terminate to return memory to the operating system. Setting `0` leaves processes running indefinitely.
- **`--scheduler <STRATEGY>`**:
  Specifies the algorithm the Head Node uses to distribute queued tasks to connected workers:
  - `Adaptive` (Recommended for production): Combines data locality, load balancing, and work-stealing.
  - `LeastLoad`: Favors the worker with the fewest active tasks.
  - `WeightedCapacity`: Dispatches proportionally to hardware capacity (CPU cores and RAM).
  - `LocalityFirst`: Prioritizes workers holding required data objects in local RAM.
  - `RoundRobin`: Cycles sequentially through workers.
- **`--auto-scale`**:
  Enables the automated cluster scaler watchdog on the Head Node. When active workers reach full capacity and the pending task queue remains saturated, the Head Node signals standby workers to awaken.
- **`--scale-out-delay <SECS>`**:
  The minimum duration (in seconds) the queue backlog must persist before waking standby workers. This prevents unnecessary scaling during brief, sub-second traffic spikes (micro-bursts).
- **`--scale-out-min-tasks <N>`**:
  The minimum number of waiting tasks in the queue required to trigger scale-out. Small batches that can be quickly absorbed by primary workers will not trigger scale-out.
- **`--scale-out-cooldown <SECS>`**:
  The quiet period (in seconds) enforced between consecutive scale-out actions, allowing the newly awakened capacity to stabilize cluster load.
- **`--standby`**:
  Launches the worker in Warm Standby mode. The process connects to the Head Node and exchanges periodic heartbeats, but consumes 0% CPU (spawns no Python workers) and is ignored by schedulers until explicitly awakened by a Head Node scale-out signal.

---

### 3.2. Submitting Scripts (`mapreduce submit`)

| Parameter / Flag | Default | Description |
| :--- | :--- | :--- |
| **`script`** *(Positional)* | *Required* | Path to the Python script containing the distributed workflow to run. |
| **`--head-addr <IP:PORT>`** | `127.0.0.1:7777` | IP address and port of the coordinating Head Node. |
| **`--max-time <SECS>`** | `None` (Unlimited) | Execution limit in seconds. If execution exceeds this window, the job is canceled to prevent resource leaks. |

- **`script`**: The target `.py` file to execute. This file imports `pymapreduce` and defines remote functions, actors, or dataset pipelines.
- **`--head-addr`**: Directs the driver client to connect to the specified Head Node.
- **`--max-time`**: Acts as a safety watchdog timer. If a long-running batch or complex training pipeline hangs or exceeds expected runtimes, the cluster terminates the job cleanly.

---

### 3.3. Usage Examples

```bash
# 1. Standard cluster (1 Head Node + 2 Worker Nodes)
# Start Head Node:
mapreduce start --head --port 7777 --scheduler Adaptive

# Start Worker 1 (16 cores):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16

# Start Worker 2 (32 cores):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 32

# 2. Cluster with Auto-Scaling (Warm Standby)
# Start Head Node with auto-scaling enabled:
mapreduce start --head --port 7777 --scheduler Adaptive --auto-scale --scale-out-delay 5 --scale-out-min-tasks 8

# Start primary active worker:
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16

# Start standby worker (idle until queue experiences sustained backlog):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16 --standby

# 3. Submit a script with a 120-second timeout
mapreduce submit pipeline.py --head-addr 127.0.0.1:7777 --max-time 120
```

---

## 4. Cluster Deployment & Auto-Scaling

### 4.1. Docker Compose

A [docker-compose.yml](file:///home/xuandong/Documents/Projects/MapReduce/docker-compose.yml) file is provided for quick multi-container setup:

```bash
# Start cluster (1 Head Node + 1 Worker Node)
docker compose up -d

# View real-time cluster logs
docker compose logs -f

# Stop the cluster
docker compose down
```

> [!NOTE]
> The worker container automatically installs dependencies listed in `./requirements.txt` upon startup. Add any required packages (such as `scikit-learn`, `pandas`, or `torch`) to `requirements.txt` before starting the containers.

### 4.2. Warm Standby Auto-Scaling

PyMapReduce supports a warm standby model for handling workload fluctuations:

1. **Standby Workers:** Nodes started with `--standby` maintain TCP connections and periodic heartbeats with the Head Node while consuming negligible CPU resources (no Python daemon processes are spawned). Scheduling algorithms bypass standby workers during regular load.
2. **Multi-layer Scale-Out Guards:** To avoid premature scaling from short bursts:
   - Queue backlog must persist for at least `--scale-out-delay` (default: 5 seconds).
   - Pending task count must reach at least `--scale-out-min-tasks` (default: 8 tasks).
   - A cooldown period of `--scale-out-cooldown` (default: 30 seconds) must elapse between scale-out events.
3. **Workload Separation:** Stateful actors are placed only on primary active workers, while standby workers process stateless tasks (`Map`, `Reduce`). This allows standby workers to transition back to idle once backlog clears without affecting actor state.

---

## 5. Scheduling Strategies

The scheduling strategy can be configured using `--scheduler <STRATEGY>` when starting the Head Node:

- **`Adaptive` (Default / Recommended):**
  Combines data locality preference, dynamic load balancing, and work-stealing. If a worker becomes idle while tasks remain in other queues, it pulls tasks to balance overall cluster throughput.
- **`LeastLoad`:**
  Assigns tasks to the worker with the fewest active tasks. Useful when individual task execution times vary widely.
- **`WeightedCapacity`:**
  Distributes tasks proportionally based on hardware capacity (CPU cores and available memory). Best suited for heterogeneous clusters.
- **`LocalityFirst`:**
  Prioritizes placing tasks on workers that already hold input dependencies in local shared memory, reducing network transfer overhead.
- **`RoundRobin`:**
  Distributes tasks sequentially across workers in round-robin order. Suitable for homogeneous test environments.

---

## 6. Memory & Fault Tolerance

### 6.1. Tiered Object Store

- **Shared Memory Tier (`/dev/shm`):** Intermediate objects are placed in POSIX shared memory for zero-copy access between Rust and Python processes, capped at 50% of system RAM.
- **Disk Spillover:** When memory thresholds are reached, least-recently-used (LRU) objects are spilled to disk and accessed via memory mapping (`memmap2`).
- **Peer-to-Peer Transfers:** Downstream workers fetch intermediate object dependencies directly from peer workers over TCP, avoiding bottlenecks at the Head Node.

### 6.2. Recovery & Health Monitoring

- **Driver Reconnection:** If the connection between a driver and the Head Node drops, the driver attempts reconnection for up to 30 seconds. Once reconnected, it re-attaches to the running job without restarting computation.
- **Write-Ahead Logging (WAL) & Checkpoints:** State updates are logged to an append-only file, with periodic snapshots taken every 5 minutes to support crash recovery on the Head Node.
- **Heartbeat Monitoring:** The Head Node tracks worker health using a 30-second heartbeat interval. If a worker fails, its pending and active tasks are rescheduled onto available workers, and affected actors are lazily recreated upon subsequent method calls.
