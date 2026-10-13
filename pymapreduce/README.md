# PyMapReduce

<div align="center">

[![Version](https://img.shields.io/badge/version-0.1.16-blue.svg?style=flat-square)](https://pypi.org/project/pymapreduce-core/)
[![Python](https://img.shields.io/badge/python-3.8%2B-brightgreen.svg?style=flat-square)](https://www.python.org/)
[![Rust](https://img.shields.io/badge/powered%20by-Rust%201.75%2B-orange.svg?style=flat-square)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)](LICENSE)

<p style="font-size: 1.15em; color: #444; max-width: 800px; margin: 15px auto;">
A high-performance, asynchronous distributed computing framework for Python, powered by an ultra-fast Rust runtime. Easily scale Python functions, stateful AI/ML actors, and data processing pipelines across multi-core CPUs and distributed clusters with native <code>async/await</code> syntax.
</p>

</div>

---

## Quickstart

```python
import asyncio
import pymapreduce

# 1. Stateless distributed function
@pymapreduce.remote
def square(x: int) -> int:
    return x * x

# 2. Stateful distributed actor with auto memory cleanup
@pymapreduce.remote(idle_timeout=60.0)
class Counter:
    def __init__(self, start: int = 0):
        self.value = start

    def increment(self, amount: int = 1) -> int:
        self.value += amount
        return self.value

async def main():
    # Start local cluster coordinator (HeadNode) and connect a local worker
    driver = await pymapreduce.init("127.0.0.1:7777")
    worker = await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # Run tasks in parallel and fetch lazy object references
    refs = [await square.remote(i) for i in range(5)]
    results = await asyncio.gather(*[ref.get() for ref in refs])
    print("Parallel Squares:", results)  # [0, 1, 4, 9, 16]

    # Instantiate stateful actor on the cluster
    counter = await Counter.remote(start=10)
    res_ref = await counter.increment.remote(5)
    print("Counter Value:", await res_ref.get())  # 15

    # Clean up actor and worker
    await pymapreduce.kill(counter)
    await worker.disconnect(graceful=True)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Table of Contents

- [PyMapReduce](#pymapreduce)
  - [Quickstart](#quickstart)
  - [Table of Contents](#table-of-contents)
  - [1. Installation](#1-installation)
    - [From PyPI (Recommended)](#from-pypi-recommended)
    - [Building from Source](#building-from-source)
  - [2. User Guide](#2-user-guide)
    - [2.1. Distributed Tasks (Functions)](#21-distributed-tasks-functions)
    - [2.2. Distributed Actors (Stateful Classes)](#22-distributed-actors-stateful-classes)
    - [2.3. Actor Memory Management & Lifecycle](#23-actor-memory-management--lifecycle)
    - [2.4. Automatic Code Shipping (`runtime_env`)](#24-automatic-code-shipping-runtime_env)
    - [2.5. Zero-Copy Shared Memory (`ObjectRef`)](#25-zero-copy-shared-memory-objectref)
    - [2.6. Distributed Datasets](#26-distributed-datasets)
    - [2.7. Job Timeouts & Task Cancellation](#27-job-timeouts--task-cancellation)
    - [2.8. Auto-Scaling Cluster (Warm Standby Pool)](#28-auto-scaling-cluster-warm-standby-pool)
  - [3. Cluster Deployment](#3-cluster-deployment)
    - [3.1. Local Mode (Embedded)](#31-local-mode-embedded)
    - [3.2. Multi-Server Production Cluster](#32-multi-server-production-cluster)
    - [3.3. Docker & Kubernetes Deployment](#33-docker--kubernetes-deployment)
    - [3.4. Auto-Scaling Production Cluster](#34-auto-scaling-production-cluster)
  - [4. Task Scheduling Strategies](#4-task-scheduling-strategies)
  - [5. Command Line Interface (CLI)](#5-command-line-interface-cli)
    - [5.1. Starting Cluster Nodes (`mapreduce start`)](#51-starting-cluster-nodes-mapreduce-start)
    - [5.2. Submitting Standalone Scripts (`mapreduce submit`)](#52-submitting-standalone-scripts-mapreduce-submit)
  - [6. Python API Reference](#6-python-api-reference)
    - [Top-Level Functions](#top-level-functions)
    - [Core Classes & Methods](#core-classes--methods)
  - [7. Configuration & Environment Variables](#7-configuration--environment-variables)
  - [8. License](#8-license)

---

## 1. Installation

### From PyPI (Recommended)

```bash
pip install pymapreduce-core
```

### Building from Source

```bash
git clone https://github.com/xuandong/MapReduce.git
cd MapReduce/pymapreduce

python3 -m venv .venv
source .venv/bin/activate

pip install maturin cloudpickle
maturin develop --release
```

---

## 2. User Guide

### 2.1. Distributed Tasks (Functions)

Decorate any Python function with `@pymapreduce.remote` to execute it asynchronously across worker CPU cores:

```python
import asyncio
import pymapreduce

@pymapreduce.remote
def process_item(item_id: int, scale: float) -> float:
    return item_id * scale

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # 1. Single execution
    res_ref = await process_item.remote(10, 2.5)
    print("Single Result:", await res_ref.get())

    # 2. Parallel execution
    refs = [await process_item.remote(i, 2.5) for i in range(10)]
    results = await asyncio.gather(*[ref.get() for ref in refs])
    print("Parallel Results:", results)

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.2. Distributed Actors (Stateful Classes)

Decorate a Python class with `@pymapreduce.remote` to create a **Stateful Actor**. The instance stays resident in worker memory, preserving state across successive RPC invocations:

```python
import asyncio
import pymapreduce

@pymapreduce.remote
class MLModelServer:
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.call_count = 0
        print(f"Loaded {self.model_name} into RAM")

    def predict(self, features: list) -> dict:
        self.call_count += 1
        return {"prediction": sum(features) * 0.5, "model": self.model_name}

    def get_stats(self) -> dict:
        return {"total_calls": self.call_count}

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # 1. Instantiate actor on the cluster
    model = await MLModelServer.remote(model_name="ResNet50")

    # 2. Call methods sequentially in FIFO order
    p1 = await (await model.predict.remote([1.0, 2.0, 3.0])).get()
    p2 = await (await model.predict.remote([4.0, 5.0])).get()
    stats = await (await model.get_stats.remote()).get()

    print("Predictions:", p1, p2)
    print("Stats:", stats)

    # 3. Explicitly kill actor when done
    await pymapreduce.kill(model)

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.3. Actor Memory Management & Lifecycle

To prevent memory leaks and protect workers from running out of RAM, PyMapReduce provides built-in lifecycle management:

- **Idle Timeout (TTL)**: Automatically unloads the actor instance if no calls are received for a specified duration.
- **Explicit Kill**: Call `await pymapreduce.kill(actor)` or `await actor.destroy.remote()` to immediately free allocated resources.
- **Resource Hook**: Define a `close()` method in your class to perform custom cleanup (closing file handles, flushing DB connections) when the actor is destroyed.

```python
@pymapreduce.remote(idle_timeout=60.0)  # Auto-unload after 60s of inactivity
class HeavyService:
    def __init__(self):
        self.cache = {}

    def query(self, key: str) -> str:
        return self.cache.setdefault(key, f"val_{key}")

    def close(self):
        """Called automatically when actor is destroyed."""
        print("Releasing resources...")
```

---

### 2.4. Automatic Code Shipping (`runtime_env`)

When distributing code across physical servers, use `runtime_env` to automatically package and synchronize your local project folder (respecting `.gitignore`):

```python
import asyncio
import pymapreduce

async def main():
    # Automatically package & sync local project files to all cluster workers
    driver = await pymapreduce.connect(
        "192.168.1.10:7777",
        runtime_env={
            "working_dir": ".",
            "excludes": ["*.log", "data/raw/*"]
        }
    )

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.5. Zero-Copy Shared Memory (`ObjectRef`)

When tasks return large NumPy arrays, Pandas DataFrames, or PyArrow tables, PyMapReduce writes them to shared memory (`/dev/shm`) and returns an `ObjectRef`. Passing this reference into subsequent tasks avoids network copies and serialization overhead:

```python
import asyncio
import pymapreduce
import pandas as pd
import numpy as np

@pymapreduce.remote
def create_dataframe(rows: int) -> pd.DataFrame:
    return pd.DataFrame(np.random.randn(rows, 10))

@pymapreduce.remote
def compute_summary(df: pd.DataFrame) -> dict:
    return df.mean().to_dict()

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # 1. Returns an ObjectRef handle
    df_ref = await create_dataframe.remote(rows=500_000)

    # 2. Pass reference directly into next task (zero network copy if collocated)
    summary_ref = await compute_summary.remote(df_ref)
    summary = await summary_ref.get()
    print("Summary:", summary)

    # 3. Or fetch the value locally
    local_df = await df_ref.get()
    print("Fetched DataFrame shape:", local_df.shape)

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.6. Distributed Datasets

The Dataset API provides lazy, functional transformations for parallel data processing:

```python
import asyncio
import pymapreduce

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # 1. Create dataset from items, CSV, or Pandas
    ds = await pymapreduce.from_items(range(1, 100_001), num_partitions=8)

    # 2. Lazy transformation pipeline
    transformed = await (
        await ds.map(lambda x: x * 2)
    ).filter(lambda x: x % 4 == 0)

    # 3. Actions & Conversions
    count = await transformed.count()
    sample = await transformed.take(5)
    df = await transformed.to_pandas()

    print(f"Total: {count}, Sample: {sample}, DataFrame Rows: {len(df)}")

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.7. Job Timeouts & Task Cancellation

Prevent hung jobs by configuring execution timeouts:

```python
import asyncio
import pymapreduce

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # Set maximum execution timeout of 30 seconds for this job
    config = pymapreduce.JobConfig("AnalyticsJob", max_time=30)
    
    # Submit job through driver
    results = await driver.submit(config)
```

---

### 2.8. Auto-Scaling Cluster (Warm Standby Pool)

PyMapReduce provides native cluster auto-scaling designed specifically for bare-metal multi-server setups without requiring Kubernetes or external orchestrators.

#### The Problem: Load Balancer Cold-Split Conflict
In traditional distributed engines, adding a standby server causes standard schedulers (RoundRobin, LeastLoad, WeightedCapacity) to immediately split incoming jobs across all connected nodes—preventing any idle standby state.

#### The Solution: Warm Standby Pool
- **Warm Standby State (`--standby`)**: The standby machine connects to the HeadNode, keeps TCP and heartbeat alive, but maintains **0% CPU usage** and **0 active Python worker processes**.
- **Scheduler Isolation**: All 5 scheduling strategies (`Adaptive`, `LeastLoad`, `WeightedCapacity`, `LocalityFirst`, `RoundRobin`) completely ignore nodes in `Standby` status during normal execution.
- **Multi-Layer Scale-Out Guard**:
  1. *Sustained Congestion Window*: Queue backlog must persist for at least $\ge 5$ seconds (configurable via `--scale-out-delay`) to filter out 200ms micro-bursts.
  2. *Queue Backlog Depth*: At least $\ge 8$ pending tasks (configurable via `--scale-out-min-tasks`) must be queued.
  3. *Cooldown Protection*: Minimum $\ge 30$ seconds (configurable via `--scale-out-cooldown`) between successive scale-outs to allow the cluster to stabilize.
- **Safe Scale-In ("Three-No" Zero Data Loss Filter)**:
  Before returning a worker to Standby mode, the Watchdog verifies:
  1. No running tasks remain on the node.
  2. No alive Stateful Actors reside in the worker's memory.
  3. No intermediate shuffle objects held by the node are needed as dependencies for active cluster jobs.
- **Self-Healing Fallback**: If a worker connects with `--standby` to a HeadNode where `--auto-scale` is disabled, the HeadNode automatically adapts it to `Active` to prevent permanent starvation.

---

## 3. Cluster Deployment

### 3.1. Local Mode (Embedded)

Ideal for single-machine development and testing:

```python
driver = await pymapreduce.init(
    address="127.0.0.1:7777",
    scheduler=pymapreduce.SchedulerStrategy.Adaptive
)
await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)
```

### 3.2. Multi-Server Production Cluster

Deploy across multiple physical servers using the `mapreduce` CLI:

```bash
# 1. Master Server (192.168.1.10): Start Head Node
mapreduce start --head --port 7777 --scheduler Adaptive

# 2. Worker Server 1 (192.168.1.20): Join cluster
mapreduce start --worker --head-addr 192.168.1.10:7777 --cpus 16

# 3. Worker Server 2 (192.168.1.21): Join cluster
mapreduce start --worker --head-addr 192.168.1.10:7777 --cpus 32
```

In your application:

```python
driver = await pymapreduce.connect(
    "192.168.1.10:7777",
    runtime_env={"working_dir": "."}
)
```

### 3.3. Docker & Kubernetes Deployment

When deploying inside Docker containers, set `init: true` to ensure clean process reaping:

```yaml
version: '3.8'
services:
  headnode:
    image: pymapreduce:latest
    init: true
    command: ["mapreduce", "start", "--head", "--port", "7777"]
    ports:
      - "7777:7777"

  workernode:
    image: pymapreduce:latest
    init: true
    command: ["mapreduce", "start", "--worker", "--head-addr", "headnode:7777"]
    depends_on:
      - headnode
```

### 3.4. Auto-Scaling Production Cluster

For elastic production setups with physical standby servers:

```bash
# 1. Master Server (192.168.1.10): Start Head Node with Auto-Scaling Watchdog
mapreduce start --head --port 7777 --auto-scale --scale-out-delay 5 --scale-out-min-tasks 8 --scale-out-cooldown 30

# 2. Primary Worker Server (192.168.1.20): Starts Active immediately
mapreduce start --worker --head-addr 192.168.1.10:7777 --cpus 16

# 3. Elastic Standby Server (192.168.1.21): Warm Standby (0% CPU until woke up)
mapreduce start --worker --head-addr 192.168.1.10:7777 --cpus 16 --standby
```

---

## 4. Task Scheduling Strategies

Configure how tasks are scheduled across available workers:

<table width="100%">
  <thead>
    <tr>
      <th width="22%">Strategy</th>
      <th width="28%">Python Enum / CLI Flag</th>
      <th width="50%">Description & Best Use Case</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Adaptive</strong><br><em>(Default)</em></td>
      <td><code>SchedulerStrategy.Adaptive</code><br><code>--scheduler Adaptive</code></td>
      <td>
        Evaluates real-time network bandwidth (EMA), CPU load, and data locality. Best for heterogeneous production clusters.
      </td>
    </tr>
    <tr>
      <td><strong>LocalityFirst</strong></td>
      <td><code>SchedulerStrategy.LocalityFirst</code><br><code>--scheduler LocalityFirst</code></td>
      <td>
        Prioritizes dispatching tasks to the worker already holding required data in RAM to minimize network transfers.
      </td>
    </tr>
    <tr>
      <td><strong>LeastLoad</strong></td>
      <td><code>SchedulerStrategy.LeastLoad</code><br><code>--scheduler LeastLoad</code></td>
      <td>
        Dispatches tasks to the worker with the fewest active tasks. Best for long-running compute jobs.
      </td>
    </tr>
    <tr>
      <td><strong>WeightedCapacity</strong></td>
      <td><code>SchedulerStrategy.WeightedCapacity</code><br><code>--scheduler WeightedCapacity</code></td>
      <td>
        Weights assignment based on each worker's physical CPU cores and available RAM size.
      </td>
    </tr>
    <tr>
      <td><strong>RoundRobin</strong></td>
      <td><code>SchedulerStrategy.RoundRobin</code><br><code>--scheduler RoundRobin</code></td>
      <td>
        Rotates tasks evenly across all available workers. Ideal for baseline benchmarking.
      </td>
    </tr>
  </tbody>
</table>

---

## 5. Command Line Interface (CLI)

PyMapReduce provides the `mapreduce` command-line tool with two primary commands: `mapreduce start` (for launching cluster nodes) and `mapreduce submit` (for submitting standalone Python scripts to the cluster).

---

### 5.1. Starting Cluster Nodes (`mapreduce start`)

Used to start either the cluster coordinator (**Head Node**) or a compute worker (**Worker Node**).

```bash
# General Syntax
mapreduce start [--head | --worker] [OPTIONS]
```

#### Detailed CLI Flags Reference for `mapreduce start`

<table width="100%">
  <thead>
    <tr>
      <th width="24%">Flag & Syntax</th>
      <th width="14%">Default</th>
      <th width="16%">Target Node</th>
      <th width="46%">Purpose & Technical Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>--head</code></td>
      <td><code>False</code></td>
      <td>Head Node</td>
      <td>
        <strong>Starts the node as the Cluster Coordinator (Head Node / Master).</strong><br>
        Listens for network connections, initializes the Global Control Store (GCS), Write-Ahead Log (WAL), multi-tier <code>TaskQueue</code>, schedules tasks across workers, and runs the Auto-Scaling Watchdog loop.
      </td>
    </tr>
    <tr>
      <td><code>--worker</code></td>
      <td><code>False</code></td>
      <td>Worker Node</td>
      <td>
        <strong>Starts the node as a Compute Worker (Worker Node).</strong><br>
        Actively connects to the Head Node, reports hardware capacity (CPUs, RAM), manages a pool of isolated Python worker processes (<code>daemon.py</code>), executes tasks, and stores intermediate objects in <code>/dev/shm</code>.
      </td>
    </tr>
    <tr>
      <td><code>--head-addr &lt;IP:PORT&gt;</code></td>
      <td><code>127.0.0.1:7777</code></td>
      <td>Worker / Submit</td>
      <td>
        <strong>IP address and port of the Head Node.</strong><br>
        Used by Worker Nodes to connect to the cluster coordinator, or by <code>mapreduce submit</code> to dispatch distributed jobs to the master.
      </td>
    </tr>
    <tr>
      <td><code>--port &lt;PORT&gt;</code></td>
      <td><code>7777</code></td>
      <td>Head Node</td>
      <td>
        <strong>TCP port for the Head Node to bind and listen on.</strong><br>
        All Worker Nodes and client Drivers communicate with the Head Node through this port via low-latency framed binary TCP.
      </td>
    </tr>
    <tr>
      <td><code>--cpus &lt;N&gt;</code></td>
      <td>All host CPU cores</td>
      <td>Worker Node</td>
      <td>
        <strong>Limits the number of physical CPU cores allocated to the Worker Node.</strong><br>
        Defaults to all detected physical cores on the host machine (100% CPU utilization). The maximum concurrency capacity is <code>2 x CPUs</code> to prevent idle latency.
      </td>
    </tr>
    <tr>
      <td><code>--idle-timeout &lt;SECS&gt;</code></td>
      <td><code>0</code> (Disabled)</td>
      <td>Worker Node</td>
      <td>
        <strong>Idle timeout in seconds before terminating idle Python worker processes.</strong><br>
        If no tasks are assigned within this duration, idle Python processes are killed to free system memory. <code>0</code> keeps worker processes persistent indefinitely.
      </td>
    </tr>
    <tr>
      <td><code>--scheduler &lt;NAME&gt;</code></td>
      <td><code>RoundRobin</code></td>
      <td>Head Node</td>
      <td>
        <strong>Task scheduling strategy used by the Head Node.</strong><br>
        Supported options:<br>
        • <code>Adaptive</code>: Dynamically evaluates real-time network bandwidth (EMA), CPU load, and data locality (Recommended for production).<br>
        • <code>LeastLoad</code>: Prioritizes workers with the lowest number of active tasks.<br>
        • <code>WeightedCapacity</code>: Weighs worker capacity based on physical CPU cores and RAM size.<br>
        • <code>LocalityFirst</code>: Prioritizes workers that already hold input object dependencies in local RAM.<br>
        • <code>RoundRobin</code>: Rotates tasks evenly across all available workers.
      </td>
    </tr>
    <tr>
      <td><code>--auto-scale</code></td>
      <td><code>False</code></td>
      <td>Head Node</td>
      <td>
        <strong>Enables cluster elastic auto-scaling on the Head Node.</strong><br>
        Spawns a periodic 1-second Watchdog loop. When active workers reach full saturation and the task backlog persists, the Watchdog wakes up warm standby workers (<code>--standby</code>) by sending <code>WakeUp</code> messages.<br>
        <em>Note: Auto-Scaling exclusively serves standard distributed compute tasks (<code>Map</code>/<code>Reduce</code>) and does not scale stateful Actors.</em>
      </td>
    </tr>
    <tr>
      <td><code>--scale-out-delay &lt;SECS&gt;</code></td>
      <td><code>5</code></td>
      <td>Head Node (Auto-Scale)</td>
      <td>
        <strong>Sustained queue congestion duration before triggering scale-out (Layer 1).</strong><br>
        Prevents false alarms from transient micro-bursts (&le; 200ms). Scale-out triggers only if the queue remains continuously congested for at least this many seconds.
      </td>
    </tr>
    <tr>
      <td><code>--scale-out-min-tasks &lt;N&gt;</code></td>
      <td><code>8</code></td>
      <td>Head Node (Auto-Scale)</td>
      <td>
        <strong>Minimum queue depth (number of pending tasks) required to scale out (Layer 2).</strong><br>
        Avoids waking up standby servers for only a few residual tasks that primary workers can quickly finish.
      </td>
    </tr>
    <tr>
      <td><code>--scale-out-cooldown &lt;SECS&gt;</code></td>
      <td><code>30</code></td>
      <td>Head Node (Auto-Scale)</td>
      <td>
        <strong>Cooldown period in seconds between successive scale-outs (Layer 3).</strong><br>
        Prevents cascading scale-out spikes, giving newly awakened workers time to accept tasks and absorb cluster load.
      </td>
    </tr>
    <tr>
      <td><code>--standby</code></td>
      <td><code>False</code></td>
      <td>Worker Node</td>
      <td>
        <strong>Starts the worker in Warm Standby Pool mode for auto-scaling.</strong><br>
        Maintains TCP connectivity and periodic heartbeats with the Head Node while consuming <strong>0% CPU</strong> (no background Python worker processes). Dedicated exclusively to stateless compute (<code>Map</code>/<code>Reduce</code>); stateful Actors are never assigned to standby workers. Ignored by scheduling algorithms until awakened by the Head Node during scale-out.
      </td>
    </tr>
  </tbody>
</table>

---

### 5.2. Submitting Standalone Scripts (`mapreduce submit`)

Dispatches a standalone Python script containing distributed tasks or actor workflows to the cluster:

```bash
# General Syntax
mapreduce submit <script.py> [--head-addr <IP:PORT>] [--max-time <SECONDS>]
```

#### Detailed CLI Flags Reference for `mapreduce submit`

<table width="100%">
  <thead>
    <tr>
      <th width="26%">Argument / Flag</th>
      <th width="18%">Default</th>
      <th width="56%">Purpose & Technical Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>script</code> <em>(Positional)</em></td>
      <td><em>Required</em></td>
      <td>
        <strong>Path to the Python script to execute.</strong><br>
        The script utilizes the <code>pymapreduce</code> library to submit distributed functions, actors, or dataset transformations.
      </td>
    </tr>
    <tr>
      <td><code>--head-addr &lt;IP:PORT&gt;</code></td>
      <td><code>127.0.0.1:7777</code></td>
      <td>
        <strong>IP address and port of the target Head Node coordinator.</strong><br>
        The client driver connects to this endpoint to submit jobs, monitor real-time execution progress, and collect outputs.
      </td>
    </tr>
    <tr>
      <td><code>--max-time &lt;SECONDS&gt;</code></td>
      <td><code>None</code> (Unlimited)</td>
      <td>
        <strong>Maximum execution timeout for the entire job in seconds (Watchdog Timeout).</strong><br>
        If job execution exceeds this limit, the Head Node automatically triggers two-phase cancellation, sending <code>SIGTERM/SIGKILL</code> to kill tasks and reclaim cluster resources.
      </td>
    </tr>
  </tbody>
</table>

---

### 5.3. CLI Quickstart Examples

```bash
# 1. Traditional Static Cluster (1 Master, 1 Worker)
mapreduce start --head --port 7777 --scheduler Adaptive
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16

# 2. Elastic Auto-Scaling Cluster (Warm Standby Pool)
# Coordinator with Watchdog:
mapreduce start --head --port 7777 --scheduler Adaptive --auto-scale --scale-out-delay 5 --scale-out-min-tasks 8
# Primary Worker (Active, accepts tasks immediately):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16
# Secondary Standby Worker (0% CPU, awakens automatically when congested):
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 16 --standby

# 3. Submit a distributed pipeline with a 120-second watchdog limit
mapreduce submit pipeline.py --head-addr 127.0.0.1:7777 --max-time 120
```

---

## 6. Python API Reference

### Top-Level Functions

<table width="100%">
  <thead>
    <tr>
      <th width="28%">Function</th>
      <th width="32%">Parameters</th>
      <th width="40%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>pymapreduce.init(...)</code></td>
      <td><code>address=None, runtime_env=None, scheduler=None</code></td>
      <td>Starts an embedded cluster coordinator and returns a connected <code>Driver</code>.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.connect(...)</code></td>
      <td><code>head_node, workers=None, runtime_env=None</code></td>
      <td>Connects to an existing remote cluster coordinator.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.connect_worker(...)</code></td>
      <td><code>head_addr, cpus=None, idle_timeout=0, standby=False</code></td>
      <td>Launches a background worker connected to the cluster and returns a <code>WorkerHandle</code>. Set <code>standby=True</code> for warm standby auto-scaling.</td>
    </tr>
    <tr>
      <td><code>@pymapreduce.remote(...)</code></td>
      <td><code>cls_or_func=None, idle_timeout=None</code></td>
      <td>Decorator turning a function into a distributed task or a class into a stateful actor.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.kill(...)</code></td>
      <td><code>actor_handle</code></td>
      <td>Asynchronously purges an actor instance from worker memory and cluster registry.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.from_items(...)</code></td>
      <td><code>items, num_partitions=None</code></td>
      <td>Creates a distributed <code>Dataset</code> from in-memory sequences.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.read_csv(...)</code></td>
      <td><code>path_or_glob, num_partitions=None</code></td>
      <td>Creates a distributed <code>Dataset</code> by chunking CSV files.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.from_pandas(...)</code></td>
      <td><code>df, num_partitions=None</code></td>
      <td>Creates a distributed <code>Dataset</code> from a Pandas DataFrame.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.from_numpy(...)</code></td>
      <td><code>arr, num_partitions=None</code></td>
      <td>Creates a distributed <code>Dataset</code> from a NumPy array.</td>
    </tr>
  </tbody>
</table>

### Core Classes & Methods

<table width="100%">
  <thead>
    <tr>
      <th width="25%">Class</th>
      <th width="35%">Method</th>
      <th width="40%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>ActorHandle</code></td>
      <td><code>&lt;method&gt;.remote(*args, **kwargs)</code></td>
      <td>Dispatches an RPC call to the actor's FIFO mailbox on the worker node.</td>
    </tr>
    <tr>
      <td><code>ActorHandle</code></td>
      <td><code>destroy.remote()</code> / <code>kill.remote()</code></td>
      <td>Destroys this actor instance and cleans up allocated memory.</td>
    </tr>
    <tr>
      <td><code>ObjectRef</code></td>
      <td><code>await ref.get()</code></td>
      <td>Fetches the object from in-memory ObjectStore.</td>
    </tr>
    <tr>
      <td><code>ObjectRef</code></td>
      <td><code>ref.pin()</code> / <code>ref.unpin()</code></td>
      <td>Pins object in hot memory to prevent LRU cache eviction.</td>
    </tr>
    <tr>
      <td><code>WorkerHandle</code></td>
      <td><code>await handle.disconnect(graceful=True)</code></td>
      <td>Gracefully drains in-flight tasks and disconnects the worker.</td>
    </tr>
    <tr>
      <td><code>JobConfig</code></td>
      <td><code>JobConfig(name, max_time=None)</code></td>
      <td>Specifies job parameters and watchdog execution timeout in seconds.</td>
    </tr>
    <tr>
      <td><code>Dataset</code></td>
      <td><code>await ds.map(fn)</code> / <code>filter(fn)</code> / <code>flat_map(fn)</code></td>
      <td>Applies transformations across distributed partitions.</td>
    </tr>
    <tr>
      <td><code>Dataset</code></td>
      <td><code>await ds.map_batches(fn, batch_format="pandas")</code></td>
      <td>Applies batch transformations using Pandas, PyArrow, or NumPy.</td>
    </tr>
    <tr>
      <td><code>Dataset</code></td>
      <td><code>await ds.count()</code> / <code>take(n)</code> / <code>collect()</code></td>
      <td>Executes the pipeline and returns results.</td>
    </tr>
    <tr>
      <td><code>Dataset</code></td>
      <td><code>await ds.to_pandas()</code> / <code>to_arrow()</code></td>
      <td>Collects distributed partitions into a single Pandas DataFrame or PyArrow Table.</td>
    </tr>
  </tbody>
</table>

---

## 7. Configuration & Environment Variables

<table width="100%">
  <thead>
    <tr>
      <th width="35%">Environment Variable</th>
      <th width="15%">Default</th>
      <th width="50%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>PYMAPREDUCE_AUTO_SCALE</code></td>
      <td><code>0</code> / <code>false</code></td>
      <td>Enables cluster auto-scaling watchdog loop on the Head Node when set to <code>1</code> or <code>true</code>.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_SCALE_OUT_DELAY</code></td>
      <td><code>5</code></td>
      <td>Sustained queue congestion duration (in seconds) required before scale-out triggers.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_SCALE_OUT_MIN_TASKS</code></td>
      <td><code>8</code></td>
      <td>Minimum depth of queued pending tasks required to qualify for scale-out.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_SCALE_OUT_COOLDOWN</code></td>
      <td><code>30</code></td>
      <td>Cooldown window (in seconds) enforced between consecutive scale-out actions.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_MAX_TIME</code></td>
      <td><code>None</code></td>
      <td>Cluster-wide execution timeout in seconds for submitted jobs.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_MAX_ACTORS_PER_WORKER</code></td>
      <td><code>32</code></td>
      <td>Maximum number of concurrently active actors per worker process before LRU eviction triggers.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_ACTOR_IDLE_TIMEOUT</code></td>
      <td><code>0</code> (disabled)</td>
      <td>Default idle timeout (in seconds) for actors that do not specify an explicit <code>idle_timeout</code>.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_MEMORY_THRESHOLD_PERCENT</code></td>
      <td><code>85.0</code></td>
      <td>Host RAM threshold percentage. If system memory exceeds this percentage, LRU eviction activates proactively to avoid OOM crashes.</td>
    </tr>
    <tr>
      <td><code>PYMAPREDUCE_MAX_ACTOR_META</code></td>
      <td><code>1024</code></td>
      <td>Maximum number of actor metadata entries cached for virtual lazy re-creation.</td>
    </tr>
  </tbody>
</table>

---

## 8. License

PyMapReduce is open-source software licensed under the [MIT License](LICENSE).
