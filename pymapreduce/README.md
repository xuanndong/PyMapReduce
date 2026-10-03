# PyMapReduce

<div align="center">

[![Version](https://img.shields.io/badge/version-0.1.13-blue.svg?style=flat-square)](https://pypi.org/project/pymapreduce-core/)
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
    # Start local cluster
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # Run tasks in parallel
    futures = [square.remote(i) for i in range(5)]
    results = await asyncio.gather(*futures)
    print("Parallel Squares:", results)  # [0, 1, 4, 9, 16]

    # Instantiate and call actor
    counter = await Counter.remote(start=10)
    print("Counter:", await counter.increment.remote(5))  # 15

    # Clean up actor
    await pymapreduce.kill(counter)

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
    - [2.3. Actor Memory Self-Protection](#23-actor-memory-self-protection)
    - [2.4. Automatic Code Shipping (`runtime_env`)](#24-automatic-code-shipping-runtime_env)
    - [2.5. Zero-Copy Shared Memory (`ObjectRef`)](#25-zero-copy-shared-memory-objectref)
    - [2.6. Distributed Datasets](#26-distributed-datasets)
    - [2.7. Job Execution Timeouts](#27-job-execution-timeouts)
  - [3. Cluster Deployment](#3-cluster-deployment)
    - [3.1. Local Mode (Embedded)](#31-local-mode-embedded)
    - [3.2. Multi-Server Production Cluster](#32-multi-server-production-cluster)
  - [4. Task Scheduling Strategies](#4-task-scheduling-strategies)
  - [5. Command Line Interface (CLI)](#5-command-line-interface-cli)
  - [6. Python API Reference](#6-python-api-reference)
    - [Top-Level Functions](#top-level-functions)
    - [Core Classes \& Methods](#core-classes--methods)
  - [7. Configuration \& Environment Variables](#7-configuration--environment-variables)
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
    res = await process_item.remote(10, 2.5)
    print("Single Result:", res)

    # 2. Parallel execution
    futures = [process_item.remote(i, 2.5) for i in range(10)]
    results = await asyncio.gather(*futures)
    print("Parallel Results:", results)

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.2. Distributed Actors (Stateful Classes)

Decorate a Python class with `@pymapreduce.remote` to create a **Stateful Actor**. The instance stays loaded in worker memory, preserving state across method calls:

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
    p1 = await model.predict.remote([1.0, 2.0, 3.0])
    p2 = await model.predict.remote([4.0, 5.0])
    stats = await model.get_stats.remote()

    print("Predictions:", p1, p2)
    print("Stats:", stats)

    # 3. Explicitly kill actor when done
    await pymapreduce.kill(model)

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.3. Actor Memory Self-Protection

To prevent memory leaks from abandoned actors and protect workers from running out of RAM (OOM), PyMapReduce provides built-in memory protection:

- **Idle Timeout (TTL)**: Automatically unload the actor instance if no calls are received for a specified duration.
- **Global LRU Eviction**: Evict the least recently used actor when the worker's actor limit is reached.
- **Lazy Re-creation**: If an evicted actor is called again, it transparently reconstructs itself.
- **RAM Safety Watchdog**: Proactively unloads idle actors and clears GPU VRAM cache when system RAM exceeds 85%.

```python
@pymapreduce.remote(idle_timeout=60.0)  # Auto-unload after 60s of inactivity
class HeavyService:
    def __init__(self):
        self.cache = {}

    def query(self, key: str) -> str:
        return self.cache.setdefault(key, f"val_{key}")

    def close(self):
        """Optional hook called when actor is unloaded or destroyed."""
        print("Releasing resources...")
```

---

### 2.4. Automatic Code Shipping (`runtime_env`)

When distributing code across multiple physical servers, use `runtime_env` to automatically package and synchronize your project folder (respecting `.gitignore`):

```python
import asyncio
import pymapreduce

async def main():
    # Connect and automatically package & sync local project files to all workers
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

    # 2. Pass reference directly into next task (zero serialization)
    summary = await compute_summary.remote(df_ref)
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
    ds = pymapreduce.from_items(range(1, 100_001), num_partitions=8)
    # Alternatively: ds = await pymapreduce.read_csv("data/*.csv", num_partitions=8)

    # 2. Lazy transformation pipeline
    transformed = (
        ds.map(lambda x: x * 2)
          .filter(lambda x: x % 4 == 0)
          .persist(pymapreduce.StorageLevel.MEMORY_ONLY)
    )

    # 3. Actions & Conversions
    count = await transformed.count()
    sample = await transformed.take(5)
    df = await transformed.to_pandas()

    print(f"Total: {count}, Sample: {sample}, DataFrame Rows: {len(df)}")

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 2.7. Job Execution Timeouts

Prevent hung jobs by configuring a watchdog execution timeout:

```python
import asyncio
import pymapreduce

async def main():
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # Set a maximum execution timeout of 30 seconds for this job
    config = pymapreduce.JobConfig("AnalyticsJob", max_time=30)
    
    # Submit job
    results = await driver.submit(config)
```

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

Deploy across multiple servers using the `mapreduce` CLI:

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
        Evaluates real-time network bandwidth (EMA), CPU load, and data locality. Best for production multi-node clusters.
      </td>
    </tr>
    <tr>
      <td><strong>LocalityFirst</strong></td>
      <td><code>SchedulerStrategy.LocalityFirst</code><br><code>--scheduler LocalityFirst</code></td>
      <td>
        Prioritizes dispatching tasks to the worker already holding required data in RAM to minimize network transfer.
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
        Weights assignment based on each worker's physical CPU cores and RAM size.
      </td>
    </tr>
    <tr>
      <td><strong>RoundRobin</strong></td>
      <td><code>SchedulerStrategy.RoundRobin</code><br><code>--scheduler RoundRobin</code></td>
      <td>
        Rotates tasks evenly across all available workers. Ideal for benchmark baselines.
      </td>
    </tr>
  </tbody>
</table>

---

## 5. Command Line Interface (CLI)

```bash
# Start cluster coordinator (HeadNode)
mapreduce start --head --port 7777 --scheduler Adaptive

# Connect a worker node
mapreduce start --worker --head-addr 127.0.0.1:7777 --cpus 8

# Submit a standalone script with a 60-second timeout
mapreduce submit pipeline.py --head-addr 127.0.0.1:7777 --max-time 60
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
      <td><code>head_addr, cpus=None, idle_timeout=0</code></td>
      <td>Launches a background worker connected to the cluster and returns a <code>WorkerHandle</code>.</td>
    </tr>
    <tr>
      <td><code>@pymapreduce.remote(...)</code></td>
      <td><code>cls_or_func=None, idle_timeout=None</code></td>
      <td>Decorator turning a function into a distributed task or a class into a stateful actor.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.kill(...)</code></td>
      <td><code>actor_handle</code></td>
      <td>Asynchronously purges an actor instance from worker memory and cluster state.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.from_items(...)</code></td>
      <td><code>items, num_partitions=None</code></td>
      <td>Creates a distributed <code>Dataset</code> from in-memory items.</td>
    </tr>
    <tr>
      <td><code>pymapreduce.read_csv(...)</code></td>
      <td><code>path_or_glob, num_partitions=None</code></td>
      <td>Creates a distributed <code>Dataset</code> by chunking a CSV file.</td>
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
      <td><code>map(fn)</code> / <code>filter(fn)</code> / <code>flat_map(fn)</code></td>
      <td>Applies transformations across distributed partitions.</td>
    </tr>
    <tr>
      <td><code>Dataset</code></td>
      <td><code>map_batches(fn, batch_format="pandas")</code></td>
      <td>Applies batch transformations using Pandas, PyArrow, or NumPy.</td>
    </tr>
    <tr>
      <td><code>Dataset</code></td>
      <td><code>count()</code> / <code>take(n)</code> / <code>collect()</code></td>
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
