# PyMapReduce

PyMapReduce is a high-performance distributed computing framework designed around the core principles of the **MapReduce programming model** — partitioning complex data workloads into concurrent **Map** (distributed transformation) and **Reduce** (distributed aggregation) operations.

The framework modernizes distributed processing by integrating a lightweight, asynchronous execution engine implemented in **Rust** with an asynchronous **Python** interface. It expands beyond traditional batch execution by offering native `asyncio` task scheduling, long-lived stateful actors, and tiered zero-copy shared memory to support modern data engineering pipelines and AI/ML workloads across multi-core systems and server clusters.

## Key Features

- **Rust Execution Engine**: The control plane and network protocol layer are built in Rust on the Tokio asynchronous runtime, delivering high task throughput, low-latency node coordination, and minimal resource utilization.
- **Asynchronous Python API**: Standard Python functions and classes can be converted into distributed tasks via the `@pymapreduce.remote` decorator, providing native interoperability with the Python `asyncio` event loop without requiring manual thread or process management.
- **Dual Computation Models**:
  - *Stateless Tasks*: Pure computational functions executed independently across worker nodes with isolated inputs and outputs.
  - *Stateful Actors*: Long-lived class instances maintained in worker memory across multiple remote method invocations, avoiding repetitive state reloading for models, cache stores, or connection pools.
- **Tiered Object Store**: Large intermediate results (such as Pandas DataFrames or NumPy arrays) are held in shared memory (RAM Hot Tier) and automatically spilled to disk via memory mapping (Warm Tier) when capacity thresholds are reached. Downstream tasks retrieve data by reference (`ObjectRef`) via peer-to-peer transfers, preventing coordinator bottlenecks.
- **Pluggable Scheduling Algorithms**: Configurable scheduling strategies tailored to diverse cluster environments:
  - `Adaptive`: Balances cluster workload dynamically with work-stealing for uneven tasks.
  - `LeastLoad`: Routes tasks to the worker with the lowest active task count.
  - `LocalityFirst`: Prioritizes workers already holding required input data in memory.
  - `WeightedCapacity`: Allocates tasks proportionally based on node hardware capacity (CPU/RAM).
  - `RoundRobin`: Cyclic task distribution across available nodes.
- **Fault Recovery and State Persistence**: Periodic heartbeat monitoring continuously validates worker node health, automatically re-dispatching unfinished tasks upon node failure. Cluster state changes are journaled via a Write-Ahead Log (WAL) to ensure reliable state recovery.

## Installation

```bash
pip install pymapreduce-core
```

## Quickstart

```python
import asyncio
import pymapreduce

# Define a distributed task
@pymapreduce.remote
def square(x: int) -> int:
    return x * x

# Define a stateful distributed actor
@pymapreduce.remote
class Counter:
    def __init__(self, initial_value: int = 0):
        self.value = initial_value

    def increment(self, amount: int = 1) -> int:
        self.value += amount
        return self.value

async def main():
    # Initialize local cluster
    driver = await pymapreduce.init()
    await pymapreduce.connect_worker("127.0.0.1:7777", cpus=4)

    # Execute parallel tasks
    tasks = [square.remote(i) for i in range(5)]
    results = await asyncio.gather(*tasks)
    print("Task Results:", results)  # [0, 1, 4, 9, 16]

    # Interact with actor
    counter = await Counter.remote(initial_value=10)
    current = await counter.increment.remote(5)
    print("Counter Value:", current)  # 15

if __name__ == "__main__":
    asyncio.run(main())
```

## Cluster Deployment

For distributed execution across multiple machines, use the `mapreduce` CLI:

#### Start Head Node

The Head Node manages the global cluster state, task dispatching, and worker health monitoring:

```bash
mapreduce start --head --port 7777 --scheduler Adaptive
```

Options:
- `--port <PORT>`: Port for Head Node service (default: `7777`).
- `--scheduler <STRATEGY>`: Scheduling algorithm (`RoundRobin`, `WeightedCapacity`, `LeastLoad`, `LocalityFirst`, `Adaptive`). Default: `RoundRobin`.

#### Start Worker Node

Worker Nodes connect to the Head Node and execute assigned computational tasks:

```bash
mapreduce start --worker --head-addr 192.168.1.100:7777 --cpus 4
```

Options:
- `--head-addr <IP:PORT>`: Network address of the Head Node (default: `127.0.0.1:7777`).
- `--cpus <N>`: Maximum CPU cores allocated for this worker (default: all available cores).
- `--idle-timeout <MINUTES>`: Worker process idle lifetime in minutes before exit (default: `0` / never).

## Docker Compose

A multi-container setup is available via `docker-compose.yml` to run the Head Node and Worker Nodes in isolated environments:

#### Start Services

```bash
docker compose up
```

#### Scale Worker Containers

```bash
docker compose up --scale workernode=3
```

#### Environment Configuration

Configure cluster parameters via environment variables:

```bash
HEAD_PORT=7777 SCHEDULER=Adaptive WORKER_CPUS=4 docker compose up
```
