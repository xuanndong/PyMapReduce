# PyMapReduce

PyMapReduce is a high-performance, asynchronous distributed computing framework built to effortlessly scale Python applications, data processing pipelines, and AI/ML workloads across multi-core machines and distributed clusters. By combining an ultra-fast execution engine written in Rust with Python's native `async`/`await` interface, PyMapReduce delivers high computational throughput without sacrificing developer ergonomics.

## Key Features

- **High-Throughput Rust Core**: Built entirely in Rust on top of the Tokio asynchronous runtime, utilizing lock-free data structures and zero-copy binary protocols for ultra-low latency task scheduling and dispatching.
- **Intuitive Python Async/Await API**: Seamlessly parallelize existing Python code using the simple `@pymapreduce.remote` decorator, fully integrated with standard Python `asyncio`.
- **Dual Execution Paradigms**: Complete support for both stateless distributed tasks (Map/Filter/Reduce) and stateful distributed actors that maintain their in-memory state across multiple remote method invocations.
- **Tiered Object Store**: Efficient memory management with an in-memory Hot Tier (RAM) and an automatic LRU Warm Tier (Disk spillover), enabling zero-copy sharing of massive datasets (`ObjectRef`) across workers.
- **Pluggable Dynamic Schedulers**: Flexible scheduling engine with multiple built-in strategies, including `Adaptive` (work stealing for heterogeneous workloads), `LocalityFirst` (data locality optimization), `LeastLoad`, `WeightedCapacity`, and `RoundRobin`.
- **Fault Recovery and Resilience**: Continuous heartbeat health monitoring, Write-Ahead Logging (WAL), and automated task re-execution to ensure robust execution even when worker nodes crash.

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
