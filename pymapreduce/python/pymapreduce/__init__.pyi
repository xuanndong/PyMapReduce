from typing import Any, List, Optional, Coroutine, Sequence, Callable
from .data import Dataset, StorageLevel

class TaskKind:
    Map: 'TaskKind'
    Reduce: 'TaskKind'
    
    def __eq__(self, other: Any) -> bool: ...

class TaskInput:
    def __init__(self, kind: TaskKind, payload: Any):
        """
        Creates a new task input with a specific kind and payload.
        The payload can be any pickle-able Python object.
        """
        pass

class JobConfig:
    def __init__(self, name: str, max_time: Optional[int] = None):
        """
        Creates a new Job Configuration with the given name and optional max execution time limit (seconds).
        """
        pass

    def add_task(self, task: TaskInput) -> None:
        """
        Appends a task to the job configuration.
        """
        pass

    def with_max_time(self, seconds: int) -> None:
        """
        Sets a maximum execution time limit for the job in seconds.
        """
        pass

class WorkerHandle:
    def disconnect(self, graceful: bool = True) -> Coroutine[Any, Any, None]:
        """
        Disconnects the worker from the cluster with optional graceful drain.
        """
        pass

class ObjectRef:
    object_id: str
    def __init__(self, object_id: str, pinned: bool = False): ...
    def pin(self) -> 'ObjectRef': ...
    def unpin(self) -> 'ObjectRef': ...

class SchedulerStrategy:
    RoundRobin: 'SchedulerStrategy'
    WeightedCapacity: 'SchedulerStrategy'
    LeastLoad: 'SchedulerStrategy'
    LocalityFirst: 'SchedulerStrategy'
    Adaptive: 'SchedulerStrategy'

class Driver:
    @staticmethod
    def init(address: Optional[str] = None, scheduler: Optional[SchedulerStrategy] = None) -> Coroutine[Any, Any, 'Driver']:
        """
        Asynchronously initializes a local cluster (HeadNode + Worker) and returns a connected Driver.
        If address is provided, it binds to that address. Otherwise, defaults to 127.0.0.1:7777.
        If scheduler is None, defaults to SchedulerStrategy.RoundRobin.
        """
        pass

    @staticmethod
    def connect(head_node: str, workers: Optional[List[str]] = None) -> Coroutine[Any, Any, 'Driver']:
        """
        Asynchronously connects to the HeadNode at the specified address (e.g. '127.0.0.1:7777').
        Optionally takes a list of worker addresses to register with the cluster.
        """
        pass

    def submit(self, config: JobConfig) -> Coroutine[Any, Any, List[Any]]:
        """
        Submits a job to the cluster and awaits the results.
        Returns a list of unpickled Python objects representing the task outputs.
        """
        pass

    def get_job_status(self, job_id: str) -> Coroutine[Any, Any, Optional[tuple[int, int]]]:
        """
        Queries the current progress (completed_tasks, total_tasks) of a job.
        """
        pass

    def put(self, object_id: str, data: bytes) -> Coroutine[Any, Any, None]: ...
    def get(self, object_id: str) -> Coroutine[Any, Any, Any]: ...

def connect_worker(head_addr: str, cpus: Optional[int] = None, idle_timeout: int = 0) -> Coroutine[Any, Any, WorkerHandle]:
    """
    Connects a new worker to the cluster and returns a WorkerHandle for lifecycle control.
    """
    pass

def start_worker(head_addr: str, workers: Optional[int] = None, idle_timeout: int = 0) -> Coroutine[Any, Any, None]:
    """
    Connects to an existing HeadNode and starts accepting tasks.
    By default, uses all available CPU cores.
    If idle_timeout > 0, kills idle python processes after that many minutes.
    """
    pass

async def init(
    address: Optional[str] = None,
    runtime_env: Optional[dict] = None,
    scheduler: Optional[SchedulerStrategy] = None,
) -> Driver:
    """
    Asynchronously initializes a local cluster (HeadNode + Worker) and returns a connected Driver.
    - If address is None: defaults to 127.0.0.1:7777.
    - If scheduler is None: defaults to SchedulerStrategy.RoundRobin.
    - runtime_env: optional dictionary specifying working_dir or environment config.
    """
    ...

async def connect(head_node: str, workers: Optional[List[str]] = None, runtime_env: Optional[dict] = None) -> Driver: ...
def remote(cls_or_func: Any, *, idle_timeout: Optional[float] = None) -> Any: ...
async def kill(actor_handle: Any) -> bool: ...

def from_blocks(blocks: Sequence[ObjectRef]) -> Dataset: ...
async def from_items(items: Sequence[Any], num_partitions: Optional[int] = None) -> Dataset: ...
async def from_pandas(df: Any, num_partitions: Optional[int] = None) -> Dataset: ...
async def from_numpy(arr: Any, num_partitions: Optional[int] = None) -> Dataset: ...
async def read_csv(path_or_glob: str, num_partitions: Optional[int] = None) -> Dataset: ...


