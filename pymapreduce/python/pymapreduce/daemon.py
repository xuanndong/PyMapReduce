import sys
import os
import gc
import time
import struct
import mmap
import traceback

# Import serialization library
try:
    import cloudpickle as pickle
except ImportError:
    import pickle

import pymapreduce

# Global store for active actors and metadata within this worker process
_ACTORS = {}
_ACTOR_META = {}

MAX_ACTORS_PER_WORKER = int(os.environ.get("PYMAPREDUCE_MAX_ACTORS_PER_WORKER", "32"))
MAX_ACTOR_META = int(os.environ.get("PYMAPREDUCE_MAX_ACTOR_META", "1024"))
DEFAULT_ACTOR_IDLE_TIMEOUT = float(os.environ.get("PYMAPREDUCE_ACTOR_IDLE_TIMEOUT", "0"))
MEMORY_PRESSURE_THRESHOLD = float(os.environ.get("PYMAPREDUCE_MEMORY_THRESHOLD_PERCENT", "85.0"))


def _teardown_instance(instance):
    """Gracefully call lifecycle termination hooks on an actor instance."""
    if instance is None:
        return
    for method_name in ("close", "destroy", "cleanup"):
        if hasattr(instance, method_name):
            try:
                hook = getattr(instance, method_name)
                if callable(hook):
                    hook()
            except Exception:
                pass


def _collect_garbage():
    """Run Python garbage collector and release OS/GPU memory pools."""
    gc.collect()

    # 1. Release PyTorch CUDA memory cache if PyTorch is loaded
    if "torch" in sys.modules:
        try:
            torch = sys.modules["torch"]
            if hasattr(torch, "cuda") and torch.cuda.is_available():
                torch.cuda.empty_cache()
                if hasattr(torch.cuda, "ipc_collect"):
                    torch.cuda.ipc_collect()
        except Exception:
            pass

    # 2. Release glibc free memory back to the OS on Linux
    try:
        import ctypes
        libc = ctypes.CDLL(None)
        if hasattr(libc, "malloc_trim"):
            libc.malloc_trim(0)
    except Exception:
        pass


def _unload_actor(actor_id: str, remove_meta: bool = False) -> bool:
    """Unload an actor instance from active memory and optionally remove its metadata."""
    instance = _ACTORS.pop(actor_id, None)
    if remove_meta:
        _ACTOR_META.pop(actor_id, None)

    if instance is not None:
        _teardown_instance(instance)
        return True
    return False


def _get_system_memory_percent() -> float:
    """Get current system memory usage percentage on Linux."""
    try:
        with open("/proc/meminfo", "r") as f:
            mem = {}
            for _ in range(10):
                line = f.readline()
                if not line:
                    break
                parts = line.split(":")
                if len(parts) == 2:
                    mem[parts[0].strip()] = int(parts[1].split()[0])
            total = mem.get("MemTotal", 0)
            avail = mem.get("MemAvailable", 0)
            if total > 0:
                return (1.0 - (avail / total)) * 100.0
    except Exception:
        pass
    return 0.0


def _sweep_idle_actors():
    """Unload instances of actors that have exceeded their idle timeout."""
    now = time.time()
    unloaded_count = 0
    for actor_id, meta in list(_ACTOR_META.items()):
        timeout = meta.get("idle_timeout") or 0
        if timeout > 0 and (now - meta.get("last_accessed", now)) > timeout:
            if actor_id in _ACTORS:
                if _unload_actor(actor_id, remove_meta=False):
                    unloaded_count += 1

    if unloaded_count > 0:
        _collect_garbage()


def _evict_lru_actor() -> bool:
    """Evict the least recently used active actor instance from memory."""
    if not _ACTORS:
        return False
    victim_id = min(
        _ACTORS.keys(),
        key=lambda aid: _ACTOR_META.get(aid, {}).get("last_accessed", 0)
    )
    if _unload_actor(victim_id, remove_meta=False):
        _collect_garbage()
        return True
    return False


def _enforce_actor_capacity():
    """Enforce actor quota, metadata bounding, and memory pressure limits."""
    # 1. Enforce max active actors per worker quota
    while len(_ACTORS) >= MAX_ACTORS_PER_WORKER and _ACTORS:
        if not _evict_lru_actor():
            break

    # 2. Proactive memory-pressure eviction (if RAM > threshold)
    if MEMORY_PRESSURE_THRESHOLD > 0:
        while _get_system_memory_percent() > MEMORY_PRESSURE_THRESHOLD and _ACTORS:
            if not _evict_lru_actor():
                break

    # 3. Bound metadata store to prevent unbounded leak
    if len(_ACTOR_META) > MAX_ACTOR_META:
        inactive_ids = [aid for aid in _ACTOR_META if aid not in _ACTORS]
        if inactive_ids:
            oldest_meta_id = min(
                inactive_ids,
                key=lambda aid: _ACTOR_META.get(aid, {}).get("last_accessed", 0)
            )
            _ACTOR_META.pop(oldest_meta_id, None)


def _load_shm_dependency(shm_path: str):
    """Load object from shared memory file (/dev/shm) with zero-copy where possible."""
    if not os.path.exists(shm_path):
        return None

    with open(shm_path, "rb") as f:
        file_size = os.fstat(f.fileno()).st_size
        if file_size == 0:
            return None

        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        # 1. Try loading as PyArrow Table/RecordBatch
        try:
            import pyarrow as pa
            try:
                reader = pa.ipc.open_stream(mm)
                return reader.read_all()
            except Exception:
                reader = pa.ipc.open_file(mm)
                return reader.read_all()
        except Exception:
            pass

        # 2. Try loading as NumPy Array via memory map
        try:
            import numpy as np
            if shm_path.endswith(".npy") or mm[:6] == b"\x93NUMPY":
                return np.load(shm_path, mmap_mode="r")
        except Exception:
            pass

        # 3. Try loading via pickle from memory map
        try:
            return pickle.loads(mm)
        except Exception:
            pass

        # 4. Fallback to raw bytes
        return mm.read()


def _resolve_args(obj, py_deps):
    """Recursively replace ObjectRef instances with their loaded dependency data."""
    if isinstance(obj, pymapreduce.ObjectRef):
        return py_deps.get(obj.object_id, obj)

    if isinstance(obj, list):
        return [_resolve_args(x, py_deps) for x in obj]

    if isinstance(obj, tuple):
        # Handle serialized ObjectRef tuple: ("__ObjectRef__", "uuid")
        if len(obj) == 2 and obj[0] == "__ObjectRef__":
            return py_deps.get(obj[1], obj)
        return tuple(_resolve_args(x, py_deps) for x in obj)

    if isinstance(obj, dict):
        return {k: _resolve_args(v, py_deps) for k, v in obj.items()}

    return obj


def process_task(envelope):
    """Execute a task received from the Rust Worker Core."""
    kind = envelope.get("kind")
    payload = envelope.get("payload")
    deps_bytes = envelope.get("deps", {})
    dep_shm = envelope.get("dep_shm", {})

    py_deps = {}

    # 1. Load dependencies from Shared Memory (/dev/shm)
    for dep_id, shm_path in dep_shm.items():
        try:
            loaded = _load_shm_dependency(shm_path)
            if loaded is not None:
                py_deps[dep_id] = loaded
        except Exception as e:
            sys.stderr.write(f"[Worker Daemon] Failed to load SHM dep {dep_id}: {e}\n")
            sys.stderr.flush()

    # 2. Fallback to direct bytes if not already resolved via SHM
    for dep_id, dep_data in deps_bytes.items():
        if dep_id in py_deps:
            continue
        try:
            py_deps[dep_id] = pickle.loads(dep_data)
        except Exception as e:
            sys.stderr.write(f"[Worker Daemon] Failed to deserialize dep {dep_id}: {e}\n")
            sys.stderr.flush()

    # 3. Handle Actor Creation
    if kind == "ActorCreation":
        _sweep_idle_actors()
        actor_id = envelope.get("actor_id")
        unpacked = pickle.loads(payload)
        
        if len(unpacked) == 4:
            cls, args, kwargs, idle_timeout = unpacked
            idle_timeout = idle_timeout or DEFAULT_ACTOR_IDLE_TIMEOUT or 0
        else:
            cls, args, kwargs = unpacked
            idle_timeout = DEFAULT_ACTOR_IDLE_TIMEOUT or 0

        _enforce_actor_capacity()

        instance = cls(*args, **kwargs)
        _ACTORS[actor_id] = instance
        _ACTOR_META[actor_id] = {
            "cls": cls,
            "args": args,
            "kwargs": kwargs,
            "idle_timeout": idle_timeout,
            "last_accessed": time.time(),
        }
        return pickle.dumps(None)

    # 4. Handle Actor Task
    if kind == "ActorTask":
        _sweep_idle_actors()
        actor_id = envelope.get("actor_id")
        method_name = envelope.get("method_name")

        instance = _ACTORS.get(actor_id)
        if instance is None:
            # Lazy Re-creation if metadata exists
            if actor_id in _ACTOR_META:
                _enforce_actor_capacity()
                meta = _ACTOR_META[actor_id]
                instance = meta["cls"](*meta["args"], **meta["kwargs"])
                _ACTORS[actor_id] = instance
            else:
                raise RuntimeError(f"Actor {actor_id} is dead or not found in this worker process.")

        _ACTOR_META[actor_id]["last_accessed"] = time.time()

        args, kwargs = pickle.loads(payload)
        resolved_args = _resolve_args(args, py_deps)
        resolved_kwargs = _resolve_args(kwargs, py_deps)

        method = getattr(instance, method_name)
        result = method(*resolved_args, **resolved_kwargs)
        return pickle.dumps(result)

    # 5. Handle Actor Destruction
    if kind == "ActorDestruction":
        actor_id = envelope.get("actor_id")
        _unload_actor(actor_id, remove_meta=True)
        _collect_garbage()
        return pickle.dumps(True)

    # 6. Handle Stateless Task (Map / Reduce)
    func, args = pickle.loads(payload)
    resolved_args = _resolve_args(args, py_deps)
    result = func(*resolved_args)
    return pickle.dumps(result)


def read_exactly(stream, n):
    """Read exactly n bytes from stream, handling partial reads."""
    buf = bytearray(n)
    view = memoryview(buf)
    pos = 0
    while pos < n:
        read_bytes = stream.readinto(view[pos:])
        if read_bytes == 0:
            return None  # Connection closed (EOF)
        pos += read_bytes
    return bytes(buf)


def main():
    # Keep dedicated binary streams for IPC communication with Rust Worker Core
    ipc_in = sys.stdin.buffer
    ipc_out = sys.stdout.buffer

    # Redirect user stdout to sys.stderr so print() statements from tasks/actors
    # never corrupt the binary IPC message framing
    sys.stdout = sys.stderr

    while True:
        try:
            # 1. Read 4-byte length prefix
            length_bytes = read_exactly(ipc_in, 4)
            if length_bytes is None:
                break  # Connection closed cleanly

            msg_len = struct.unpack(">I", length_bytes)[0]

            # 2. Read exact payload bytes
            msg_bytes = read_exactly(ipc_in, msg_len)
            if msg_bytes is None:
                break

            # 3. Process task and send result
            try:
                envelope = pickle.loads(msg_bytes)
                result_bytes = process_task(envelope)

                out_len = struct.pack(">I", len(result_bytes))
                ipc_out.write(b"\x00" + out_len + result_bytes)
                ipc_out.flush()

                # Trigger GC to free temporary intermediate allocations
                if len(result_bytes) > 1024 * 1024 or envelope.get("dep_shm"):
                    gc.collect()

            except Exception:
                err_str = traceback.format_exc().encode("utf-8")
                out_len = struct.pack(">I", len(err_str))
                ipc_out.write(b"\x01" + out_len + err_str)
                ipc_out.flush()

        except Exception as critical_e:
            sys.stderr.write(f"[Worker Daemon Critical Error] {critical_e}\n")
            sys.stderr.flush()
            break


if __name__ == "__main__":
    main()
