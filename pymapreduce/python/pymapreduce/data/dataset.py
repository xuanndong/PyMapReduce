"""Distributed Dataset abstraction for high-throughput Data Parallelism."""

from __future__ import annotations

import asyncio
import os
import uuid
from enum import Enum
from typing import Any, Callable, Iterable, List, Optional, Sequence, Union

try:
    import cloudpickle as pickle
except ImportError:
    import pickle

import pymapreduce


class StorageLevel(Enum):
    """Storage levels for persisting datasets."""
    MEMORY_ONLY = "MEMORY_ONLY"
    MEMORY_AND_DISK = "MEMORY_AND_DISK"
    DISK_ONLY = "DISK_ONLY"


# Remote task execution helpers for Dataset transformations

@pymapreduce.remote
def _block_map_batches(block: Any, fn: Callable[[Any], Any], batch_format: str) -> Any:
    """Transform a single partition block using a batch-oriented function."""
    # Convert block if format transformation is requested
    if batch_format == "pandas":
        import pandas as pd
        if hasattr(block, "to_pandas"):
            block = block.to_pandas()
        elif not isinstance(block, pd.DataFrame) and isinstance(block, (list, dict)):
            block = pd.DataFrame(block)

    elif batch_format == "arrow":
        try:
            import pyarrow as pa
            if not isinstance(block, (pa.Table, pa.RecordBatch)):
                if hasattr(block, "to_numpy") or isinstance(block, dict):
                    block = pa.Table.from_pydict(block)
        except ImportError:
            pass

    elif batch_format == "numpy":
        import numpy as np
        if not isinstance(block, np.ndarray):
            block = np.asarray(block)

    # Execute user transformation
    return fn(block)


@pymapreduce.remote
def _block_filter(block: Any, pred_fn: Callable[[Any], bool]) -> Any:
    """Filter records within a single block."""
    import pandas as pd

    if isinstance(block, pd.DataFrame):
        mask = block.apply(pred_fn, axis=1)
        return block[mask]

    try:
        import pyarrow as pa
        if isinstance(block, (pa.Table, pa.RecordBatch)):
            df = block.to_pandas()
            mask = df.apply(pred_fn, axis=1)
            filtered_df = df[mask]
            return pa.Table.from_pandas(filtered_df)
    except ImportError:
        pass

    if isinstance(block, (list, tuple)):
        return [item for item in block if pred_fn(item)]

    return block


@pymapreduce.remote
def _block_count(block: Any) -> int:
    """Count number of items or rows inside a single block."""
    if hasattr(block, "__len__"):
        return len(block)
    if hasattr(block, "num_rows"):
        return block.num_rows
    return len(list(block))


@pymapreduce.remote
def _read_csv_chunk(
    file_path: str,
    skip_rows: int,
    num_rows: int,
    columns: Optional[List[str]] = None,
) -> Any:
    """Read a specific chunk of a CSV file as an Arrow Table or Pandas DataFrame."""
    try:
        import pyarrow.csv as pv
        # Read with PyArrow CSV reader for zero-copy memory layout
        opts = pv.ReadOptions(skip_rows=skip_rows, block_size=16 * 1024 * 1024)
        parse_opts = pv.ParseOptions()
        convert_opts = pv.ConvertOptions(include_columns=columns) if columns else None
        return pv.read_csv(file_path, read_options=opts, parse_options=parse_opts, convert_options=convert_opts)
    except Exception:
        import pandas as pd
        header = 0 if skip_rows == 0 else None
        return pd.read_csv(file_path, skiprows=skip_rows, nrows=num_rows, names=columns, header=header)


class Dataset:
    """A distributed collection of partition blocks across the cluster."""

    def __init__(self, blocks: Sequence[pymapreduce.ObjectRef]):
        self._blocks = list(blocks)
        self._persisted = False
        self._storage_level: Optional[StorageLevel] = None

    @property
    def blocks(self) -> List[pymapreduce.ObjectRef]:
        """Return references to all distributed partition blocks."""
        return self._blocks

    @property
    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return len(self._blocks)

    def _get_driver(self):
        """Retrieve the global Driver instance."""
        driver = getattr(pymapreduce, "_global_driver", None)
        if driver is None:
            raise RuntimeError("pymapreduce cluster is not initialized. Call pymapreduce.init() first.")
        return driver

    # Transformations (Lazy / Parallel Execution)

    async def map_batches(
        self,
        fn: Callable[[Any], Any],
        batch_format: str = "native",
    ) -> Dataset:
        """Apply a batch-oriented transformation function to each partition block."""
        if not self._blocks:
            return Dataset([])

        futures = [
            _block_map_batches.remote(block, fn, batch_format)
            for block in self._blocks
        ]
        new_blocks = await asyncio.gather(*futures)
        return Dataset(new_blocks)

    async def filter(self, pred_fn: Callable[[Any], bool]) -> Dataset:
        """Filter records across all partition blocks using a predicate function."""
        if not self._blocks:
            return Dataset([])

        futures = [
            _block_filter.remote(block, pred_fn)
            for block in self._blocks
        ]
        new_blocks = await asyncio.gather(*futures)
        return Dataset(new_blocks)

    async def map(self, fn: Callable[[Any], Any]) -> Dataset:
        """Apply an element-wise mapping function across all elements in the dataset."""
        def _map_wrapper(batch):
            if hasattr(batch, "__iter__") and not isinstance(batch, (str, bytes, dict)):
                return [fn(x) for x in batch]
            return fn(batch)

        return await self.map_batches(_map_wrapper, batch_format="native")

    async def repartition(self, num_partitions: int) -> Dataset:
        """Reshuffle or re-chunk the dataset into a target number of partitions."""
        if num_partitions <= 0:
            raise ValueError(f"num_partitions must be positive, got {num_partitions}")

        if num_partitions == len(self._blocks):
            return self

        # Collect raw items and redistribute
        items = await self.collect()
        return await Dataset.from_items(items, num_partitions=num_partitions)

    def persist(self, level: StorageLevel = StorageLevel.MEMORY_ONLY) -> Dataset:
        """Mark dataset blocks to be kept in cluster memory and pinned from GC."""
        self._persisted = True
        self._storage_level = level
        for block in self._blocks:
            if hasattr(block, "pin"):
                block.pin()
        return self

    def unpersist(self) -> Dataset:
        """Unpin dataset blocks and allow GC reclamation when out of scope."""
        self._persisted = False
        self._storage_level = None
        for block in self._blocks:
            if hasattr(block, "unpin"):
                block.unpin()
        return self

    # Actions & Materialization

    async def count(self) -> int:
        """Return the total number of records across all partitions."""
        if not self._blocks:
            return 0

        futures = [_block_count.remote(block) for block in self._blocks]
        counts = await asyncio.gather(*futures)
        return sum(counts)

    async def collect(self) -> List[Any]:
        """Gather and concatenate all partition blocks into a local Python list."""
        if not self._blocks:
            return []

        driver = self._get_driver()
        fetch_tasks = [driver.get(block.object_id) for block in self._blocks]
        raw_blocks = await asyncio.gather(*fetch_tasks)

        results = []
        for block in raw_blocks:
            if hasattr(block, "to_pylist"):
                results.extend(block.to_pylist())
            elif hasattr(block, "to_dict"):
                results.extend(block.to_dict(orient="records"))
            elif isinstance(block, (list, tuple)):
                results.extend(block)
            else:
                results.append(block)

        return results

    async def to_pandas(self):
        """Concatenate all distributed partition blocks into a single pandas DataFrame."""
        import pandas as pd

        if not self._blocks:
            return pd.DataFrame()

        driver = self._get_driver()
        fetch_tasks = [driver.get(block.object_id) for block in self._blocks]
        raw_blocks = await asyncio.gather(*fetch_tasks)

        dfs = []
        for block in raw_blocks:
            if isinstance(block, pd.DataFrame):
                dfs.append(block)
            elif hasattr(block, "to_pandas"):
                dfs.append(block.to_pandas())
            else:
                dfs.append(pd.DataFrame(block))

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    async def to_arrow(self):
        """Concatenate all distributed partition blocks into a single PyArrow Table."""
        import pyarrow as pa

        if not self._blocks:
            return pa.Table.from_batches([])

        driver = self._get_driver()
        fetch_tasks = [driver.get(block.object_id) for block in self._blocks]
        raw_blocks = await asyncio.gather(*fetch_tasks)

        tables = []
        for block in raw_blocks:
            if isinstance(block, pa.Table):
                tables.append(block)
            elif hasattr(block, "to_arrow"):
                tables.append(block.to_arrow())
            else:
                tables.append(pa.Table.from_pandas(block))

        if not tables:
            return pa.Table.from_batches([])

        return pa.concat_tables(tables)

    async def take(self, n: int) -> List[Any]:
        """Fetch the first n records of the dataset."""
        if n <= 0 or not self._blocks:
            return []

        collected = []
        driver = self._get_driver()

        for block in self._blocks:
            data = await driver.get(block.object_id)
            if hasattr(data, "to_pylist"):
                items = data.to_pylist()
            elif hasattr(data, "to_dict"):
                items = data.to_dict(orient="records")
            elif isinstance(data, (list, tuple)):
                items = list(data)
            else:
                items = [data]

            collected.extend(items)
            if len(collected) >= n:
                return collected[:n]

        return collected

    async def show(self, n: int = 10) -> None:
        """Print the first n records in tabular or formatted style."""
        items = await self.take(n)
        for idx, item in enumerate(items):
            print(f"[{idx}] {item}")

    # Factory Ingestion Methods

    @classmethod
    def from_blocks(cls, blocks: Sequence[pymapreduce.ObjectRef]) -> Dataset:
        """Create a Dataset directly from existing ObjectRef blocks."""
        return cls(blocks)

    @classmethod
    async def from_items(
        cls,
        items: Sequence[Any],
        num_partitions: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dataset:
        """Create a Dataset by partitioning an in-memory sequence of Python items."""
        total_items = len(items)
        if total_items == 0:
            return cls([])

        if batch_size is not None and batch_size > 0:
            partitions = [items[i : i + batch_size] for i in range(0, total_items, batch_size)]
        else:
            k = num_partitions or max(1, os.cpu_count() or 4)
            chunk_size = max(1, (total_items + k - 1) // k)
            partitions = [items[i : i + chunk_size] for i in range(0, total_items, chunk_size)]

        driver = getattr(pymapreduce, "_global_driver", None)
        if driver is None:
            raise RuntimeError("pymapreduce cluster is not initialized.")

        blocks = []
        for part in partitions:
            obj_id = str(uuid.uuid4())
            data_bytes = pickle.dumps(part)
            await driver.put(obj_id, data_bytes)
            blocks.append(pymapreduce.ObjectRef(obj_id))

        return cls(blocks)

    @classmethod
    async def from_pandas(
        cls,
        df: Any,
        num_partitions: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dataset:
        """Create a Dataset by partitioning a pandas DataFrame across the cluster."""
        total_rows = len(df)
        if total_rows == 0:
            return cls([])

        if batch_size is not None and batch_size > 0:
            chunks = [df.iloc[i : i + batch_size] for i in range(0, total_rows, batch_size)]
        else:
            k = num_partitions or max(1, os.cpu_count() or 4)
            chunk_size = max(1, (total_rows + k - 1) // k)
            chunks = [df.iloc[i : i + chunk_size] for i in range(0, total_rows, chunk_size)]

        driver = getattr(pymapreduce, "_global_driver", None)
        if driver is None:
            raise RuntimeError("pymapreduce cluster is not initialized.")

        blocks = []
        for chunk in chunks:
            obj_id = str(uuid.uuid4())
            # Try serializing as PyArrow IPC for zero-copy if pyarrow is installed
            try:
                import pyarrow as pa
                table = pa.Table.from_pandas(chunk)
                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, table.schema) as writer:
                    writer.write_table(table)
                data_bytes = sink.getvalue().to_pybytes()
            except Exception:
                data_bytes = pickle.dumps(chunk)

            await driver.put(obj_id, data_bytes)
            blocks.append(pymapreduce.ObjectRef(obj_id))

        return cls(blocks)

    @classmethod
    async def from_numpy(
        cls,
        arr: Any,
        num_partitions: Optional[int] = None,
    ) -> Dataset:
        """Create a Dataset by partitioning a NumPy array."""
        total_len = len(arr)
        if total_len == 0:
            return cls([])

        k = num_partitions or max(1, os.cpu_count() or 4)
        chunk_size = max(1, (total_len + k - 1) // k)
        chunks = [arr[i : i + chunk_size] for i in range(0, total_len, chunk_size)]

        driver = getattr(pymapreduce, "_global_driver", None)
        if driver is None:
            raise RuntimeError("pymapreduce cluster is not initialized.")

        blocks = []
        for chunk in chunks:
            obj_id = str(uuid.uuid4())
            data_bytes = pickle.dumps(chunk)
            await driver.put(obj_id, data_bytes)
            blocks.append(pymapreduce.ObjectRef(obj_id))

        return cls(blocks)

    @classmethod
    async def read_csv(
        cls,
        file_path_or_glob: str,
        num_partitions: Optional[int] = None,
        chunksize: Optional[int] = None,
    ) -> Dataset:
        """Read a CSV dataset and partition it across the cluster in parallel."""
        import glob
        import pandas as pd

        files = glob.glob(file_path_or_glob)
        if not files:
            files = [file_path_or_glob]

        if len(files) > 1:
            # Multi-file dataset: each file is a partition block
            @pymapreduce.remote
            def _load_file(f):
                return pd.read_csv(f)

            futures = [_load_file.remote(f) for f in files]
            blocks = await asyncio.gather(*futures)
            return cls(blocks)

        # Single file dataset: chunk rows
        single_file = files[0]
        if not os.path.exists(single_file):
            raise FileNotFoundError(f"File not found: {single_file}")

        # Count total rows quickly
        with open(single_file, "r") as f:
            total_lines = sum(1 for _ in f) - 1  # Exclude header

        if total_lines <= 0:
            return cls([])

        k = num_partitions or max(1, os.cpu_count() or 4)
        step = chunksize or max(1, (total_lines + k - 1) // k)

        futures = []
        for start_row in range(0, total_lines, step):
            count = min(step, total_lines - start_row)
            fut = _read_csv_chunk.remote(single_file, start_row, count)
            futures.append(fut)

        blocks = await asyncio.gather(*futures)
        return cls(blocks)


# Convenient top-level functions

def from_blocks(blocks: Sequence[pymapreduce.ObjectRef]) -> Dataset:
    return Dataset.from_blocks(blocks)

async def from_items(items: Sequence[Any], num_partitions: Optional[int] = None) -> Dataset:
    return await Dataset.from_items(items, num_partitions=num_partitions)

async def from_pandas(df: Any, num_partitions: Optional[int] = None) -> Dataset:
    return await Dataset.from_pandas(df, num_partitions=num_partitions)

async def from_numpy(arr: Any, num_partitions: Optional[int] = None) -> Dataset:
    return await Dataset.from_numpy(arr, num_partitions=num_partitions)

async def read_csv(path_or_glob: str, num_partitions: Optional[int] = None) -> Dataset:
    return await Dataset.read_csv(path_or_glob, num_partitions=num_partitions)
