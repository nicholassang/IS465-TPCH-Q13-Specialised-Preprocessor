from pathlib import Path
from typing import Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq


def iter_record_batches(
    parquet_path: Path,
    columns: Sequence[str],
    batch_size: int = 65536,
) -> Iterator[pa.RecordBatch]:
    """
    Stream Parquet data as Arrow record batches, reading only requested columns.
    """
    parquet_file = pq.ParquetFile(parquet_path)
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=list(columns)):
        yield batch


def get_max_int_column(parquet_path: Path, column: str, batch_size: int = 65536) -> int:
    """
    Scan one integer column and return its max value.
    Used to size dense arrays safely.
    """
    max_value = -1
    for batch in iter_record_batches(parquet_path, [column], batch_size=batch_size):
        arr = batch.column(0).to_pylist()
        if arr:
            local_max = max(arr)
            if local_max > max_value:
                max_value = local_max
    return max_value