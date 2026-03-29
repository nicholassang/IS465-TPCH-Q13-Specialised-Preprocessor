from pathlib import Path
from typing import Iterator, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def iter_record_batches(
    parquet_path: Path,
    columns: Sequence[str],
    batch_size: int = 262144,
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
    parquet_file = pq.ParquetFile(parquet_path)
    column_index = parquet_file.schema_arrow.get_field_index(column)
    if column_index != -1:
        metadata_max = -1
        found_stats = False
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            stats = parquet_file.metadata.row_group(row_group_index).column(column_index).statistics
            if stats is None or stats.max is None:
                found_stats = False
                break
            found_stats = True
            metadata_max = max(metadata_max, int(stats.max))

        if found_stats:
            return metadata_max

    max_value = -1
    for batch in iter_record_batches(parquet_path, [column], batch_size=batch_size):
        local_max = pc.max(batch.column(0)).as_py()
        if local_max is not None and local_max > max_value:
            max_value = local_max
    return max_value


def get_parquet_num_rows(parquet_path: Path) -> int:
    return pq.ParquetFile(parquet_path).metadata.num_rows