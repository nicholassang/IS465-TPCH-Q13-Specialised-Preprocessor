from pathlib import Path


def compare_csv_outputs(path_a: Path, path_b: Path) -> bool:
    """
    Exact textual CSV comparison after normalizing trailing whitespace.
    """
    a = [line.strip() for line in path_a.read_text(encoding="utf-8").splitlines()]
    b = [line.strip() for line in path_b.read_text(encoding="utf-8").splitlines()]
    return a == b