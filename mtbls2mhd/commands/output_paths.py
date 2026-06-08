import os
from pathlib import Path


def resolve_output_file_path(output_dir: str | Path, output_filename: str) -> Path:
    """Resolve a CLI output filename while keeping writes inside output_dir."""
    drive, _ = os.path.splitdrive(output_filename)
    if (
        not output_filename
        or drive
        or output_filename in {".", ".."}
        or "/" in output_filename
        or "\\" in output_filename
    ):
        raise ValueError("--output-filename must be a file name, not a path")

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    return output_dir_path / output_filename
