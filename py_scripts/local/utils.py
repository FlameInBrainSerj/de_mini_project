import os

from py_scripts.env import DATA_SOURCE_PATH, DATA_ARCHIVE_PATH


def archivise_file(source_type: str, filename: str) -> None:
    os.rename(
        DATA_SOURCE_PATH / source_type / filename,
        DATA_ARCHIVE_PATH / source_type / f"{filename}.backup",
    )
