import shutil

from py_scripts.env import DATA_SOURCE_PATH, DATA_ARCHIVE_PATH


def archivise_file(source_type: str, filename: str) -> None:
    shutil.move(
        DATA_SOURCE_PATH / source_type / filename,
        DATA_ARCHIVE_PATH / source_type / f"{filename}.backup",
    )
