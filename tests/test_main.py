import pytest
import pandas as pd
from pathlib import Path

from easy_csv_db import EasyCsvDb

import pytest
from easy_csv_db import EasyCsvDb
import pytest
from pathlib import Path
import tempfile
import csv
from typing import Generator

SCRIPT_PARENT_DIR_PATH = Path(__file__).parent
TEST_OUTPUT_DIR_PATH = SCRIPT_PARENT_DIR_PATH / "outputs"


# Fixture for EasyCsvDb instance
@pytest.fixture
def easy_csv_db() -> Generator[EasyCsvDb, None, None]:
    db = EasyCsvDb()
    yield db
    db.conn.close()


# Fixture for creating a temporary CSV file
@pytest.fixture
def temp_csv_file() -> Generator[Path, None, None]:
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", newline="") as tmpfile:
        writer = csv.DictWriter(tmpfile, fieldnames=["id", "name"])
        writer.writeheader()
        writer.writerows(
            [
                {"id": "1", "name": "Alice"},
                {"id": "2", "name": "Bob"},
            ]
        )
    yield Path(tmpfile.name)
    Path(tmpfile.name).unlink()  # Clean up after the test


def test_create_table_from_csv(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    result = easy_csv_db.execute(f"SELECT * FROM {table_name}")
    assert len(result) == 2
    assert result[0] == {"id": "1", "name": "Alice"}
    assert result[1] == {"id": "2", "name": "Bob"}


def test_execute(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_query_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    result = easy_csv_db.execute(f"INSERT INTO {table_name} (id, name) VALUES ('3', 'Charlie')")
    assert not result
    result = easy_csv_db.execute(f"SELECT * FROM {table_name}")
    assert len(result) == 3
    assert result[2] == {"id": "3", "name": "Charlie"}


def test_display_tables(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    """Test to make sure no errors are thrown"""
    table_name = "test_query_table_1"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    table_name = "test_query_table_2"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    easy_csv_db.display_tables()


def test_backup_to_db_file(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_backup_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)

    backup_file_path = TEST_OUTPUT_DIR_PATH / "test_backup.sqlite"
    easy_csv_db.backup_to_db_file(backup_file_path)

    # Check if the backup file exists
    assert backup_file_path.exists()

    # Check if the backup file has the correct data
    backup_db = EasyCsvDb(backup_file_path)
    result = backup_db.execute(f"SELECT * FROM {table_name}")
    assert len(result) == 2
    assert result[0] == {"id": "1", "name": "Alice"}
    assert result[1] == {"id": "2", "name": "Bob"}
