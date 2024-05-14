from pprint import pprint
import pytest
from pathlib import Path

from easy_csv_db import EasyCsvDb

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
    db.connection.close()


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


def test_display_tables(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    """Test to make sure no errors are thrown"""
    table_name = "test_query_table_1"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    table_name = "test_query_table_2"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    easy_csv_db.display_tables()


def test_create_table_from_csv(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    cursor = easy_csv_db.connection.execute(f"SELECT * FROM {table_name}")
    assert cursor.fetchall() == [("1", "Alice"), ("2", "Bob")]


def test_execute(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_query_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)
    cursor = easy_csv_db.connection.execute(f"INSERT INTO {table_name} (id, name) VALUES ('3', 'Charlie')")
    assert not cursor.fetchall()
    cursor = easy_csv_db.connection.execute(f"SELECT * FROM {table_name}")
    assert cursor.fetchall() == [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]


def test_backup_to_db_file(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_backup_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)

    backup_file_path = TEST_OUTPUT_DIR_PATH / "test_backup.sqlite"
    easy_csv_db.backup_to_db_file(backup_file_path)

    # Check if the backup file exists
    assert backup_file_path.exists()

    # Check if the backup file has the correct data
    backup_db = EasyCsvDb(backup_file_path)
    cursor = backup_db.connection.execute(f"SELECT * FROM {table_name}")
    assert cursor.fetchall() == [("1", "Alice"), ("2", "Bob")]


def test_create_view_from_query(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)  # Create the test_table
    # easy_csv_db.display_tables()
    query = "SELECT * FROM test_table WHERE id > '1'"
    # exit("here")
    view_name = "test_view"
    easy_csv_db.create_view_from_query(query, view_name)
    easy_csv_db.display_tables()

    # exit("here")

    cursor = easy_csv_db.connection.execute(f"SELECT * FROM {view_name}")
    
    assert cursor.fetchall() == [("2", "Bob")]


def test_get_all_view_names(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)  # Create the test_table
    query = "SELECT * FROM test_table WHERE id > 1"
    view_name = "test_view"
    easy_csv_db.create_view_from_query(query, view_name)
    assert easy_csv_db.get_all_view_names() == [view_name]


def test_get_all_table_and_view_names(easy_csv_db: EasyCsvDb, temp_csv_file: Path) -> None:
    table_name = "test_table"
    easy_csv_db.create_table_from_csv(temp_csv_file, table_name)  # Create the test_table
    query = "SELECT * FROM test_table WHERE id > 1"
    view_name = "test_view"
    easy_csv_db.create_view_from_query(query, view_name)
    assert easy_csv_db.get_all_table_and_view_names() == [table_name, view_name]