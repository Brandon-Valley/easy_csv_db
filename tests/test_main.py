import pytest
import pandas as pd
from pathlib import Path

from easy_csv_db import EasyCsvDb


@pytest.fixture
def temp_csv(tmp_path) -> Path:
    # Create a temporary CSV file
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
    df.to_csv(csv_path, index=False)
    return csv_path


def test_create_table_from_csv_and_query(temp_csv):
    db = EasyCsvDb()
    table_name = "test_table"

    # Test creating table from CSV
    db.create_table_from_csv(temp_csv, table_name)

    # Test querying data
    df_queried = db.query(f"SELECT * FROM {table_name};")

    # Verify the queried data matches the original CSV
    assert not df_queried.empty
    assert (df_queried.columns == ["col1", "col2"]).all()
    assert df_queried["col1"].tolist() == [1, 2]
    assert df_queried["col2"].tolist() == ["A", "B"]


def test_to_json(temp_csv):
    db = EasyCsvDb()
    table_name = "test_table"

    db.create_table_from_csv(temp_csv, table_name)
    json_output = db.to_json()

    # Verify the JSON output
    assert table_name in json_output
    assert json_output[table_name] == temp_csv.as_posix()


def test_repr(temp_csv):
    db = EasyCsvDb()
    table_name = "test_table"

    db.create_table_from_csv(temp_csv, table_name)
    repr_output = db.__repr__()

    # Verify the repr output
    assert table_name in repr_output
    assert temp_csv.name in repr_output
