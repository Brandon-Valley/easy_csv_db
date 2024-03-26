import csv
from pathlib import Path
import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class EasyCsvDb:
    '''
    ## Simple Usage Example:

    ```python
    db = EasyCsvDb()
    db.create_table_from_csv(CSV_A_PATH, "a_table")
    db.create_table_from_csv(CSV_B_PATH, "b_table")
    db.display_tables()
    row_dicts = db.execute(
        """
        SELECT * FROM b_table
        JOIN equiv_table
        ON b_table.common_field = a_table.common_field
    """
    )
    row_dicts.to_csv(CSV_C_PATH, index=False)
    ```
    '''

    def __init__(self, db_file_path: Optional[Path] = None):
        """db_file_path defaults to None, which creates an in-memory database."""
        self.csv_path_by_table_name: Dict[str, Path] = {}

        if db_file_path:
            # Connect to SQLite Database (On-disk)
            self.conn = sqlite3.connect(db_file_path)
        else:
            # Connect to SQLite Database (In-memory)
            self.conn = sqlite3.connect(":memory:")

    def execute(self, statement: str) -> Optional[List[Dict[str, Any]]]:
        """
        Executes the given SQL statement and returns result as row_dicts if applicable.

        Example output format:

        ```json
        [
            {"column_1": value_1, "column_2": value_2},
            {"column_1": value_3, "column_2": value_4},
            ...
        ]
        ```
        """
        cursor = self.conn.execute(statement)

        if cursor.description:
            column_names = [d[0] for d in cursor.description]
            row_dicts = [dict(zip(column_names, row)) for row in cursor.fetchall()]
            return row_dicts

    def get_all_table_names(self) -> List[str]:
        return [row["name"] for row in self.execute("SELECT name FROM sqlite_master WHERE type='table';")]

    def display_tables(self, max_table_rows_to_display: int = 4) -> list:
        def display_row_dicts_as_text_table(row_dicts: List[Dict[str, Any]]) -> None:
            if not row_dicts:
                print("No data to display.")
                return

            # Calculate column widths
            column_names = list(row_dicts[0].keys())
            column_widths = {column: len(column) for column in column_names}
            for row_dict in row_dicts:
                for column, value in row_dict.items():
                    column_widths[column] = max(column_widths[column], len(str(value)))

            # Print the column names with proper spacing
            headers = " | ".join(f"{name:{column_widths[name]}}" for name in column_names)
            print(headers)

            # Divider
            print("-" * len(headers))

            # Print the row data
            for row_dict in row_dicts:
                row = " | ".join(f"{str(value):{column_widths[column]}}" for column, value in row_dict.items())
                print(row)

        print("")
        print("#####################################################################################################")
        print("#####################################################################################################")
        print(f"EasyCsvDb Table Display ({max_table_rows_to_display=}):")
        print("")

        for table_name in self.get_all_table_names():

            # Get csv_path_str
            csv_path_str = "This table was not created from a CSV file."
            if table_name in self.csv_path_by_table_name:
                csv_path_str = self.csv_path_by_table_name[table_name].as_posix()

            print(f"Table: {table_name}")
            print(f"  - From: {csv_path_str}")
            print("")

            # Get row_dicts to display
            row_dicts = self.execute(f"SELECT * FROM {table_name} LIMIT {max_table_rows_to_display};")

            # Print the list of row_dicts as a nice text-based table
            display_row_dicts_as_text_table(row_dicts)

            print("")
        print("#####################################################################################################")
        print("#####################################################################################################")

    def create_table_from_csv(self, csv_path: Path, table_name: Optional[str] = None) -> None:
        """table_name defaults to the csv_path's stem if not provided."""
        if not table_name:
            table_name = csv_path.stem

        with open(csv_path, encoding="utf-8", newline="") as f:
            with self.conn:
                dr = csv.DictReader(f, dialect="excel")
                field_names = dr.fieldnames

                sql = 'DROP TABLE IF EXISTS "{}"'.format(table_name)
                self.conn.execute(sql)

                formatted_field_names = ",".join('"{}"'.format(col) for col in field_names)
                sql = f'CREATE TABLE "{table_name}" ( {formatted_field_names} )'

                self.conn.execute(sql)

                vals = ",".join("?" for _ in field_names)
                sql = f'INSERT INTO "{table_name}" VALUES ( {vals} )'
                self.conn.executemany(sql, (list(map(row.get, field_names)) for row in dr))

        self.csv_path_by_table_name[table_name] = csv_path

    def backup_to_db_file(self, backup_db_file_path: Path) -> None:
        """Writes the database to a file."""
        backup_db_file_path.parent.mkdir(parents=True, exist_ok=True)

        new_backup_connection = sqlite3.connect(
            f"file:{backup_db_file_path.as_posix()}", detect_types=sqlite3.PARSE_DECLTYPES, uri=True
        )

        with new_backup_connection:
            self.conn.backup(new_backup_connection)

    def to_json(self) -> dict:
        json_serializable_csv_path_by_table_name = {
            table_name: csv_path.as_posix() for table_name, csv_path in self.csv_path_by_table_name.items()
        }
        return json_serializable_csv_path_by_table_name

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

    def __exit__(self) -> str:
        # Save Changes and Close Connection
        self.conn.commit()
        self.conn.close()
