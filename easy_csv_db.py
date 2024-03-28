import csv
from pathlib import Path
import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class EasyCsvDb:
    def __init__(self, db_file_path: Optional[Path] = None):
        """db_file_path defaults to None, which creates an in-memory database."""
        self.csv_path_by_table_name: Dict[str, Path] = {}

        if db_file_path:
            # Connect to SQLite Database (On-disk)
            self.connection = sqlite3.connect(db_file_path)
        else:
            # Connect to SQLite Database (In-memory)
            self.connection = sqlite3.connect(":memory:")

    def get_all_table_names(self) -> List[str]:
        """Returns a list of all table names in the database."""
        cursor = self.connection.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row[0] for row in cursor.fetchall()]

    def display_tables(self, max_table_rows_to_display: int = 4) -> list:
        def _display_cursor_as_text_table(cursor: sqlite3.Cursor) -> None:
            """
            Example output:

                ```
                id | name
                ----------
                1  | Alice
                2  | Bob
                ```
            """
            # Get column names
            column_names = [description[0] for description in cursor.description]

            # Calculate column widths
            column_widths = {column: len(column) for column in column_names}
            for row in cursor.fetchall():
                for column, value in zip(column_names, row):
                    column_widths[column] = max(column_widths[column], len(str(value)))

            # Print the column names with proper spacing
            headers = " | ".join(f"{name:{column_widths[name]}}" for name in column_names)
            print(headers)

            # Divider
            print("-" * len(headers))

            # Print the row data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {max_table_rows_to_display};")
            for row in cursor.fetchall():
                row = " | ".join(f"{str(value):{column_widths[column]}}" for column, value in zip(column_names, row))
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
            cursor = self.connection.execute(f"SELECT * FROM {table_name} LIMIT {max_table_rows_to_display};")

            # Print the list of row_dicts as a nice text-based table
            _display_cursor_as_text_table(cursor)

            print("")
        print("#####################################################################################################")
        print("#####################################################################################################")

    def create_table_from_csv(self, csv_path: Path, table_name: Optional[str] = None) -> None:
        """table_name defaults to the csv_path's stem if not provided."""
        if not table_name:
            table_name = csv_path.stem

        with open(csv_path, encoding="utf-8", newline="") as f:
            with self.connection:
                dr = csv.DictReader(f, dialect="excel")
                field_names = dr.fieldnames

                sql = 'DROP TABLE IF EXISTS "{}"'.format(table_name)
                self.connection.execute(sql)

                formatted_field_names = ",".join('"{}"'.format(col) for col in field_names)
                sql = f'CREATE TABLE "{table_name}" ( {formatted_field_names} )'

                self.connection.execute(sql)

                vals = ",".join("?" for _ in field_names)
                sql = f'INSERT INTO "{table_name}" VALUES ( {vals} )'
                self.connection.executemany(sql, (list(map(row.get, field_names)) for row in dr))

        self.csv_path_by_table_name[table_name] = csv_path

    def backup_to_db_file(self, backup_db_file_path: Path) -> None:
        """Writes the database to a file."""
        backup_db_file_path.parent.mkdir(parents=True, exist_ok=True)

        new_backup_connection = sqlite3.connect(
            f"file:{backup_db_file_path.as_posix()}", detect_types=sqlite3.PARSE_DECLTYPES, uri=True
        )

        with new_backup_connection:
            self.connection.backup(new_backup_connection)

    def to_json(self) -> dict:
        json_serializable_csv_path_by_table_name = {
            table_name: csv_path.as_posix() for table_name, csv_path in self.csv_path_by_table_name.items()
        }
        return json_serializable_csv_path_by_table_name

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

    def __exit__(self) -> str:
        # Save Changes and Close Connection
        self.connection.commit()
        self.connection.close()
