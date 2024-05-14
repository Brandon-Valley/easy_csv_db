import csv
import logging
from pathlib import Path
import sqlite3
import json
from typing import Dict, List, Optional


class EasyCsvDb:
    def __init__(self, db_file_path: Optional[Path] = None):
        """db_file_path defaults to None, which creates an in-memory database."""
        self.csv_path_by_entity_name: Dict[str, Path] = {}  # TMP why need this?

        if db_file_path:
            # Connect to SQLite Database (On-disk)
            self.connection: sqlite3.Connection = sqlite3.connect(db_file_path)
        else:
            # Connect to SQLite Database (In-memory)
            self.connection: sqlite3.Connection = sqlite3.connect(":memory:")

    def get_all_table_names(self) -> List[str]:
        """Returns a list of all table names in the database."""
        cursor = self.connection.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row[0] for row in cursor.fetchall()]

    def get_all_view_names(self) -> List[str]:
        """Returns a list of all view names in the database."""
        cursor = self.connection.execute("SELECT name FROM sqlite_master WHERE type='view';")
        return [row[0] for row in cursor.fetchall()]

    def get_all_entity_names(self) -> List[str]:
        """Returns a list of all table and view names in the database."""
        cursor = self.connection.execute("SELECT name FROM sqlite_master;")
        return [row[0] for row in cursor.fetchall()]

    # FIXME rename table vars
    def display(self, max_rows_to_display: int = 4) -> list:
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
            cursor.execute(f"SELECT * FROM {entity_name} LIMIT {max_rows_to_display};")
            for row in cursor.fetchall():
                row = " | ".join(f"{str(value):{column_widths[column]}}" for column, value in zip(column_names, row))
                print(row)

        print("")
        print("#####################################################################################################")
        print("#####################################################################################################")
        print(f"EasyCsvDb Display ({max_rows_to_display=}):")
        print("")

        for entity_name in self.get_all_entity_names():

            # Get csv_path_str
            csv_path_str = "This entity was not created from a CSV file."
            if entity_name in self.csv_path_by_entity_name:
                csv_path_str = self.csv_path_by_entity_name[entity_name].as_posix()

            print(f"Name: {entity_name}")
            print(f"  - From: {csv_path_str}")
            print("")

            # Get row_dicts to display
            cursor = self.connection.execute(f"SELECT * FROM {entity_name} LIMIT {max_rows_to_display};")

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

        self.csv_path_by_entity_name[table_name] = csv_path

    def create_view(
        self, create_view_statement: str, csv_path: Path, view_name: Optional[str] = None, write_csv: bool = True
    ) -> None:
        """
        Creates a view from the provided create_view_statement.
          - The view_name defaults to the csv_path's stem if not provided.
          - If write_csv, the csv at csv_path will be updated to reflect the view after it is created.
        """

        def _view_exists(view_name: str) -> bool:
            cursor = self.connection.execute("SELECT name FROM sqlite_master WHERE type='view';")
            return view_name in [row[0] for row in cursor.fetchall()]

        if not view_name:
            view_name = csv_path.stem

        # Create the view
        with self.connection:
            self.connection.execute(create_view_statement)

        assert _view_exists(view_name), (
            f"View '{view_name}' was not created, this most likely means the provided {view_name=} does not match the "
            f"actual view name defined in {create_view_statement=}."
        )

        self.csv_path_by_entity_name[view_name] = csv_path

        if write_csv:
            self.update_csv(view_name)

    def update_csv(self, entity_name: str) -> None:
        """Updates the csv at the provided csv_path with the data from the table or view."""
        csv_path = self.csv_path_by_entity_name[entity_name]

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            cursor = self.connection.execute(f"SELECT * FROM {entity_name}")
            writer.writerow([description[0] for description in cursor.description])
            writer.writerows(cursor.fetchall())

    def update_csvs(self, entity_names: Optional[List[str]] = None) -> None:
        """If not entity_names, updates all csvs that are associated with a table or view."""
        if not entity_names:
            entity_names = self.get_all_entity_names()

        for entity_name in entity_names:
            if entity_name in self.csv_path_by_entity_name:
                self.update_csv(entity_name)

    def backup_to_db_file(self, backup_db_file_path: Path) -> None:
        """Writes the database to a file."""
        backup_db_file_path.parent.mkdir(parents=True, exist_ok=True)

        new_backup_connection = sqlite3.connect(
            f"file:{backup_db_file_path.as_posix()}", detect_types=sqlite3.PARSE_DECLTYPES, uri=True
        )

        with new_backup_connection:
            self.connection.backup(new_backup_connection)

    def to_json(self) -> dict:
        json_serializable_csv_path_by_entity_name = {
            entity_name: csv_path.as_posix() for entity_name, csv_path in self.csv_path_by_entity_name.items()
        }
        return json_serializable_csv_path_by_entity_name

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

    def __exit__(self) -> str:
        # Save Changes and Close Connection
        self.connection.commit()
        self.connection.close()
