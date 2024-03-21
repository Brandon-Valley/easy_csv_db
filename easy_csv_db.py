from pathlib import Path
import pandas as pd
from pandas import DataFrame
import sqlite3
import json
from pathlib import Path
from typing import Dict, Optional
from tabulate import tabulate


class EasyCsvDb:
    '''
    ## Simple Usage Example:

    ```python
    db = EasyCsvDb()
    db.create_table_from_csv(CSV_A_PATH, "a_table")
    db.create_table_from_csv(CSV_B_PATH, "b_table")
    db.display_tables()
    df = db.query(
        """
        SELECT * FROM b_table
        JOIN equiv_table
        ON b_table.common_field = a_table.common_field
    """
    )
    df.to_csv(CSV_C_PATH, index=False)
    ```
    '''

    def __init__(self):
        self.csv_path_by_table_name: Dict[str, Path] = {}

        # Connect to SQLite Database (In-memory)
        self.sqlite_connection = sqlite3.connect(":memory:")

    def query(self, query: str) -> DataFrame:
        return pd.read_sql_query(query, self.sqlite_connection)

    def display_tables(self, max_table_rows_to_display: int = 4) -> list:
        print("")
        print("#####################################################################################################")
        print("#####################################################################################################")
        print(f"EasyCsvDb Table Display ({max_table_rows_to_display=}):")
        print("")
        result = self.query("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = result["name"].tolist()
        for table_name in table_names:

            # Get csv_path_str
            csv_path_str = "This table was not created from a CSV file."
            if table_name in self.csv_path_by_table_name:
                csv_path_str = self.csv_path_by_table_name[table_name].as_posix()

            print(f"Table: {table_name}")
            print(f"  - From: {csv_path_str}")
            print("")

            # Get DataFrame to display
            df = self.query(f"SELECT * FROM {table_name} LIMIT {max_table_rows_to_display};")

            # Print the DataFrame as a nice text-based table
            print(tabulate(df, headers="keys", tablefmt="psql", maxcolwidths=None, showindex=False))
            print("")
        print("\n#####################################################################################################")
        print("\n#####################################################################################################")

    def create_table_from_csv(
        self, csv_path: Path, table_name: Optional[str] = None, index: bool = False, low_memory=False
    ) -> None:
        """
        # Parameters:
        ---
        index : bool, default True
            Write DataFrame index as a column. Uses index_label as the column name in the table.

        low_memory : bool, default True
            Setting low_memory=False causes pandas to read more of the file to decide what the data types should be.
            This can use more memory, but it can also prevent incorrect data type guesses. If your file is very large
            and you're running out of memory, you might need to consider other options, such as reading the file in
            chunks or specifying the data types of the columns manually.
        """
        if not table_name:
            table_name = csv_path.stem

        # Read CSV files into pandas DataFrames
        data_frame: DataFrame = pd.read_csv(csv_path, low_memory=low_memory)

        # Store DataFrames in the database
        data_frame.to_sql(table_name, self.sqlite_connection, index=index)

        self.csv_path_by_table_name[table_name] = csv_path

    def to_json(self) -> dict:
        json_serializable_csv_path_by_table_name = {
            table_name: csv_path.as_posix() for table_name, csv_path in self.csv_path_by_table_name.items()
        }
        return json_serializable_csv_path_by_table_name

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

    def __exit__(self) -> str:
        # Save Changes and Close Connection
        self.sqlite_connection.commit()
        self.sqlite_connection.close()
