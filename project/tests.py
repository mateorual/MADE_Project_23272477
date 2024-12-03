import os
import sys
import unittest
import sqlite3
from pathlib import Path
from io import StringIO
from tqdm import tqdm
from pipeline import Pipeline
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text
from prettytable import PrettyTable


class PipelineAutomatedTesting(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
            Executes the data pipeline with progress tracking and sets up the environment for subsequent tests.
            
            - Redirects stdout to suppress print statements from pipeline.py.
            - Initializes the pipeline and runs it step-by-step with a progress bar.
            - Captures the SQLite database path and establishes a SQLAlchemy engine for further validation.
        """
        print("\n[INFO] Executing the data pipeline for testing...")

        # Redirect stdout to suppress print statements of pipeline.py and capture output
        cls.old_stdout = sys.stdout
        cls.stdout_buffer = StringIO()
        sys.stdout = cls.stdout_buffer

        try:
            cls.pipeline = Pipeline()

            # Define the total steps (based on pipeline's progress)
            total_steps = 3  # Extract, Transform, Load

            with tqdm(total=total_steps, desc="Pipeline Execution", unit="step") as pbar:
                # Extract data
                try:
                    data = cls.pipeline.extract_data()
                    pbar.update(1)  # Extraction completed
                except Exception as e:
                    cls.fail(f"Error during data extraction: {e}")
                
                #Transform data
                try:
                    transformed_data = cls.pipeline.transform_data(data)
                    pbar.update(1)  # Transformation completed
                except Exception as e:
                    cls.fail(f"Error during data transformation: {e}")
                    
                # Load data into SQLite
                try:
                    cls.pipeline.save_data_to_sqlite({
                        "sales_rents_2011_2021": transformed_data["sales_rents"],
                        "monthly_entry_colombians_foreigners": transformed_data["tourism_1"],
                        "monthly_passengers_origin": transformed_data["tourism_2"],
                    })
                    pbar.update(1)  # Loading completed
                except Exception as e:
                    cls.fail(f"Error during data loading: {e}")

            cls.db_path = cls.pipeline.database_name
            cls.engine = create_engine(f"sqlite:///{cls.db_path}", echo=False)

        finally:
            # Restore stdout after pipeline execution
            sys.stdout = cls.old_stdout


    @classmethod
    def tearDownClass(cls):
        """
            Cleans up resources after all test cases have executed.

            - Disposes of the SQLAlchemy engine to ensure all connections to the SQLite database are properly closed.
            - Prints confirmation of resource cleanup.
        """
        try:
            cls.engine.dispose()
            print("\n[INFO] Database engine disposed.")
        except Exception as e:
            print(f"Error disposing engine: {e}")

    
    def test_01_output_database_exists(self):
        """
            Ensures the SQLite database was successfully created by the pipeline.
            - Verifies the existence of the database file.
        """
        print("\n[1/6] Validating: SQLite database creation...")
        self.assertTrue(os.path.exists(self.db_path), f"Database file '{self.db_path}' does not exist.")

    
    def test_02_database_validity(self):
        """
            Ensures the SQLite database is valid and can be connected.
        """
        try:
            print("[2/6] Validating: SQLite database is valid...")
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute("SELECT 1")
            
        except sqlite3.DatabaseError as e:
            self.fail(f"SQLite database is invalid: {e}")


    def test_03_tables_exist(self):
        """
            Verifies that all expected tables are present in the SQLite database.

            - Uses SQLAlchemy's inspect to list the tables in the database.
            - Asserts the existence of key tables: sales_rents_2011_2021, monthly_entry_colombians_foreigners, and monthly_passengers_origin.
        """
        print("[3/6] Validating: Expected tables are present in the database...")
        inspector = inspect(self.engine)

        try:
            tables = inspector.get_table_names()
            expected_tables = ["sales_rents_2011_2021", "monthly_entry_colombians_foreigners", "monthly_passengers_origin"]
            for table in expected_tables:
                self.assertIn(table, tables, f"Table '{table}' not found in the database.")
        except OperationalError as e:
            self.fail(f"Operational error when inspecting database: {e}")


    def test_04_table_data_non_empty(self):
        """
            Ensures that all tables in the database contain data.

            - Queries the count of rows in each table.
            - Asserts that none of the tables are empty.
        """
        print("[4/6] Validating: Tables are non-empty...")
        with self.engine.connect() as connection:
            try:
                table_row_counts = {
                    "sales_rents_2011_2021": connection.execute(text("SELECT COUNT(*) FROM sales_rents_2011_2021")).fetchone()[0],
                    "monthly_entry_colombians_foreigners": connection.execute(text("SELECT COUNT(*) FROM monthly_entry_colombians_foreigners")).fetchone()[0],
                    "monthly_passengers_origin": connection.execute(text("SELECT COUNT(*) FROM monthly_passengers_origin")).fetchone()[0],
                }

                for table, row_count in table_row_counts.items():
                    print(f"--Table '{table}' contains {row_count} rows.")
                    self.assertGreater(row_count, 0, f"Table '{table}' is empty.")
            except OperationalError as e:
                self.fail(f"Operational error when querying table data: {e}")


    def test_05_column_integrity(self):
        """
            Validates the column integrity of each table, identifying missing or unexpected columns.
        
            - Checks that each table contains the expected columns.
            - Ensures the structure of the tables matches the predefined schema.
        """
        print("[5/6] Validating: Column integrity for all tables...")
        with self.engine.connect() as connection:
            try:
                # Define expected columns for each table
                expected_columns = {
                    "sales_rents_2011_2021": [
                        "Period", "Research", "Property", "Condition", "Neighborhood", "Stratum",
                        "Private_Area_m2", "Lot_Area_m2", "Commercial_Price_COP", "Price_per_m2_COP",
                        "Longitude", "Latitude"
                    ],
                    "monthly_entry_colombians_foreigners": ["Nationality", "Period", "Number"],
                    "monthly_passengers_origin": ["Code", "Origin", "Period", "Number", "Nationality"]
                }

                for table, columns in expected_columns.items():
                    query = f"PRAGMA table_info({table})"
                    result = connection.execute(text(query)).fetchall()
                    table_columns = [row[1] for row in result]  # Extract column names

                    # Identify missing and unexpected columns
                    missing_columns = [col for col in columns if col not in table_columns]
                    unexpected_columns = [col for col in table_columns if col not in columns]

                    # Assert no missing or unexpected columns
                    self.assertTrue(not missing_columns, f"Table '{table}' is missing columns: {missing_columns}")
                    self.assertTrue(not unexpected_columns, f"Table '{table}' has unexpected columns: {unexpected_columns}")

                    if not missing_columns and not unexpected_columns:
                        print(f"--Table '{table}' passed column integrity check.")
            except OperationalError as e:
                self.fail(f"Operational error when checking column integrity: {e}")
    
    
    def test_06_data_sanity(self):
        """
        Performs sanity checks on data in the database.
        - Ensures numerical columns have non-negative values.
        - Validates categorical values in specific columns.
        """
        print("[6/6] Validating: Sanity checks on data...")
        with self.engine.connect() as connection:
            # Sanity check for `monthly_entry_colombians_foreigners`
            result = connection.execute(text("SELECT MIN(Number), MAX(Number) FROM monthly_entry_colombians_foreigners")).fetchone()
            min_number, max_number = result
            self.assertGreaterEqual(min_number, 0, "Found negative Number in 'monthly_entry_colombians_foreigners'.")

            allowed_nationalities = {"Extranjero", "Colombiano"}
            result = connection.execute(text("SELECT DISTINCT Nationality FROM monthly_entry_colombians_foreigners")).fetchall()
            distinct_nationalities = {row[0] for row in result}
            self.assertTrue(
                distinct_nationalities.issubset(allowed_nationalities),
                f"Invalid Nationality values found in 'monthly_entry_colombians_foreigners': {distinct_nationalities - allowed_nationalities}"
            )

            # Sanity check for `monthly_passengers_origin`
            result = connection.execute(text("SELECT MIN(Number), MAX(Number) FROM monthly_passengers_origin")).fetchone()
            min_number, max_number = result
            self.assertGreaterEqual(min_number, 0, "Found negative Number in 'monthly_passengers_origin'.")

            result = connection.execute(text("SELECT DISTINCT Code FROM monthly_passengers_origin")).fetchall()
            for row in result:
                self.assertRegex(row[0], r"^[A-Z]{2}$", f"Invalid Code '{row[0]}' in 'monthly_passengers_origin'.")

            # Sanity check for `sales_rents_2011_2021`
            numerical_columns = ["Private_Area_m2", "Lot_Area_m2", "Commercial_Price_COP", "Price_per_m2_COP"]
            for column in numerical_columns:
                result = connection.execute(text(f"SELECT MIN({column}), MAX({column}) FROM sales_rents_2011_2021")).fetchone()
                min_value, max_value = result
                self.assertGreaterEqual(min_value, 0, f"Found negative value in column '{column}' of 'sales_rents_2011_2021'.")
                self.assertGreater(max_value, min_value, f"Max value should be greater than min value in column '{column}' of 'sales_rents_2011_2021'.")



if __name__ == "__main__":
    # Run tests
    result = unittest.TextTestRunner(verbosity=0).run(unittest.TestLoader().loadTestsFromTestCase(PipelineAutomatedTesting))

    # Generate a summary
    print("\n\nTest Summary")
    print("============")
    table = PrettyTable()
    table.field_names = ["Test Case", "Status"]
    
    # Iterate over results and populate the summary table
    for test in result.failures + result.errors:
        test_name = test[0]._testMethodName  # Extract the test method name
        table.add_row([test_name, "Failed"])

    # Add passed tests by checking total tests and subtracting errors and failures
    passed_count = result.testsRun - len(result.failures) - len(result.errors)
    for _ in range(passed_count):
        table.add_row([f"Test {_ + 1}", "Passed"])

    print(table)
    print(f"Total Tests: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
