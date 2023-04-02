import unittest
import csv
from database.Database import Database
from jobs.stores_etl_job import main as etl_job


class TestDimStoresETLJob(unittest.TestCase):

    def setUp(self) -> None:
        self.db = Database()
        self.db.create_eyos_db_and_tables()

    def tearDown(self) -> None:
        self.db.cleanup()
        self.db.close_connection()

    def test_dim_stores_etl_job(self):
        # Arrange
        with open('data/sample_data.csv', mode='r') as file:
            csv_rows = [dict(row) for row in csv.DictReader(file)]

        # Act
        etl_job()

        # Assert
        dim_stores = self.db.get_dim_stores_table()
        self.assert_no_null_values(dim_stores)
        self.assert_store_ids_are_unique(dim_stores)
        self.assert_store_codes_are_valid(dim_stores)
        self.assert_pin_code_positive_integer(dim_stores)
        self.assert_data_integrity(dim_stores, csv_rows)


    def assert_store_ids_are_unique(self, dim_stores):
        store_ids = [row[0] for row in dim_stores]
        self.assertTrue(all(isinstance(x, int) for x in store_ids))
        self.assertEqual(len(store_ids), len(set(store_ids)))


    def assert_store_codes_are_valid(self, dim_stores):
        store_codes = [f"{row[0]}{row[1]}" for row in dim_stores]
        self.assertEqual(len(store_codes), len(set(store_codes)))
        

    def assert_no_null_values(self, dim_stores):
        for row in dim_stores:
            for i, value in enumerate(row):
                if i == 8: # Skip checking lvl3_geog column
                    continue
                self.assertIsNotNone(value)

    
    def assert_pin_code_positive_integer(self, dim_stores):
        for row in dim_stores:
            pin_code = row[5]
            self.assertTrue(isinstance(pin_code, int))
            self.assertTrue(pin_code >= 0)
            self.assertTrue(pin_code == int(pin_code))
        

    def assert_data_integrity(self, dim_stores, csv_rows):
        table_columns = ['store_id', 'store_code', 'store_name', 'country_code', 'street_name', 'pin_code', 'lvl1_geog', 'lvl2_geog', 'lvl3_geog']
        table_rows = [dict(zip(table_columns, row)) for row in dim_stores]

        # Loop through each row in the CSV file
        for csv_row in csv_rows:

            # Find the corresponding row in the db table (if it exists)
            table_row = None
            for row in table_rows:
                if row['store_name'] == csv_row['store_name']:
                    table_row = row
                    break

            # If a corresponding row was found in the table, compare the values for each column
            if table_row is not None:
                for column in csv_row.keys():
                    if column in table_row:
                        # If a table value is null substitute for ''
                        table_value = str(table_row[column]) if str(table_row[column]) != 'None' else ''
                        csv_value = csv_row[column]
                        self.assertEqual(table_value, csv_value, f"Value mismatch for column {column} in row with store_name {csv_row['store_name']}")

                
if __name__ == '__main__':
    unittest.main()