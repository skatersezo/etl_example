import pandas as pd
import sqlite3


class Database:

    def __init__(self):
        self.con = sqlite3.connect('eyos.db')
        self.cur = self.con.cursor()


    def create_eyos_db_and_tables(self):
        self.cleanup()
        self.create_table_dim_stores()    
        self.create_table_dim_country()
        self.load_data_in_dim_country()
        self.load_data_in_dim_stores()


    def create_table_dim_stores(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_stores (
                store_id INTEGER PRIMARY KEY,
                store_code TEXT,
                store_name TEXT,
                country_code TEXT,
                street_name TEXT,
                pin_code INTEGER,
                lvl1_geog TEXT,
                lvl2_geog TEXT,
                lvl3_geog TEXT
        )""")


    def create_table_dim_country(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_country (
                country_id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL
        )""")


    def load_data_in_dim_country(self):
        df = pd.read_csv('database/dim_country.csv')
        df.to_sql('dim_country', self.con, if_exists='append', index=False)


    def load_data_in_dim_stores(self):
        df = pd.read_csv('database/dim_stores.csv')
        df.to_sql('dim_stores', self.con, if_exists='append', index=False)


    def get_dim_stores_table(self):
        self.cur.execute("SELECT * FROM dim_stores")
        return self.cur.fetchall()


    def get_dim_country_table(self):
        self.cur.execute("SELECT * FROM dim_country")
        return self.cur.fetchall()


    def cleanup(self):
        self.cur.execute("DROP TABLE IF EXISTS dim_stores;")
        self.cur.execute("DROP TABLE IF EXISTS dim_country;")

    def close_connection(self):
        self.con.close()
