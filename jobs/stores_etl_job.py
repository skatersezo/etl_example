import pandas as pd
from database.Database import Database


def main():
    # execute ETL pipeline
    data = extract("data/sample_data.csv")
    data_transformed = transform(data)
    load(data_transformed)


def extract(csv_file):
    try:
        stores_df = pd.read_csv(csv_file)

        stores_schema = ['store_name', 'country_name', 'street_name', 'pin_code', 'lvl1_geog', 'lvl2_geog', 'lvl3_geog']

        if list(stores_df.columns) != stores_schema:
            raise Exception('Input data does not conform to the schema')
        else:
            return stores_df
    except Exception as e:
        print("Data extraction error: " + str(e))


def transform(stores_df):
    try:
        db = Database()
        # Country code lookup
        countries_df = pd.read_sql_query("SELECT country_name, country_code FROM dim_country", db.con)
        countries_df['country_name'] = countries_df['country_name'].str.lower()
        country_dict = dict(zip(countries_df['country_name'], countries_df['country_code']))
        stores_df['country_code'] = stores_df['country_name'].str.lower().map(country_dict)

        # Generate store_id and store_code columns
        last_store_id = pd.read_sql_query('SELECT MAX(store_id) as last_store_id FROM dim_stores', con=db.con).iloc[0]['last_store_id'] or 0
        stores_df['store_id'] = range(last_store_id + 1, last_store_id + 1 + len(stores_df))
        stores_df['store_code'] = stores_df['country_code'] + stores_df['store_id'].astype(str)

        # Reorder the columns
        stores_df = stores_df[['store_id', 'store_code', 'store_name', 'country_code', 'street_name', 'pin_code', 'lvl1_geog', 'lvl2_geog', 'lvl3_geog']]
        return stores_df
    except Exception as e:
        print("Data transformation error: " + str(e))
    finally:
        db.con.close()



def load(stores_df):
    try:
        db = Database()
        stores_df.to_sql('dim_stores', db.con, if_exists='append', index=False)
        db.con.commit()
    except Exception as e:
        print("Data loading error: " + str(e))
    finally:
        db.con.close()
