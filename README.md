# Stores ETL job

Python ETL basic project that aims to build a basic ETL job that extracts data from a csv file and loads it in a Sqlite3 database
following the table structure found in [Table Structure.xslx](docs/Table%20Structure.xlsx) and testing it.
Test cases are documented in the [Test cases.xlsx](docs/Test%20cases.xlsx) file.

NOTE: Data quality issues can happen with the geography levels since there's no cross referecing for this particular piece of data.


## Prerequisites
* Python3 (this project was developed with Python@3.9)

## Setup

1. Clone this repository to your local machine.
2. Create the virtual environment
``` bash
python -m venv env
```
3. Activate the virtual environment:
```bash
source env/bin/activate # For unix based systems
env\Scripts\activate.bat # For Windows
```
4. Install the dependencies:
```bash
python -m pip install -r requirements.txt
```

## Running the tests
To run the tests, use the following command:
```bash
python -m unittest tests/test_dim_stores_etl_job.py
```