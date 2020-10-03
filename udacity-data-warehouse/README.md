# udacity-data-warehouse

The Udacity Data Warehouse project models the Sparkifydb from Sparkify to gain insights into user activity. The database is an AWS based Redshift database loaded from AWS S3 storage. To complete the load, data is first loaded to staging tables, before being inserted into the final tables

## Database Schema

The schema is based on the star schema pattern, with a single fact table and four dimension tables. The layout is described by the following diagram:

![alt text][logo]

[logo]: doc/diagram.png "Database ER diagram"

Short description as follows:

- songplays - Fact table and record of each song play.
- songs - Dimension table and table of songs and their details.
- artists - Dimension table and table of artists and their details.
- users - Dimension table and table of user details.
- time - Dimension table and breakdown of song play records time details.

Each dimension table contains a primary key to link it to the fact table, and the fact table uses foreign keys to reference back to the dimension tables.

## ETL (Extract, Transform, Load) Scripts

- [create_tables.py](create_tables.py) - This script will drop any existing sparkifydb database tables, then create new sparkifydb tables.
- [etl.py](etl.py) - Run the extract, transform and load routines.
- [sql_queries.py](sql_queries.py) - SQL queries used in the other scripts.

### ETL Notes

The script implements a simple command line interface. This is not refined, and has been added to aid implementation and debug of the pipeline. This allows various parts of the pipeline to be run separately in case there are problems. For help:

```bash
etl.py --help
```

The script is built up from a number of commands. Both 'verify' and 'load' contain nested commands to choose between staging and final.

```bash
etl.py verify --help
etl.py load --help
```

### Running

Follow the following steps to run:

- Setup and configure the AWS Redshift database. 
- Add configuration details to the config file dwh.cfg

Now run full pipeline as follows:
```bash
./etl.py full
```

This will do the following:

- Create tables
- Load to staging tables
- Insert to final tables

A short verification can be done as follows:

```bash
./etl.py verify staging
./etl.py verify final
```

A row count and 10 rows will be printed from each table.

## Additional Files

There is one additional directories to support the project:

- doc - Any documents and diagrams required.
