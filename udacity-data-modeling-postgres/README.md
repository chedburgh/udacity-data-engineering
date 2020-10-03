# udacity-data-modeling-postgres

The Udacity Data Modeling Postgres project models the Sparkifydb from Sparkify to gain insights into user activity.

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

Due to the song/artist logs lacking many entries in the events log, its not possible to use foreign keys in the schema.

## ETL (Extract, Transform, Load) Scripts

- [create_tables.py](create_tables.py) - This script will drop any existing sparkifydb database tables, then create new sparkifydb tables.
- [etl.py](etl.py) - Run the extract, transform and load routines
- [sql_queries.py](sql_queries.py) - SQL queries used in the other scripts.

### ETL Notes

Where possible the etl.py script inserts data via the PostgreSQL copy command rather than a single line inserts. This greatly increases performance. To archive this, the following steps are taken:

- A temporary staging table is created in the database to hold data.
- The pandas dataframe is output to a temporary memory buffer in csv format.
- This buffer is used as the source for the PostgreSQL copy command to move the data directly into a PostgreSQL staging table.
- Data is inserted into the final table from the staging table.
- The staging table is removed.

### Running

Ensure a PostgreSQL database is available on localhost:5432, then:

```bash
./create_tables.py
./etl.py
```

## Docker

The etl.py script was developed against a dockerized PostgreSQL database. This is setup to mimic the sparkifydb login credentials. The Dockerfile and its build system are kept under /docker.

### Building

There is a simple build system to build the docker image:

```bash
cd docker
make
```

This will build a docker image named: udacity-postgres-student

This is not tagged and pushed to dockerhub, instead it is intended to be used locally as a development aid.

### Running

The docker image can be run as follows:

```bash
docker run -d -p 5432:5432 udacity-postgres-student
```

The etl.py script will automatically use this database once it is running, since it is hard coded to connect to localhost.

## Additional Files

There are two additional directories to support the project:

- doc - Any documents and diagrams required.
- schema - Dump of the PostgreSQL schema to help document the project.
