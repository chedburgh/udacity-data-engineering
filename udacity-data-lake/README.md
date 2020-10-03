# udacity-data-lake

This project uses Spark on AWS (EMR) to process the Sparkifydb data lake on S3 with an ETL pipeline implemented in python. The raw data is kept on an S3 data lake, processed by the etl script then put to another S3 storage location as a series of tables. These final tables will allow users to gain insights into the habits and interests of the existing user base.

## ETL Process

The process is composed of the following tasks:

- Obtaining the event logs and song data from the S3 storage.
- Process the data using Spark running on an EMR cluster in AWS. The process extracts various data tables (described below) and prepares them for storage.
- Store the table data back to S3 in parquet format.

## Schema

The following dimension tables are created as a result of processing the log data:

- users (user_id, first_name, last_name, gender, level). Concerning the users of the Sparkify app.
- songs (song_id, title, artist_id, year, duration). Details of the individual songs in the song data.
- artists (artist_id, name, location, latitude, longitude). Artists from the song data.
- time (start_time, hour, day, week, month, year, weekday). Record of each songs play event.

The follow table is created as a fact table to link the above dimension tables:

- songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent). Detailed information about each song play and links to dimension data.

## Partitioning

The output to S3 is in Spark parqute format, and partitioned as follows:

- Songs table files are partitioned by year and artist. 
- Time table files are partitioned by year and month. 
- Songplays table files are partitioned by year and month.

## Running ETL

Use the following steps to fun the etl script:

- Set AWS credentials in dl.cfg.
- Launch an EMR cluster on AWS. 
  - Ensure Spark is installed in the cluster.
  - Ensure you have ssh access and generate appropriate keys.
- Create an S3 bucket and update the etl.py to store its parquet files on this new bucket.
- Copy the etl.py and dl.cfg files to the EMR cluster (use scp).
- Use ssh to access the EMC cluster and run the script as follows:

```bash
spark-submit etl.py
```

With a 5 node cluster (1 master and 4 workers), expect the job to complete in around 2 hours.

## Documents

Some additional documents are included with the repository as follows:

- Screenshots of the completed spark job have been placed in [screenshots](screenshots)
- A cleaned up python notebook used to develop the ETL script is available under [notebooks](notebooks). This is retained as a development reference.

