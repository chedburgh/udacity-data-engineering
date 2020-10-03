#!/usr/bin/env python3

import os
import io
import glob
import psycopg2
import pandas as pd

from sql_queries import *


def process_song_file(cursor, filepath):
    """
    Process song files and upload the data into postgres
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location",
                           "artist_latitude", "artist_longitude"]].values[0])

    cursor.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = list(
        df[["song_id", "artist_id", "title", "year", "duration"]].values[0])

    cursor.execute(song_table_insert, song_data)


def upload_time_data(cursor, df):
    """
    Handle the time data, and insert it into postgres via a copy
    """

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = [df.ts.values, t.dt.hour.values, t.dt.day.values,
                 t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.dayofweek.values]

    column_labels = ["start_time", "hour", "day",
                     "week", "month", "year", "weekday"]

    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    # trim any local duplicates
    time_df.drop_duplicates(["start_time"], inplace=True)

    # we will use this buffer to create csv files in memory, then
    # copy them directly into the database
    buffer = io.StringIO()

    # now we are going to copy directly into a staging table and then
    # move the data to the time table. This speeds up the process
    # greatly, and still allows the check on the primary key when copying
    # from staging to final table
    time_df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # the copy to staging
    cursor.execute(time_staging_create)
    cursor.copy_from(buffer, time_staging, sep=",")

    # now staging to final table
    cursor.execute(time_insert_from_staging)
    cursor.execute(time_staging_drop)


def upload_user_data(cursor, df):
    """
    Process the user data, and insert it into postgres via a copy
    """

    # prepare a dataframe for the user data
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]]
    user_df.drop_duplicates(["userId"], inplace=True)

    # we will use this buffer to create csv files in memory, then
    # copy them directly into the database
    buffer = io.StringIO()

    # like the time data, we load it directly into a staging table
    user_df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # the copy to staging
    cursor.execute(user_staging_create)
    cursor.copy_from(buffer, user_staging, sep=",")

    # now staging to final table
    cursor.execute(user_insert_from_staging)
    cursor.execute(user_staging_drop)


def upload_songplay_data(cursor, df):
    """
    Process the songplay data, and insert it into postgres via a copy
    """

    # we will use this buffer to create csv files in memory, then
    # copy them directly into the database
    buffer = io.StringIO()

    # dump the available column data to csv for copy import
    df.to_csv(buffer, index=False, header=False, sep="\t",
              columns=["userId", "ts", "sessionId", "level", "location", "userAgent", "song", "artist", "length"])

    buffer.seek(0)

    # the copy to staging
    cursor.execute(songplay_staging_create)
    cursor.copy_from(buffer, songplay_staging, sep="\t")

    # now staging to final table
    cursor.execute(songplay_insert_from_staging)
    cursor.execute(songplay_staging_drop)


def process_log_file(cursor, filepath):
    """
    Process the event log files and populate the database from them
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # break into separate functions for each table, to keep the code clean
    upload_time_data(cursor, df)
    upload_user_data(cursor, df)
    upload_songplay_data(cursor, df)


def process_data(cursor, conn, filepath, func):
    """
    Collect all the log files for the given filepath and pass them
    to the func parameter
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cursor = conn.cursor()
    process_data(cursor, conn, filepath="data/song_data",func=process_song_file)
    process_data(cursor, conn, filepath="data/log_data", func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()
