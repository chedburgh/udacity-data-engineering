#!/usr/bin/env python3

import sys
import argparse
import configparser
import psycopg2

from sql_queries import copy_table_queries, insert_table_queries
from create_tables import create_tables, drop_tables


def connect():
    """
    Wrap the connect process. Returns a connection and cursor. Caller has
    to close connection.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))

    cur = conn.cursor()
    print("Connected to {}".format(config.get('CLUSTER', 'HOST')))
    return conn, cur


def load_tables(queries):
    """
    Wrap the load tables routine so it can be used for both staging and
    final insert queries. Takes a list of queries to execute
    """

    try:
        conn, cur = connect()

        for query in queries:
            cur.execute(query)
            conn.commit()

        conn.close()

    except (Exception, psycopg2.Error) as error:
        print("Error while loading table: ", error)
        return False

    return True


def verify_table(cur, conn, table):
    """
    Run a basic verification of the given table. Simple get its size
    and 10 rows, and print these out. 
    """

    print("Fetching some records and count for {}...".format(table))
    cur.execute("SELECT COUNT(*) FROM {}".format(table))
    records = cur.fetchall()
    print("Row count for {}:{}".format(table, records[0]))
    print("Record sample")
    cur.execute("SELECT * FROM {} LIMIT 10".format(table))
    records = cur.fetchall()

    # dump the fetcged records
    for row in records:
        print(row)


def verify_tables(tables):
    """
    Wrapper to run a verify on the given list of tables
    """

    print("Fetching some records from the staging tables...")

    try:
        conn, cur = connect()

        for table in tables:
            verify_table(cur, conn, table)

        conn.commit()
        conn.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching sample data: ", error)
        return False

    return True


def verify_staging_mode(args):
    """
    Pull some rows and do a count on the staging tables. Helps to verify
    if everything went ok
    """

    return verify_tables(["staging_events", "staging_songs"])


def verify_final_mode(args):
    """
    Pull some rows and do a count on the final tables. Helps to verify
    if everything went ok
    """

    return verify_tables(["songplays", "users", "songs", "artists", "time"])


def create_mode(args):
    """
    Initially drop then create all the required tables in both staging 
    and final
    """

    try:
        print("Dropping any existing tables...")

        conn, cur = connect()
        drop_tables(cur, conn)

        print("Creating tables...")

        create_tables(cur, conn)
        conn.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables: ", error)
        return False

    return True


def drop_mode(args):
    """
    Drop all staging and final tables
    """

    try:
        print("Dropping any existing tables...")

        conn, cur = connect()
        drop_tables(cur, conn)
        conn.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables: ", error)
        return False

    return True


def staging_insert_mode(args):
    """
    Copy from the csv files into the staging tables
    """

    print("Copying data into staging tables...")
    return load_tables(copy_table_queries)


def final_insert_mode(args):
    """
    Insert from the staging area into the final tables
    """

    print("Copying data into final tables...")
    return load_tables(insert_table_queries)


def etl_mode(args):
    """
    Run the full pipeline, creating tables, inserting to the
    staging area, then inserting into the final tables
    """

    # run the entire process
    if create_mode(args) and staging_insert_mode(args) and final_insert_mode(args):
        return True

    return False


def main():
    """ 
    Main function to handle entry and script arguments
    """

    def help_message(parser):
        print(parser)
        parser.print_help(sys.stderr)

    parser = argparse.ArgumentParser(description="Project 3 ETL Script")
    subparsers = parser.add_subparsers(title="available commands", metavar="mode")

    parser_verify = subparsers.add_parser("verify", help="run some simple verification routines")
    parser_verify.set_defaults(func=help_message, parser=parser_verify)
    verify_subparsers = parser_verify.add_subparsers(title="available subcommands", metavar="mode")

    parser_verify_staging = verify_subparsers.add_parser("staging", help="run some simple verification routines on staging")
    parser_verify_staging.set_defaults(func=verify_staging_mode)

    parser_verify_final = verify_subparsers.add_parser("final", help="run some simple verification routines on final tables")
    parser_verify_final.set_defaults(func=verify_final_mode)

    parser_create = subparsers.add_parser("create", help="create the database tables (drops tables first)")
    parser_create.set_defaults(func=create_mode)

    parser_drop = subparsers.add_parser("drop", help="drop the database tables")
    parser_drop.set_defaults(func=drop_mode)

    parser_load = subparsers.add_parser("load", help="load data into the tables")
    load_subparsers = parser_load.add_subparsers(title="available subcommands", metavar="mode")

    parser_staging = load_subparsers.add_parser("staging", help="load data into the staging tables")
    parser_staging.set_defaults(func=staging_insert_mode)

    parser_final = load_subparsers.add_parser("final", help="load data from staging to final tables")
    parser_final.set_defaults(func=final_insert_mode)

    parser_final = subparsers.add_parser("full", help="run the complete etl pipeline")
    parser_final.set_defaults(func=etl_mode)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    if main() is False:
        sys.stdout.write("Command failed\n")
