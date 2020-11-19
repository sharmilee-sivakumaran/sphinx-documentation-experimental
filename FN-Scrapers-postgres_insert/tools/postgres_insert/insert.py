from sqlalchemy.engine import create_engine
from sqlalchemy import text
import argparse
import os
import yaml


class PostgresDB:
    CREATE_TABLE = u"""
    CREATE TABLE {} (
    id serial NOT NULL PRIMARY KEY,
    data json
    );
    """

    TABLE_EXISTS = u"""
    SELECT to_regclass(:table);
    """

    INSERT = u"""
    INSERT into {} (data)
    VALUES
    (:data)
    """

    def __init__(self):
        with open('settings.yaml') as fp:
            self.config = yaml.safe_load(fp)

    def insert(self, file_loc, table_name):
        with open(file_loc) as f:
            lines = [line[11:] for line in f.readlines()]

        engine = create_engine("postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}".format(**self.config))
        with engine.connect() as connection:
            table_exists = connection.execute(text(self.TABLE_EXISTS), {'table': table_name}).scalar()
            if not table_exists:
                connection.execute(text(self.CREATE_TABLE.format(table_name)))

            for line in lines:
                    connection.execute(text(self.INSERT.format(table_name)), {'data': line})
        print "Done"


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    parser = argparse.ArgumentParser()
    parser.add_argument("rabbitmq_file", help="Rabbitmq dump file to persist into the db")
    parser.add_argument("-t", "--table_name", help="Table name to use")

    args = parser.parse_args()

    rabbitmq_file = args.rabbitmq_file
    table_name = args.table_name

    if not rabbitmq_file or not table_name:
        print("rabbitmq_file and table_name is required")
        parser.print_help()
        exit()

    db = PostgresDB()
    db.insert(rabbitmq_file, table_name)


main()
