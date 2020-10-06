import psycopg2
import configparser

from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):

    """
    DROP all tables
    :param cur:
    :param conn:
    :return:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    """
    Create all tables
    :param curr:
    :param conn:
    :return:

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    curr = conn.cursor()

    drop_tables(curr,conn)
    create_tables(curr,conn)

    conn.close()

if __name__ == "__main__":
    main()


