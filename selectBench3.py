"""Perform select benchmark test.

usage: selectBench3.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench

address = ['Tokyo', 'Chiba', 'Saitama', 'Kanagawa', 'Osaka', 'Kyoto']

def bulkUpdate(conn, beginTranFunc, commitTranFunc, updateFunc):
    cur = conn.cursor()
    # Bulk insert 100 records at once.
    indicies = range(1, 50000)
    for chunk in [indicies[x:x+100] for x in range(1, len(indicies), 100)]:
        beginTranFunc(cur)
        updateFunc(cur, chunk)
        commitTranFunc(cur)

def selectBenchSqlite(conn):
    def insertAddress(cur):
        cur.executemany(
            "insert into addresses (address) values (?)",
            map((lambda x: (x,)), address)
        )

    def insertFunc(cur, chunk):
        for i in chunk:
            cur.execute("select address_id from addresses where address = ?", [address[i % len(address)]])
            aid = cur.fetchone()[0]
            cur.execute(
                """
                insert into users (address_id, user_name, first_name, last_name, created) 
                values (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [aid, "user%08d" %i, "first%08d" %i, "last%08d" %i]
            )

    def performInsert():
        cur = conn.cursor()
        insertAddress(cur)
        bulkUpdate(
            conn, 
            lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), insertFunc
        )

    def performSelect():
        cur = conn.cursor()
        # Seems no prepared statement support for sqlite...
        for i in range(1, 500):
            cur.execute(
                "select count(*) from users u inner join addresses a on u.address_id = a.address_id where address = ?",
                (address[i % len(address)],)
            )
            cur.fetchall()

    bench.createTableSqlite(conn)
    bench.withStopwatch("insert departments with SQLite", performInsert)
    bench.withStopwatch("select departments with SQLite", performSelect)

def selectBenchPgsql(conn):
    def insertAddress(cur):
        cur.executemany(
            "insert into addresses (address) values (%s)",
            map((lambda x: (x,)), address)
        )

    def insertFunc(cur, chunk):
        for i in chunk:
            cur.execute("select address_id from addresses where address = %s", [address[i % len(address)]])
            aid = cur.fetchone()[0]
            cur.execute(
                """
                insert into users (address_id, user_name, first_name, last_name, created) 
                values (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                [aid, "user%08d" %i, "first%08d" %i, "last%08d" %i]
            )

    def performInsert():
        cur = conn.cursor()
        insertAddress(cur)
        bulkUpdate(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc
        )

    def performSelect():
        cur = conn.cursor()
        cur.execute(
            "prepare myquery as "
            "select count(*) from users u inner join addresses a on u.address_id = a.address_id where address = $1"
        )
        for i in range(1, 500):
            cur.execute("execute myquery (%s)", (address[i % len(address)],))
            cur.fetchall()
            
    bench.createTablePgsql(conn)
    bench.withStopwatch("insert departments with Postgres", performInsert)
    bench.withStopwatch("select departments with Postgres", performSelect)

if __name__ == '__main__':
    args = docopt(__doc__)
    bench.withSqliteConnection("/tmp/test.db", selectBenchSqlite, isolationLevel = None, useWal = args["--wal"])
    bench.withPgsqlConnection(selectBenchPgsql)
