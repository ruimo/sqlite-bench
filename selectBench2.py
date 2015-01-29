"""Perform select benchmark test.

usage: selectBench2.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench

def updateDepartmentBench(conn, beginTranFunc, commitTranFunc, updateFunc):
    cur = conn.cursor()
    # Bulk insert 100 records at once.
    indicies = range(1, 50000)
    for chunk in [indicies[x:x+100] for x in range(1, len(indicies), 100)]:
        beginTranFunc(cur)
        updateFunc(cur, chunk)
        commitTranFunc(cur)

def selectBenchSqlite(conn):
    def insertFunc(cur, chunk):
        cur.executemany(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)",
            map((lambda i: ("dept%08d" %i,)), chunk)
        )

    def performInsert():
        updateDepartmentBench(
            conn, lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), insertFunc
        )

    def performSelect():
        cur = conn.cursor()
        # Seems no prepared statement support for sqlite...
        for i in range(1, 50000):
            cur.execute("select * from departments order by created desc limit 5")
            cur.fetchall()

    bench.createTableSqlite(conn)
    bench.withStopwatch("insert departments with SQLite", performInsert)
    bench.withStopwatch("select departments with SQLite", performSelect)

def selectBenchPgsql(conn):
    def insertFunc(cur, chunk):
        cur.executemany(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            map((lambda i: ("dept%08d" %i,)), chunk)
        )

    def performInsert():
        updateDepartmentBench(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc
        )

    def performSelect():
        cur = conn.cursor()
        cur.execute(
            "prepare myquery as "
            "select * from departments order by created desc limit 5"
        )
        for i in range(1, 50000):
            cur.execute("execute myquery")
            cur.fetchall()
            
    bench.createTablePgsql(conn)
    bench.withStopwatch("insert departments with Postgres", performInsert)
    bench.withStopwatch("select departments with Postgres", performSelect)

if __name__ == '__main__':
    args = docopt(__doc__)
    bench.withSqliteConnection("/tmp/test.db", selectBenchSqlite, isolationLevel = None, useWal = args["--wal"])
    bench.withPgsqlConnection(selectBenchPgsql)
