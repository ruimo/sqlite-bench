"""Perform update benchmark test.

usage: updateBench.py [-h] [--wal]

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

def updateBenchSqlite(conn):
    def insertFunc(cur, chunk):
        cur.executemany(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)",
            map((lambda i: ("dept%08d" %i,)), chunk)
        )

    def updateFunc(cur, chunk):
        cur.executemany(
            "update departments set department_name = ? where department_name = ?",
            map((lambda i: ("deptUpdated%08d" %i, "dept%08d" %i)), chunk)
        )

    def performInsert():
        updateDepartmentBench(
            conn, lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), insertFunc
        )

    def performUpdate():
        updateDepartmentBench(
            conn, lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), updateFunc
        )

    bench.createTableSqlite(conn)
    bench.withStopwatch("insert departments with SQLite", performInsert)
    bench.withStopwatch("update departments with SQLite", performUpdate)

def updateBenchPgsql(conn):
    def insertFunc(cur, chunk):
        cur.executemany(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            map((lambda i: ("dept%08d" %i,)), chunk)
        )

    def updateFunc(cur, chunk):
        cur.executemany(
            "update departments set department_name = %s where department_name = %s",
            map((lambda i: ("deptUpdated%08d" %i, "dept%08d" %i)), chunk)
        )

    def performInsert():
        updateDepartmentBench(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc
        )

    def performUpdate():
        updateDepartmentBench(
            conn, (lambda cur: None), lambda cur: conn.commit(), updateFunc
        )

    bench.createTablePgsql(conn)
    bench.withStopwatch("insert departments with Postgres", performInsert)
    bench.withStopwatch("update departments with Postgres", performUpdate)

if __name__ == '__main__':
    args = docopt(__doc__)
    bench.withSqliteConnection("/tmp/test.db", updateBenchSqlite, isolationLevel = None, useWal = args["--wal"])
    bench.withPgsqlConnection(updateBenchPgsql)
