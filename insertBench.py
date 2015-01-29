"""Perform insert benchmark test.

usage: insertBench.py [-h] [--wal] [--copy]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
    --copy      USE COPY statement instead of insert statement for Postgres.
"""

from docopt import docopt
import io, datetime, bench

def insertDepartmentBench(conn, beginTranFunc, commitTranFunc, insertFunc):
    cur = conn.cursor()
    # Bulk insert 100 records at once.
    indicies = range(1, 100000)
    for chunk in [indicies[x:x+100] for x in range(1, len(indicies), 100)]:
        beginTranFunc(cur)
        insertFunc(cur, chunk)
        commitTranFunc(cur)

def insertBenchSqlite(conn):
    def insertFunc(cur, chunk):
        cur.executemany(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)",
            map((lambda i: ("dept%08d" %i,)), chunk)
        )

    def performBench():
        insertDepartmentBench(
            conn, lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), insertFunc
        )

    bench.createTableSqlite(conn)
    bench.withStopwatch("insert departments with SQLite", performBench)

def insertBenchPgsql(conn):
    def insertFunc(cur, chunk):
        cur.executemany(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            map((lambda i: ["dept%08d" %i]), chunk)
        )

    def performBench():
        insertDepartmentBench(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc
        )

    bench.createTablePgsql(conn)
    bench.withStopwatch("insert departments with Postgres", performBench)

def copyInsertBenchPgsql(conn):
    def insertFunc(cur, chunk):
        cur.copy_from(
            io.StringIO(''.join(map((lambda i: "dept%08d\t%s\n" % (i, datetime.datetime.now())), chunk))),
            'departments', columns=('department_name', 'created'))

    def performBench():
        insertDepartmentBench(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc
        )

    bench.createTablePgsql(conn)
    bench.withStopwatch("insert departments with Postgres using COPY", performBench)

if __name__ == '__main__':
    args = docopt(__doc__)
    # 'isolationLevel = None' means auto commit.
    bench.withSqliteConnection("/tmp/test.db", insertBenchSqlite, isolationLevel = None, useWal = args["--wal"])
    if args["--copy"]:
        bench.withPgsqlConnection(copyInsertBenchPgsql)
    else:
        bench.withPgsqlConnection(insertBenchPgsql)
