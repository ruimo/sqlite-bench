"""Perform select benchmark test.

usage: selectBench4.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench

address = ['Tokyo', 'Chiba', 'Saitama', 'Kanagawa', 'Osaka', 'Kyoto']
department = ['Sales1', 'Sales2', 'Consulting', 'HumanResources', 'Marketing']

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

    def insertDepartment(cur):
        cur.executemany(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)",
            map((lambda x: (x,)), department)
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

    def insertFunc2(cur, chunk):
        for i in chunk:
            if (i % 33) != 0:
                cur.execute(
                    "select department_id from departments where department_name = ?",
                    [department[i % len(department)]]
                )
                did = cur.fetchone()[0]
                cur.execute(
                    "select user_id from users where user_name = ?",
                    ["user%08d" %i]
                )
                uid = cur.fetchone()[0]
                cur.execute(
                    """
                    insert into user_department (user_id, department_id) 
                    values (?, ?)
                    """,
                    [uid, did]
                )

    def performInsert():
        cur = conn.cursor()
        insertAddress(cur)
        insertDepartment(cur)
        bulkUpdate(
            conn, 
            lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), insertFunc
        )
        bulkUpdate(
            conn, 
            lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), insertFunc2
        )

    def performSelect():
        cur = conn.cursor()
        for i in range(1, 500):
            cur.execute(
                """
                select count(u.user_id) from users u
                inner join addresses a on u.address_id = a.address_id
                left join user_department ud on u.user_id = ud.user_id
                inner join departments d on ud.department_id = d.department_id
                where (d.department_name = 'Sales1' or d.department_name is null) and address = 'Tokyo'
                """
            )
            cur.fetchone()

    bench.createTableSqlite(conn)
    bench.withStopwatch("insert departments with SQLite", performInsert)
    bench.withStopwatch("select departments with SQLite", performSelect)

def selectBenchPgsql(conn):
    def insertAddress(cur):
        cur.executemany(
            "insert into addresses (address) values (%s)",
            map((lambda x: (x,)), address)
        )

    def insertDepartment(cur):
        cur.executemany(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            map((lambda x: (x,)), department)
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

    def insertFunc2(cur, chunk):
        for i in chunk:
            if (i % 33) != 0:
                cur.execute(
                    "select department_id from departments where department_name = %s",
                    [department[i % len(department)]]
                )
                did = cur.fetchone()[0]
                cur.execute(
                    "select user_id from users where user_name = %s",
                    ["user%08d" %i]
                )
                uid = cur.fetchone()[0]
                cur.execute(
                    """
                    insert into user_department (user_id, department_id) 
                    values (%s, %s)
                    """,
                    [uid, did]
                )

    def performInsert():
        cur = conn.cursor()
        insertAddress(cur)
        insertDepartment(cur)
        bulkUpdate(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc
        )
        bulkUpdate(
            conn, (lambda cur: None), lambda cur: conn.commit(), insertFunc2
        )

    def performSelect():
        cur = conn.cursor()
        cur.execute(
            "prepare myquery as "
            """
            select count(u.user_id) from users u
            inner join addresses a on u.address_id = a.address_id
            left join user_department ud on u.user_id = ud.user_id
            inner join departments d on ud.department_id = d.department_id
            where (d.department_name = 'Sales1' or d.department_name is null) and address = 'Tokyo'
            """
        )
        for i in range(1, 500):
            cur.execute("execute myquery")
            cur.fetchone()
            
    bench.createTablePgsql(conn)
    bench.withStopwatch("insert departments with Postgres", performInsert)
    bench.withStopwatch("select departments with Postgres", performSelect)

if __name__ == '__main__':
    args = docopt(__doc__)
    bench.withSqliteConnection("/tmp/test.db", selectBenchSqlite, isolationLevel = None, useWal = args["--wal"])
    bench.withPgsqlConnection(selectBenchPgsql)
