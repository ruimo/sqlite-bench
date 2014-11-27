"""Perform concurrent insert benchmark test.

usage: concurrentInsertBench.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench, time
import threading

def doInTransaction(conn, beginTranFunc, commitTranFunc, func):
    cur = conn.cursor()
    beginTranFunc(cur)
    ret = func(cur)
    commitTranFunc(cur)
    return ret

def doInTransactionSqlite(conn, func):
    return doInTransaction(
        conn, lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), func
    )

def doInTransactionPgsql(conn, func):
    return doInTransaction(
        conn, (lambda cur: None), lambda cur: conn.commit(), func
    )
    
def getKeys(conn, picker):
    keys = doInTransactionSqlite(conn, picker)
    if keys is None:
        time.sleep(0.3)
        return getKeys(conn, picker)
    return keys

def addrPicker(cur):
    cur.execute("select address_id from addresses order by RANDOM() limit 1")
    return cur.fetchone()

def userDeptPicker(cur):
    cur.execute(
        "select user_id, department_id from users u cross join departments d " +
        "where not exists (select 'X' from user_department where user_id = u.user_id and department_id = d.department_id) " +
        "order by RANDOM() limit 1"
    )
    return cur.fetchone()

def insertDepartmentSqlite(conn, doNtimes):
    def insertFunc(cur, i):
        cur.execute(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)", 
            ("dept%08d" % i, )
        )

    bench.withStopwatch(
        "insert departments with SQLite",
        lambda: doNtimes(lambda i: doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i)))
    )

def insertAddressSqlite(conn, doNtimes):
    def insertFunc(cur, i):
        cur.execute(
            "insert into addresses (address) values (?)", ("addr%08d" % i, )
        )

    bench.withStopwatch(
        "insert addresses with SQLite",
        lambda: doNtimes(lambda i: doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i)))
    )

def insertUserSqlite(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into users(address_id, user_name, first_name, last_name, created) " +
            "values (?, ?, ?, ?, CURRENT_TIMESTAMP)",
            (keys[0], "user%08d" % i, "fname%08d" % i, "lname%08d" % i)
        )

    def performer(i):
        keys = getKeys(conn, addrPicker)
        doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert users with SQLite", lambda: doNtimes(performer))

def insertUserDepartmentSqlite(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into user_department(user_id, department_id) values (?, ?)", (keys[0], keys[1])
        )

    def performer(i):
        keys = getKeys(conn, userDeptPicker)
        doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch(
        "insert user_department with SQLite", lambda: doNtimes(performer)
    )

def insertDepartmentPgsql(conn, doNtimes):
    def insertFunc(cur, i):
        cur.execute(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            ("dept%08d" % i, )
        )

    def performer(i):
        doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert departments with Postgres", lambda: doNtimes(performer))

def insertAddressPgsql(conn, n):
    def insertFunc(cur, i):
        cur.execute(
            "insert into addresses (address) values (%s)", ("addr%08d" % i, )
        )

    def performer(i):
        doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert addresses with Postgres", lambda: doNtimes(performer))

def insertUserPgsql(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into users(address_id, user_name, first_name, last_name, created) " +
            "values (%s, %s, %s, %s, CURRENT_TIMESTAMP)",
            (keys[0], "user%08d" % i, "fname%08d" % i, "lname%08d" % i)
        )

    def performer(i):
        keys = getKeys(conn, addrPicker)
        doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert users with Postgres", lambda: doNtimes(performer))

def insertUserDepartmentPgsql(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into user_department(user_id, department_id) values (%s, %s)", (keys[0], keys[1])
        )

    def performer(i):
        keys = getKeys(conn, userDeptPicker)
        doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert user_department with Postgres", lambda: doNtimes(performer))

def loop(func, n):
    for i in range(0, n):
        func(i)

def doWithThreadSqlite(func):
    t = threading.Thread(
        target = lambda : bench.withSqliteConnection("/tmp/test.db", func, isolationLevel = None, useWal = args["--wal"])
    )
    t.start()
    return t

def doWithThreadPgsql(func):
    t = threading.Thread(target = lambda: bench.withPgsqlConnection(func))
    t.start()
    return t

def doInThreads(*funcs):
    threads = []
    for f in funcs:
        threads.append(f())
    for t in threads:
        t.join()

def sqliteBench(doNtimes, args):
    doWithThreadSqlite(bench.createTableSqlite).join()
    doInThreads(
        lambda: doWithThreadSqlite(lambda conn: insertDepartmentSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: insertAddressSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: insertUserSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: insertUserDepartmentSqlite(conn, doNtimes))
    )

def pgsqlBench(doNtimes, args):
    doWithThreadPgsql(bench.createTablePgsql).join()
    doInThreads(
        lambda: doWithThreadPgsql(lambda conn: insertDepartmentPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: insertAddressPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: insertUserPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: insertUserDepartmentPgsql(conn, doNtimes))
    )

if __name__ == '__main__':
    doNtimes = lambda func: loop(func, 300)
    args = docopt(__doc__)
    # 'isolationLevel = None' means auto commit.
    bench.withStopwatch("SQLite all", lambda: sqliteBench(doNtimes, args))
    bench.withStopwatch("Postgres all", lambda: pgsqlBench(doNtimes, args))
